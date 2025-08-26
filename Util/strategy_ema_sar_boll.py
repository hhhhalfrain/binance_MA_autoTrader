# strategy_ema_sar_bw_percentile.py
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import numpy as np
import pandas as pd
from math import tanh

@dataclass
class StrategyConfig:
    # —— 因子1: EMA + SAR ——
    ema_periods: Tuple[int, int, int] = (20, 30, 60)   # (快, 中, 慢)
    ema_order_weight: float = 0.6                      # EMA“多空排列”权重
    ema_slope_weight: float = 0.4                      # EMA“斜率”权重
    f1_weight_ema: float = 0.7                         # f1 中 EMA 模块权重
    f1_weight_sar: float = 0.3                         # f1 中 SAR 模块权重
    sar_af: float = 0.02                               # SAR 加速因子步进
    sar_af_max: float = 0.20                           # SAR 最大加速因子
    sar_vol_window: int = 30                           # 用近 N 根波动归一化 (close - sar)

    # —— 因子2: 布林“带宽”的分位数（+斜率） ——
    boll_period: int = 20
    boll_mult: float = 2.0
    lookback_for_percentile: int = 500                 # 以过去 N 根（不含当前）做分位数基准
    f2_width_base: str = "ma"                          # 'ma' | 'price' | 'none' —— 宽度比例化基准
    f2_level_invert: bool = True                       # True: 宽度越大仓位越小 ⇒ level = 1 - percentile
    f2_weight_level: float = 0.7                       # f2 中“水平分量”权重
    f2_weight_slope: float = 0.3                       # f2 中“斜率分量”权重
    f2_slope_scale: float = 3.0                        # 斜率归一化尺度（越小越敏感）
    min_sigma: float = 1e-8                            # 防止除零
    percentile_inclusive: bool = True                  # True: 用 <= 计算分位；False: 用 <

class EMASARBollBandwidthStrategy:
    """
    输入：klines: List[Dict]，每项至少包含 close/high/low（open/volume/timestamp 可选）
    输出：持仓权重 ∈ [-1, 1]，定义为 pos = F1 * F2
      - F1 ∈ [-1, 1]（EMA+SAR）
      - F2 ∈ [0, 1]（布林带宽分位数+斜率）
    """
    def __init__(self, cfg: Optional[StrategyConfig] = None):
        self.cfg = cfg or StrategyConfig()

    # ===== 公共接口 =====
    def position_size(self, klines: List[Dict], return_factors: bool = False):
        df = self._to_df(klines)

        # —— 动态最小样本数计算（不再写死 1500）——
        required = self._min_bars_required()
        if len(df) < required:
            raise ValueError(f"样本不足：需要≥{required} 根K线，当前={len(df)}")

        close, high, low = df["close"], df["high"], df["low"]

        # 因子1：EMA + SAR（-1 ~ 1）
        f1 = self._factor1_ema_sar(close, high, low)

        # 因子2：布林带宽分位数（0 ~ 1），可叠加斜率权重
        f2 = self._factor2_bandwidth_percentile(close)

        # —— 最终持仓：F1 * F2 ∈ [-1, 1] ——
        pos = float(np.clip(f1 * f2, -1.0, 1.0))
        if return_factors:
            return pos, {"f1": f1, "f2": f2}
        return pos

    # ===== 动态最小样本数 =====
    def _min_bars_required(self) -> int:
        """
        取所有用到的窗口上界：
          - EMA 需要 max(ema_periods) + 2（用于斜率差分）
          - SAR 本身只需≥2，但 f1 中还用到了 sar_vol_window 的 std
          - F2 需要 boll_period + lookback_for_percentile + 1（当前+历史N）
        """
        e_max = max(self.cfg.ema_periods)
        need_ema = e_max + 2
        need_sar = max(2, self.cfg.sar_vol_window + 1)
        need_f2  = self.cfg.boll_period + self.cfg.lookback_for_percentile + 1
        return max(need_ema, need_sar, need_f2)

    # ===== 因子1：EMA + SAR =====
    def _factor1_ema_sar(self, close: pd.Series, high: pd.Series, low: pd.Series) -> float:
        e1, e2, e3 = self.cfg.ema_periods
        ema1 = close.ewm(span=e1, adjust=False).mean()
        ema2 = close.ewm(span=e2, adjust=False).mean()
        ema3 = close.ewm(span=e3, adjust=False).mean()

        # EMA 排列得分（完全多头=+1，完全空头=-1）
        up_votes = int(ema1.iloc[-1] > ema2.iloc[-1]) + int(ema2.iloc[-1] > ema3.iloc[-1]) + int(ema1.iloc[-1] > ema3.iloc[-1])
        down_votes = int(ema1.iloc[-1] < ema2.iloc[-1]) + int(ema2.iloc[-1] < ema3.iloc[-1]) + int(ema1.iloc[-1] < ema3.iloc[-1])
        order_score = (up_votes - down_votes) / 3.0  # ∈ {-1,-0.33,0,0.33,1}

        # EMA 斜率（快+慢）
        slope1 = np.sign(ema1.iloc[-1] - ema1.iloc[-2]) if len(ema1) >= 2 else 0.0
        slope3 = np.sign(ema3.iloc[-1] - ema3.iloc[-2]) if len(ema3) >= 2 else 0.0
        slope_score = 0.5 * (slope1 + slope3)          # ∈ {-1,-0.5,0,0.5,1}

        ema_score = (
            self.cfg.ema_order_weight * order_score +
            self.cfg.ema_slope_weight * slope_score
        )
        ema_score = float(np.clip(ema_score, -1.0, 1.0))

        # SAR：以 (close - sar) / 波动，用 tanh 压缩
        sar = self._psar(high.to_numpy(), low.to_numpy(),
                         af=self.cfg.sar_af, af_max=self.cfg.sar_af_max)
        last_close = float(close.iloc[-1])
        last_sar   = float(sar[-1])
        vol = float(close.tail(self.cfg.sar_vol_window).std(ddof=0)) or 1e-8
        sar_score = float(tanh((last_close - last_sar) / (3.0 * vol)))  # (-1,1)

        w_ema, w_sar = self.cfg.f1_weight_ema, self.cfg.f1_weight_sar
        norm = max(1e-8, abs(w_ema) + abs(w_sar))
        f1 = (w_ema * ema_score + w_sar * sar_score) / norm
        return float(np.clip(f1, -1.0, 1.0))

    # ===== 因子2：布林带宽分位数（+斜率） =====
    def _factor2_bandwidth_percentile(self, close: pd.Series) -> float:
        n   = self.cfg.boll_period
        k   = self.cfg.boll_mult
        N   = self.cfg.lookback_for_percentile

        ma = close.rolling(n, min_periods=n).mean()
        sd = close.rolling(n, min_periods=n).std(ddof=0)
        width = 2.0 * k * sd  # 带宽 = 上轨 - 下轨 = 2*k*std

        # —— 带宽比例化（可选）——
        base_mode = (self.cfg.f2_width_base or "ma").lower()
        if base_mode == "ma":
            base = ma
        elif base_mode == "price":
            base = close
        else:
            base = pd.Series(1.0, index=close.index, dtype=float)
        w = (width / base).replace([np.inf, -np.inf], np.nan).dropna()

        # 需要历史 N 根 + 当前 1 根
        if len(w) < N + 1:
            raise ValueError(f"有效带宽样本不足：需要≥{N+1}（含当前），当前={len(w)}")

        hist = w.iloc[-(N+1):-1].to_numpy(dtype=float)  # 过去 N 根
        cur  = float(w.iloc[-1])

        # —— 分位数：当前打败历史的比例 ——
        if self.cfg.percentile_inclusive:
            rank = np.count_nonzero(hist <= cur)
        else:
            rank = np.count_nonzero(hist < cur)
        level = rank / len(hist)  # 0~1
        if self.cfg.f2_level_invert:  # 宽→大→仓位小（风险控制）
            level = 1.0 - level

        # —— 斜率分量：按历史STD归一化后 tanh，再映射到[0,1] ——
        sigma = float(np.std(hist, ddof=1)) if len(hist) > 1 else 0.0
        if sigma < self.cfg.min_sigma:
            slope_mapped = 0.5
        else:
            delta = float(w.iloc[-1] - w.iloc[-2])
            slope_score = tanh(delta / (self.cfg.f2_slope_scale * sigma))  # (-1,1)
            slope_mapped = 0.5 * (slope_score + 1.0)                       # (0,1)

        # —— 融合 ——
        wl, ws = self.cfg.f2_weight_level, self.cfg.f2_weight_slope
        norm = max(1e-8, abs(wl) + abs(ws))
        f2 = (wl * level + ws * slope_mapped) / norm
        return float(np.clip(f2, 0.0, 1.0))

    # ===== 工具：DataFrame 化 =====
    @staticmethod
    def _to_df(klines: List[Dict]) -> pd.DataFrame:
        df = pd.DataFrame(klines)
        need = {"close", "high", "low"}
        miss = need - set(df.columns)
        if miss:
            raise KeyError(f"klines 缺少字段: {miss}（必须包含 close/high/low）")
        for c in ["open", "close", "high", "low", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
        return df.dropna(subset=["close", "high", "low"]).reset_index(drop=True)

    # ===== 工具：简化 PSAR =====
    @staticmethod
    def _psar(high: np.ndarray, low: np.ndarray, af: float = 0.02, af_max: float = 0.2) -> np.ndarray:
        n = len(high)
        if n == 0:
            return np.array([], dtype=float)
        psar = np.zeros(n, dtype=float)
        bull = True
        ep = high[0]
        psar[0] = low[0]
        af_cur = af
        for i in range(1, n):
            prev = psar[i - 1]
            if bull:
                psar[i] = prev + af_cur * (ep - prev)
                psar[i] = min(psar[i], low[i - 1])
                if high[i] > ep:
                    ep = high[i]; af_cur = min(af_cur + af, af_max)
                if low[i] < psar[i]:
                    bull = False; psar[i] = ep; ep = low[i]; af_cur = af
            else:
                psar[i] = prev + af_cur * (ep - prev)
                psar[i] = max(psar[i], high[i - 1])
                if low[i] < ep:
                    ep = low[i]; af_cur = min(af_cur + af, af_max)
                if high[i] > psar[i]:
                    bull = True; psar[i] = ep; ep = high[i]; af_cur = af
        return psar
