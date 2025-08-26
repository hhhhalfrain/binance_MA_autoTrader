# backtester_with_export.py
from dataclasses import dataclass
from typing import Iterable, List, Dict, Any, Tuple, Optional
from itertools import product
from tqdm import tqdm
import pandas as pd
import numpy as np
import os

# --- 按你的文件名调整导入 ---
from quant_info_module import QuantInfoModule
from strategy_ema_sar_boll import EMASARBollBandwidthStrategy, StrategyConfig


@dataclass
class BacktestResult:
    symbol: str
    interval: str
    bars_used: int
    initial_capital: float
    final_equity: float
    pnl: float
    return_pct: float
    max_drawdown_pct: float
    trades: int
    fee_paid: float
    equity_curve: pd.Series           # 净值曲线
    period_records: pd.DataFrame      # 每期记录（时间/收盘/启动价/持仓/持仓净值）


class Backtester:
    """
    每根收盘：用策略给出目标持仓权重 pos∈[-1,1]（你的定义：pos=F1*F2），
    在收盘价执行到该权重。手续费：fee_rate * |成交名义金额|。
    可将每期数据导出到 xlsx。
    """
    def __init__(self, quant: Optional[QuantInfoModule] = None):
        self.quant = quant or QuantInfoModule("config.json")

    @staticmethod
    def required_bars(cfg: StrategyConfig) -> int:
        emax = max(cfg.ema_periods)
        need_ema = emax + 2
        need_sar = max(2, cfg.sar_vol_window + 1)
        need_f2  = cfg.boll_period + cfg.lookback_for_percentile + 1
        return max(need_ema, need_sar, need_f2)

    def fetch_klines(self, symbol: str, interval: str, n: int) -> List[Dict[str, Any]]:
        return self.quant.get_klines(symbol=symbol, n=n, interval=interval)

    def run(self,
            symbol: str,
            interval: str,
            bars: int,
            cfg: StrategyConfig,
            initial_capital: float = 10_000.0,
            fee_rate: float = 0.0004,
            show_progress: bool = True,
            output_xlsx_path: Optional[str] = None,
            sheet_name: Optional[str] = None) -> BacktestResult:
        """
        单次回测；若提供 output_xlsx_path，则把每期数据写到 Excel（sheet_name 默认 <symbol>_<interval>）。
        """
        need = self.required_bars(cfg)
        if bars < need + 1:
            bars = need + 1

        kl = self.fetch_klines(symbol, interval, bars)
        if len(kl) < need + 1:
            raise ValueError(f"可用K线不足：需要≥{need+1}，当前={len(kl)}")

        dfk = pd.DataFrame(kl)
        # 实际时间
        if "timestamp" in dfk.columns and (dfk["timestamp"] != 0).all():
            idx = pd.to_datetime(dfk["timestamp"].astype("int64"), unit="ms")
        else:
            idx = pd.RangeIndex(len(dfk))
        close = pd.to_numeric(dfk["close"], errors="coerce").astype(float)

        strat = EMASARBollBandwidthStrategy(cfg)

        E = float(initial_capital)   # 账户净值
        q = 0.0                      # 持仓数量（base 数量）
        fee_paid = 0.0
        trades = 0

        eq = np.zeros(len(close))
        eq[0] = E

        # 启动价格：从第一根可交易的 K（need）处的收盘价
        start_price = float(close.iloc[need])

        # 每期记录（从 t=need 开始）
        rows: List[Dict[str, Any]] = []

        rng = range(need, len(close) - 1)
        if show_progress:
            rng = tqdm(rng, desc=f"Backtest {symbol} {interval}", unit="bar")

        for t in rng:
            price_t = float(close.iloc[t])

            # 用到“截至 t”的窗口（含 t）
            window = kl[:t + 1]
            pos = strat.position_size(window, return_factors=False)  # ∈ [-1,1]

            # 目标持仓数量（无杠杆）：q*price = pos * Equity
            target_q = (pos * E) / price_t

            # 在收盘价交易到目标持仓
            dq = target_q - q
            if abs(dq) > 0:
                notional = abs(dq) * price_t
                fee = notional * max(0.0, fee_rate)
                E -= fee
                fee_paid += fee
                q = target_q
                trades += 1

            # 记录本期（在 t 收盘后的状态）
            rows.append({
                "time": idx[t],
                "close": price_t,
                "start_price": start_price,
                "position_qty": q,
                "position_value": q * price_t,
            })

            # 持有至下一根收盘兑现盈亏
            price_next = float(close.iloc[t + 1])
            E += q * (price_next - price_t)
            eq[t + 1] = E

        # 最后一根也补一条记录（不再交易，仅展示快照）
        t_last = len(close) - 1
        rows.append({
            "time": idx[t_last],
            "close": float(close.iloc[t_last]),
            "start_price": start_price,
            "position_qty": q,
            "position_value": q * float(close.iloc[t_last]),
        })

        # 每期 DataFrame（时间转字符串更友好；你也可以保持为 datetime）
        period_df = pd.DataFrame(rows)
        if isinstance(period_df.loc[0, "time"], pd.Timestamp):
            period_df["time"] = period_df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")

        final_equity = float(E)
        pnl = final_equity - initial_capital
        ret = pnl / initial_capital if initial_capital > 0 else 0.0
        mdd = self._max_drawdown(eq)

        result = BacktestResult(
            symbol=symbol,
            interval=interval,
            bars_used=len(close),
            initial_capital=initial_capital,
            final_equity=final_equity,
            pnl=pnl,
            return_pct=ret * 100.0,
            max_drawdown_pct=mdd * 100.0,
            trades=trades,
            fee_paid=fee_paid,
            equity_curve=pd.Series(eq, index=idx),
            period_records=period_df
        )

        # 输出到 xlsx
        if output_xlsx_path:
            sn = sheet_name or f"{symbol}_{interval}"
            self._write_sheet(output_xlsx_path, sn, period_df)

        return result

    @staticmethod
    def _max_drawdown(equity: np.ndarray) -> float:
        peaks = np.maximum.accumulate(equity)
        dd = (peaks - equity) / np.where(peaks == 0, 1, peaks)
        return float(np.max(dd))

    @staticmethod
    def _write_sheet(path: str, sheet_name: str, df: pd.DataFrame):
        """将 df 写入 Excel 的一个工作表；存在则附加新表。"""
        # Excel sheet 名最长 31 字符
        safe_name = (sheet_name or "Sheet")[:31]
        mode = "a" if os.path.exists(path) else "w"
        with pd.ExcelWriter(path, engine="openpyxl", mode=mode) as writer:
            df.to_excel(writer, index=False, sheet_name=safe_name)


# =========================
#   StrategyConfig 有序生成器
# =========================
class StrategyConfigGrid:
    """
    有序生成 StrategyConfig（笛卡尔积，顺序稳定），用于参数搜索。
    """
    def __init__(self,
                 ema_periods_list: Optional[List[Tuple[int, int, int]]] = None,
                 ema_order_weight_list: Optional[List[float]] = None,
                 ema_slope_weight_list: Optional[List[float]] = None,
                 f1_weight_ema_list: Optional[List[float]] = None,
                 f1_weight_sar_list: Optional[List[float]] = None,
                 sar_af_list: Optional[List[float]] = None,
                 sar_af_max_list: Optional[List[float]] = None,
                 sar_vol_window_list: Optional[List[int]] = None,
                 boll_period_list: Optional[List[int]] = None,
                 boll_mult_list: Optional[List[float]] = None,
                 lookback_for_percentile_list: Optional[List[int]] = None,
                 f2_width_base_list: Optional[List[str]] = None,
                 f2_level_invert_list: Optional[List[bool]] = None,
                 f2_weight_level_list: Optional[List[float]] = None,
                 f2_weight_slope_list: Optional[List[float]] = None,
                 f2_slope_scale_list: Optional[List[float]] = None):
        d = StrategyConfig()
        self.space = {
            "ema_periods": ema_periods_list or [d.ema_periods],
            "ema_order_weight": ema_order_weight_list or [d.ema_order_weight],
            "ema_slope_weight": ema_slope_weight_list or [d.ema_slope_weight],
            "f1_weight_ema": f1_weight_ema_list or [d.f1_weight_ema],
            "f1_weight_sar": f1_weight_sar_list or [d.f1_weight_sar],
            "sar_af": sar_af_list or [d.sar_af],
            "sar_af_max": sar_af_max_list or [d.sar_af_max],
            "sar_vol_window": sar_vol_window_list or [d.sar_vol_window],
            "boll_period": boll_period_list or [d.boll_period],
            "boll_mult": boll_mult_list or [d.boll_mult],
            "lookback_for_percentile": lookback_for_percentile_list or [d.lookback_for_percentile],
            "f2_width_base": f2_width_base_list or [d.f2_width_base],
            "f2_level_invert": f2_level_invert_list or [d.f2_level_invert],
            "f2_weight_level": f2_weight_level_list or [d.f2_weight_level],
            "f2_weight_slope": f2_weight_slope_list or [d.f2_weight_slope],
            "f2_slope_scale": f2_slope_scale_list or [d.f2_slope_scale],
        }
        self.keys = list(self.space.keys())

    def __iter__(self):
        for combo in product(*[self.space[k] for k in self.keys]):
            kwargs = {k: combo[i] for i, k in enumerate(self.keys)}
            yield StrategyConfig(**kwargs)

    def size(self) -> int:
        s = 1
        for v in self.space.values():
            s *= max(1, len(v))
        return s


# ================
#   网格搜索工具
# ================
def grid_search_best(backtester: Backtester,
                     symbol: str,
                     interval: str,
                     bars: int,
                     grid: StrategyConfigGrid,
                     initial_capital: float = 10_000.0,
                     fee_rate: float = 0.0004,
                     show_progress: bool = True,
                     top_k: int = 5,
                     output_xlsx_path: Optional[str] = None) -> Tuple[BacktestResult, List[Tuple[StrategyConfig, BacktestResult]]]:
    """
    网格搜索；若给出 output_xlsx_path，所有配置的每期数据会写入“同一个 xlsx 文件”里的不同工作表。
    工作表命名：cfg1/cfg2/...，并在第一行写入该配置的概要指标（收益/回撤/手续费/交易次数等）。
    """
    results: List[Tuple[StrategyConfig, BacktestResult]] = []
    total = grid.size()
    iterator = iter(grid)
    if show_progress:
        iterator = tqdm(iterator, total=total, desc="Grid Search", unit="cfg")

    for i, cfg in enumerate(iterator, start=1):
        sheet = f"cfg{i}"
        res = backtester.run(
            symbol=symbol, interval=interval, bars=bars, cfg=cfg,
            initial_capital=initial_capital, fee_rate=fee_rate,
            show_progress=False,
            output_xlsx_path=output_xlsx_path,   # 写入同一个文件
            sheet_name=sheet                      # 每个配置一个工作表
        )

        # 在各自 sheet 顶部追加一行“概要信息”（通过在该 sheet 新增一个 Summary 表头行）
        if output_xlsx_path:
            summary = pd.DataFrame([{
                "symbol": symbol,
                "interval": interval,
                "bars_used": res.bars_used,
                "initial_capital": res.initial_capital,
                "final_equity": res.final_equity,
                "return_pct": f"{res.return_pct:.4f}",
                "max_drawdown_pct": f"{res.max_drawdown_pct:.4f}",
                "fee_paid": f"{res.fee_paid:.4f}",
                "trades": res.trades,
                "cfg": str(cfg)
            }])
            # 读回该 sheet，再在顶部拼接 Summary（保留 period_records 原结构）
            # 为简洁，这里直接另建一个汇总工作表“_summary”，集中列出各 cfg 的摘要
            _append_summary_sheet(output_xlsx_path, "_summary", sheet, summary)

        results.append((cfg, res))

    # 排序与返回
    results.sort(key=lambda x: x[1].return_pct, reverse=True)
    best_cfg, best_res = results[0]
    return best_res, results[:top_k]


def _append_summary_sheet(path: str, summary_sheet: str, sheet_name: str, summary_df: pd.DataFrame):
    """在同一 xlsx 文件里维护一个 '_summary' 工作表，累积每个 cfg 的摘要（含其 sheet 名）。"""
    summary_df = summary_df.copy()
    summary_df.insert(0, "sheet", sheet_name)
    mode = "a" if os.path.exists(path) else "w"
    if mode == "a":
        # 追加到已有 _summary（若不存在则新建）
        with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
            existing_sheets = writer.book.sheetnames
            if summary_sheet in existing_sheets:
                # 读已有，再合并（避免覆盖）
                existing = pd.read_excel(path, sheet_name=summary_sheet)
                merged = pd.concat([existing, summary_df], ignore_index=True)
                # 用 replace 写回
                with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as w2:
                    merged.to_excel(w2, index=False, sheet_name=summary_sheet)
            else:
                summary_df.to_excel(writer, index=False, sheet_name=summary_sheet)
    else:
        with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
            summary_df.to_excel(writer, index=False, sheet_name=summary_sheet)


# =================
#      使用示例
# =================
if __name__ == "__main__":
    SYMBOL = "BTCUSDT"
    INTERVAL = "15m"
    BARS = 1800
    INITIAL = 10_000.0
    FEE = 0.0004

    # 单次回测并导出
    bt = Backtester()
    cfg = StrategyConfig()
    res = bt.run(
        symbol=SYMBOL, interval=INTERVAL, bars=BARS, cfg=cfg,
        initial_capital=INITIAL, fee_rate=FEE,
        show_progress=True,
        output_xlsx_path="single_backtest.xlsx",   # 单次回测输出
        sheet_name=f"{SYMBOL}_{INTERVAL}"
    )
    print(f"[{SYMBOL} {INTERVAL}] 终值 {res.final_equity:.2f}  收益 {res.return_pct:.2f}%  回撤 {res.max_drawdown_pct:.2f}%")

    # 网格搜索并在同一文件中多表输出
    grid = StrategyConfigGrid(
        ema_periods_list=[(20,30,60), (10,20,60)],
        f2_weight_level_list=[0.6, 0.7],
        f2_weight_slope_list=[0.4, 0.3],
        lookback_for_percentile_list=[400, 500],
    )
    best_res, top_list = grid_search_best(
        backtester=bt,
        symbol=SYMBOL, interval=INTERVAL, bars=BARS, grid=grid,
        initial_capital=INITIAL, fee_rate=FEE,
        show_progress=True,
        top_k=3,
        output_xlsx_path="grid_backtest.xlsx"      # 同一 xlsx 的多工作表
    )
    print("Grid 最优收益: %.2f%%  终值: %.2f" % (best_res.return_pct, best_res.final_equity))
