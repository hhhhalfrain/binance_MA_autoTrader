# quant_info_module.py
import logging
from typing import Any, Dict, List, Optional

from binance.client import Client
from binance.exceptions import BinanceAPIException
from config_manager import ConfigManager  # 你之前已放在同文件夹

class QuantInfoModule:
    """
    精简版行情/合约信息模块（USDⓈ-M Futures）
    - 不维护内存里的 k 线数组
    - 缓存 futures_exchange_info 以便查询合约规则
    - 按需拉取最新 n 条 K 线，n>1500 自动分批拼接

    返回 K 线字段：
      {
        "open": float,
        "close": float,
        "high": float,       # = high
        "low": float,
        "volume": float,
        "timestamp": int    # 开盘时间戳(ms)
      }
    """

    def __init__(self, config_path: str = "config.json"):
        # 1) 配置 & 日志
        self.cfg = ConfigManager(config_path)
        self._init_logger(self.cfg.get("debug", True))

        # 2) 初始化 Binance Futures 客户端（支持测试网）
        api_key, api_secret = self.cfg.get_api_keys()
        self.client = Client(api_key, api_secret)
        if self.cfg.get("testnet", False):
            # python-binance 各版本字段命名可能不同，尽量兼容
            base = "https://testnet.binancefuture.com"
            self.client.FUTURES_URL = base + "/fapi"
            if hasattr(self.client, "futures_api_url"):
                self.client.futures_api_url = self.client.FUTURES_URL
            self.logger.info("已切换到 Futures 测试网：%s", getattr(self.client, "FUTURES_URL", ""))
        else:
            self.logger.info("使用 Futures 正式网。")

        # 3) 合约规则缓存
        self._exchange_info: Optional[Dict[str, Any]] = None
        self._refresh_exchange_info()

    # ---------------------------
    # 日志
    # ---------------------------
    def _init_logger(self, debug: bool):
        self.logger = logging.getLogger("QuantInfoModule")
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

        # 文件日志
        fh = logging.FileHandler("quant_info_module.log", encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        self.logger.addHandler(fh)

        # 控制台（debug 才开）
        if debug:
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
            self.logger.addHandler(ch)

        self.logger.debug("日志初始化完成（debug=%s）", debug)

    # ---------------------------
    # 合约规则缓存
    # ---------------------------
    def _refresh_exchange_info(self, force: bool = False):
        if self._exchange_info is not None and not force:
            return
        try:
            self._exchange_info = self.client.futures_exchange_info()
            self.logger.debug("已缓存 futures_exchange_info。")
        except Exception as e:
            self._exchange_info = None
            self.logger.error("获取 futures_exchange_info 失败：%s", e)

    def get_contract_filters(self, symbol: str) -> Optional[Dict[str, Dict[str, Any]]]:
        """
        返回该合约常用过滤器（LOT_SIZE/PRICE_FILTER/MARKET_LOT_SIZE）。
        示例：
        {
          "LOT_SIZE": {...},
          "PRICE_FILTER": {...},
          "MARKET_LOT_SIZE": {...}
        }
        """
        self._refresh_exchange_info()
        info = self._exchange_info
        if not info:
            return None
        for s in info.get("symbols", []):
            if s.get("symbol") == symbol:
                out: Dict[str, Dict[str, Any]] = {}
                for f in s.get("filters", []):
                    ftype = f.get("filterType")
                    if ftype in ("LOT_SIZE", "PRICE_FILTER", "MARKET_LOT_SIZE"):
                        out[ftype] = {k: v for k, v in f.items() if k != "filterType"}
                return out
        return None

    # ---------------------------
    # 拉取最新 n 条 K 线（n>1500 自动拼接）
    # ---------------------------
    def get_klines(self, symbol: str, n: int, interval: str) -> List[Dict[str, Any]]:
        """
        获取最新 n 条 USDⓈ-M 合约 K 线，按时间升序返回。
        币安单次最多 1500 条；若 n>1500 自动分批，向更早历史拼接。

        参数：
          symbol   : 如 "BTCUSDT"
          n        : 需要的条数（>0）
          interval : 如 "15m","1h","4h","1d" 等

        返回：list[dict]，每个元素：
          {"open":..., "close":..., "high":..., "low":..., "volume":..., "timestamp":...}
        """
        if n <= 0:
            raise ValueError("n 必须为正整数。")

        MAX_LIMIT = 1500
        need = n
        end_time: Optional[int] = None  # ms；每轮用 endTime 向更早历史拉取
        out_rows: List[List[Any]] = []

        while need > 0:
            limit = MAX_LIMIT if need > MAX_LIMIT else need
            params = {"symbol": symbol, "interval": interval, "limit": limit}
            if end_time is not None:
                params["endTime"] = end_time

            try:
                batch = self.client.futures_klines(**params)
            except BinanceAPIException as e:
                self.logger.error("[KLINE] 拉取失败 %s %s：%s", symbol, interval, e)
                break
            except Exception as e:
                self.logger.exception("[KLINE] 异常 %s %s：%s", symbol, interval, e)
                break

            if not batch:
                # 无更多历史
                break

            # 记录并准备下一轮 endTime（取本批最早开盘时间 - 1ms）
            out_rows = batch + out_rows  # 先旧后新，最终顺序升序
            first_open = int(batch[0][0])
            end_time = first_open - 1
            need -= len(batch)

            # 若历史不足，已尽力拉取，跳出
            if len(batch) < limit:
                break

        if not out_rows:
            return []

        # 若拼接过多，取最后 n 条（升序）
        if len(out_rows) > n:
            out_rows = out_rows[-n:]

        # 映射为所需字段
        mapped = [self._map_row(r) for r in out_rows]
        return mapped

    @staticmethod
    def _map_row(r: List[Any]) -> Dict[str, Any]:
        """
        将币安 futures_klines 的原始数组映射为需要的字典：
        r = [openTime, open, high, low, close, volume, closeTime, ...]
        """
        try:
            return {
                "open": float(r[1]),
                "close": float(r[4]),
                "high": float(r[2]),       # = high
                "low": float(r[3]),
                "volume": float(r[5]),
                "timestamp": int(r[0]),   # 开盘时间(ms)
            }
        except Exception:
            # 若格式异常，返回空占位（也可选择抛出）
            return {
                "open": 0.0, "close": 0.0, "high": 0.0, "low": 0.0,
                "volume": 0.0, "timestamp": 0
            }


if __name__ == "__main__":
    """
    简单自检：
      - 读取 config.json/.env
      - 拉取最新 2000 根 15m 的 BTCUSDT K 线（需要分批）
      - 展示前2/后2条
    """
    q = QuantInfoModule("config.json")
    kls = q.get_klines("BTCUSDT", n=20000, interval="15m")
    print(f"拉取到 {len(kls)} 条")
    if kls:
        print("前两条：", kls[:2])
        print("后两条：", kls[-2:])
        print("合约过滤器：", q.get_contract_filters("BTCUSDT"))
