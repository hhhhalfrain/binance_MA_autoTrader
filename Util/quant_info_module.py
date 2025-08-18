"""
量化交易 - 信息模块（单线程，去掉账户/基础信息部分）
依赖：
  pip install python-binance python-dotenv

功能：
  1) 读取 .env / config.json，初始化币安 Futures 客户端（支持测试网）
  2) 维护 data[symbol][interval]['k'|'v'|'t'] 三数组结构，自动去重合并与限长
  3) 详细日志，debug 模式打印到控制台
"""

import logging
from typing import Dict, List, Any
from binance.client import Client
from binance.exceptions import BinanceAPIException
from config_manager import ConfigManager


class QuantInfoModule:
    """
    使用示例：
        qim = QuantInfoModule(config_path="config.json")
        qim.update_data()  # 首次拉取并填充数据
        ds = qim.get_data()
        print(ds)
    """

    def __init__(self, config_path: str = "config.json"):
        # 1) 加载配置与 .env
        self.config_mgr = ConfigManager(config_path)
        self.config = self.config_mgr.all()

        self._init_logger(self.config.get("debug", False))

        # 2) 初始化客户端（优先 .env）
        self.client = self._init_client_from_env_or_config(self.config)

        # 3) 运行期数据存储结构：data[symbol][interval]['k'|'v'|'t'] = list
        self.data: Dict[str, Dict[str, Dict[str, List[float]]]] = {}


    def _init_logger(self, debug: bool):
        self.logger = logging.getLogger("QuantInfoModule")
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

        fh = logging.FileHandler("quant_info_module.log", encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        self.logger.addHandler(fh)

        if debug:
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
            self.logger.addHandler(ch)

        self.logger.debug("日志系统已初始化（debug=%s）", debug)

    # -------------------------
    # 客户端初始化
    # -------------------------
    def _init_client_from_env_or_config(self, cfg: Dict[str, Any]) -> Client:

        testnet = bool(cfg.get("testnet", False))
        api_key, api_secret = self.config_mgr.get_api_keys()
        client = Client(api_key, api_secret)

        if testnet:
            futures_base = "https://testnet.binancefuture.com"
            client.FUTURES_URL = futures_base + "/fapi"
            if hasattr(client, "futures_api_url"):
                client.futures_api_url = client.FUTURES_URL
            self.logger.info("已切换到 Binance Futures 测试网：%s", client.FUTURES_URL)
        else:
            self.logger.info("使用 Binance Futures 正式网。")

        return client

    # -------------------------
    # 数据维护：K线/成交量
    # -------------------------
    def _ensure_symbol_interval_keys(self, symbol: str, interval: str):
        if symbol not in self.data:
            self.data[symbol] = {}
        if interval not in self.data[symbol]:
            self.data[symbol][interval] = {"k": [], "v": [], "t": []}

    @staticmethod
    def _merge_klines_triplets(
        cur_t: List[int],
        cur_k: List[float],
        cur_v: List[float],
        new_rows: List[List[Any]],
        max_k: int
    ):
        merged: Dict[int, Dict[str, float]] = {int(t): {"k": float(k), "v": float(v)} for t, k, v in zip(cur_t, cur_k, cur_v)}

        for row in new_rows:
            try:
                open_time = int(row[0])
                close_price = float(row[4])
                volume = float(row[5])
            except Exception:
                continue
            merged[open_time] = {"k": close_price, "v": volume}

        all_times = sorted(merged.keys())
        if len(all_times) > max_k:
            all_times = all_times[-max_k:]

        new_t, new_k, new_v = [], [], []
        for t in all_times:
            new_t.append(t)
            new_k.append(merged[t]["k"])
            new_v.append(merged[t]["v"])

        cur_t[:] = new_t
        cur_k[:] = new_k
        cur_v[:] = new_v

    def update_data(self):
        symbols = list(self.config.get("symbols", []))
        intervals = list(self.config.get("intervals", []))
        max_k = int(self.config.get("max_k", 100))

        if not symbols or not intervals:
            self.logger.warning("symbols 或 intervals 为空，无法拉取数据。")
            return

        for sym in symbols:
            for itv in intervals:
                self._ensure_symbol_interval_keys(sym, itv)
                cur_triplet = self.data[sym][itv]
                cur_t, cur_k, cur_v = cur_triplet["t"], cur_triplet["k"], cur_triplet["v"]

                try:
                    limit = max(200, max_k) if len(cur_t) == 0 else 200
                    rows = self.client.futures_klines(symbol=sym, interval=itv, limit=limit)
                    if not rows:
                        self.logger.info("[KLINE] %s %s 无返回数据。", sym, itv)
                        continue
                    self._merge_klines_triplets(cur_t, cur_k, cur_v, rows, max_k)
                    self.logger.debug("[KLINE] %s %s 合并后长度=%d", sym, itv, len(cur_t))
                except BinanceAPIException as e:
                    self.logger.error("[KLINE] %s %s 拉取失败：%s", sym, itv, e)
                    continue
                except Exception as e:
                    self.logger.exception("[KLINE] %s %s 合并异常：%s", sym, itv, e)
                    continue

        self.logger.info("update_data 完成。")

    # -------------------------
    # 对外接口
    # -------------------------
    def get_data(self) -> Dict[str, Dict[str, Dict[str, List[float]]]]:
        return self.data


if __name__ == "__main__":
    qim = QuantInfoModule(config_path="config.json")
    qim.update_data()
    ds = qim.get_data()
    print("已有数据的品种与周期：", list(ds.keys()))
