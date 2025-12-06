# quant_info_module.py
import logging
from typing import Any, Dict, List, Optional
import time
import requests
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
    # ---------------------------
    # 交换所规则缓存
    # ---------------------------
    def _refresh_exchange_info(self, force: bool = False) -> None:
        """
        缓存/刷新 USDT-M Futures 的 exchangeInfo。
        """
        if self._exchange_info is not None and not force:
            return
        try:
            self._exchange_info = self.client.futures_exchange_info()
            n = len(self._exchange_info.get("symbols", [])) if isinstance(self._exchange_info, dict) else 0
            self.logger.debug("已刷新 Futures exchangeInfo，symbols=%d", n)
        except BinanceAPIException as e:
            self.logger.error("获取 futures_exchange_info 失败：%s", e)
            self._exchange_info = None
        except Exception as e:
            self.logger.exception("获取 futures_exchange_info 异常：%s", e)
            self._exchange_info = None

    # ---------------------------
    # 函数1：获取所有可交易的合约对名
    # ---------------------------
    def get_trading_symbols(self, contract_type: Optional[str] = None) -> List[str]:
        """
        返回当前可交易（status=='TRADING'）的 USDT-M 合约对列表。
        参数 contract_type 可选：'PERPETUAL' | 'CURRENT_MONTH' | 'NEXT_MONTH'
        不传则不过滤类型。
        """
        self._refresh_exchange_info()
        if not self._exchange_info:
            return []
        symbols = []
        for s in self._exchange_info.get("symbols", []):
            if s.get("status") != "TRADING":
                continue
            if contract_type and s.get("contractType") != contract_type:
                continue
            # e.g. 'BTCUSDT'
            sym = s.get("symbol")
            if sym:
                symbols.append(sym)
        symbols.sort()
        return symbols

    # ---------------------------
    # 函数2：给定合约对，获取当前资金费率
    # ---------------------------
    def get_current_funding_rate(self, symbol: str) -> Optional[float]:
        """
        使用 fapi/v1/premiumIndex 获取 lastFundingRate。
        返回 float；若获取失败返回 None。
        """
        try:
            data = self.client.futures_mark_price(symbol=symbol.upper())
            # data 包含: symbol, markPrice, lastFundingRate, nextFundingTime, time, ...
            rate = data.get("lastFundingRate")
            return float(rate) if rate is not None else None
        except BinanceAPIException as e:
            self.logger.error("获取当前资金费率失败 %s：%s", symbol, e)
            return None
        except Exception as e:
            self.logger.exception("获取当前资金费率异常 %s：%s", symbol, e)
            return None

    # ---------------------------
    # 函数3：给定合约对，获取过去的资金费率
    # ---------------------------
    def get_funding_rate_history(
        self,
        symbol: str,
        limit: int = 100,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        使用 fapi/v1/fundingRate 获取历史资金费率。
        时间单位毫秒（ms Unix epoch）。Binance 限制最多 1000 条；默认 100。
        返回列表，每个元素含: symbol, fundingRate(str), fundingTime(int), ...
        （已将 fundingRate 转为 float 便于后续计算）
        """
        time.sleep(0.5)
        params: Dict[str, Any] = {"symbol": symbol.upper(), "limit": limit}
        if start_time is not None:
            params["startTime"] = int(start_time)
        if end_time is not None:
            params["endTime"] = int(end_time)

        try:
            rows = self.client.futures_funding_rate(**params) or []
            for r in rows:
                fr = r.get("fundingRate")
                # 转 float，若失败则原样保留
                try:
                    r["fundingRate"] = float(fr) if fr is not None else None
                except Exception:
                    pass
            return rows
        except BinanceAPIException as e:
            self.logger.error("获取资金费率历史失败 %s：%s", symbol, e)
            return []
        except Exception as e:
            self.logger.exception("获取资金费率历史异常 %s：%s", symbol, e)
            return []

    def usdc_usdt_funding_diff_table(
        self,
        hours_24: int = 24,
        days_7: int = 7,
        sleep_sec: float = 0.1,
        export_path: Optional[str] = None,   # e.g. "funding_diff.csv" 或 "funding_diff.xlsx"
    ):
        """
        汇总同一币种 USDC 与 USDT 永续合约的资金费率，并计算
        - 当前差值 (USDC - USDT)
        - 近24小时均值差值 (USDC - USDT)
        - 近7天均值差值 (USDC - USDT)

        注意：
        - 排除特例 'USDCUSDT'
        - 仅统计 status == 'TRADING' 且 contractType == 'PERPETUAL' 的合约
        """
        import time
        from statistics import mean

        try:
            import pandas as pd  # 可选：用于表格/导出
        except Exception:
            pd = None

        self._refresh_exchange_info()
        info = self._exchange_info or {}
        symbols_meta = info.get("symbols", [])

        # 1) 找出所有 USDC 永续合约（TRADING），排除 USDCUSDT
        usdc_syms = []
        usdt_set = set()
        for s in symbols_meta:
            try:
                if s.get("status") != "TRADING":
                    continue
                if s.get("contractType") != "PERPETUAL":
                    continue
                # 有些版本是 quoteAsset 字段，有些也可通过 symbol 后缀判断
                sym = s.get("symbol", "")
                quote = s.get("quoteAsset", "")
                is_usdc_quote = (quote == "USDC") or sym.endswith("USDC")
                if not is_usdc_quote:
                    continue
                if sym == "USDCUSDT":  # 特例剔除
                    continue
                usdc_syms.append(sym)
                usdt_set.add(sym[:-4] + "USDT")  # 先记录可能的对应 USDT
            except Exception:
                continue

        # 2) 在交易所元数据里确认对应 USDT 永续合约是否存在且 TRADING
        valid_usdt = set()
        meta_by_symbol = {s.get("symbol"): s for s in symbols_meta}
        for cand in list(usdt_set):
            m = meta_by_symbol.get(cand)
            if m and m.get("status") == "TRADING" and m.get("contractType") == "PERPETUAL":
                valid_usdt.add(cand)

        # 3) 逐一拉取当前资金费率 & 历史资金费率均值并汇总
        now_ms = int(time.time() * 1000)
        win_24_ms = hours_24 * 60 * 60 * 1000
        win_7d_ms = days_7 * 24 * 60 * 60 * 1000

        rows = []
        for usdc_sym in sorted(usdc_syms):
            usdt_sym = usdc_sym[:-4] + "USDT"
            if usdt_sym not in valid_usdt:
                self.logger.debug("跳过 %s：未找到对应 USDT 永续 %s", usdc_sym, usdt_sym)
                continue

            # 当前资金费率
            fr_now_usdc = self.get_current_funding_rate(usdc_sym)
            time.sleep(sleep_sec)
            fr_now_usdt = self.get_current_funding_rate(usdt_sym)
            time.sleep(sleep_sec)

            # 历史均值 - 24h
            hist_usdc_24 = self.get_funding_rate_history(
                usdc_sym, start_time=now_ms - win_24_ms, end_time=now_ms, limit=1000
            )
            time.sleep(sleep_sec)
            hist_usdt_24 = self.get_funding_rate_history(
                usdt_sym, start_time=now_ms - win_24_ms, end_time=now_ms, limit=1000
            )
            time.sleep(sleep_sec)

            def _avg(arr):
                vals = [x.get("fundingRate") for x in arr if isinstance(x.get("fundingRate"), (int, float))]
                return mean(vals) if len(vals) > 0 else None

            avg_24_usdc = _avg(hist_usdc_24)
            avg_24_usdt = _avg(hist_usdt_24)

            # 历史均值 - 7d
            hist_usdc_7d = self.get_funding_rate_history(
                usdc_sym, start_time=now_ms - win_7d_ms, end_time=now_ms, limit=1000
            )
            time.sleep(sleep_sec)
            hist_usdt_7d = self.get_funding_rate_history(
                usdt_sym, start_time=now_ms - win_7d_ms, end_time=now_ms, limit=1000
            )
            time.sleep(sleep_sec)

            avg_7d_usdc = _avg(hist_usdc_7d)
            avg_7d_usdt = _avg(hist_usdt_7d)

            def _diff(a, b):
                if a is None or b is None:
                    return None
                return a - b  # 按需求：USDC - USDT（有符号差值）

            row = {
                "base": usdc_sym[:-4],
                "USDC_symbol": usdc_sym,
                "USDT_symbol": usdt_sym,
                "now_USDC": fr_now_usdc,
                "now_USDT": fr_now_usdt,
                "now_diff(USDC-USDT)": _diff(fr_now_usdc, fr_now_usdt),
                "avg24h_USDC": avg_24_usdc,
                "avg24h_USDT": avg_24_usdt,
                "avg24h_diff": _diff(avg_24_usdc, avg_24_usdt),
                "avg7d_USDC": avg_7d_usdc,
                "avg7d_USDT": avg_7d_usdt,
                "avg7d_diff": _diff(avg_7d_usdc, avg_7d_usdt),
            }
            rows.append(row)

        # 4) 展示/导出
        # 优先用 pandas 输出表格；否则用文本表格
        if pd is not None:
            df = pd.DataFrame(rows)
            # 按 base 排序 + 差值列更直观
            order = [
                "base", "USDC_symbol", "USDT_symbol",
                "now_USDC", "now_USDT", "now_diff(USDC-USDT)",
                "avg24h_USDC", "avg24h_USDT", "avg24h_diff",
                "avg7d_USDC", "avg7d_USDT", "avg7d_diff",
            ]
            df = df.reindex(columns=order)

            # 可选导出
            if export_path:
                if export_path.lower().endswith(".xlsx"):
                    try:
                        df.to_excel(export_path, index=False)
                        self.logger.info("已导出到 Excel：%s", export_path)
                    except Exception as e:
                        self.logger.error("导出 Excel 失败：%s", e)
                else:
                    try:
                        df.to_csv(export_path, index=False)
                        self.logger.info("已导出到 CSV：%s", export_path)
                    except Exception as e:
                        self.logger.error("导出 CSV 失败：%s", e)

            return df
        else:
            # 简单文本表格
            def fmt(x):
                if x is None:
                    return "-"
                try:
                    return f"{x:.8f}"
                except Exception:
                    return str(x)

            headers = [
                "base", "USDC_symbol", "USDT_symbol",
                "now_USDC", "now_USDT", "now_diff",
                "avg24h_USDC", "avg24h_USDT", "avg24h_diff",
                "avg7d_USDC", "avg7d_USDT", "avg7d_diff",
            ]
            col_w = {h: len(h) for h in headers}
            for r in rows:
                r_view = {
                    "base": r["base"],
                    "USDC_symbol": r["USDC_symbol"],
                    "USDT_symbol": r["USDT_symbol"],
                    "now_USDC": fmt(r["now_USDC"]),
                    "now_USDT": fmt(r["now_USDT"]),
                    "now_diff": fmt(r["now_diff(USDC-USDT)"]),
                    "avg24h_USDC": fmt(r["avg24h_USDC"]),
                    "avg24h_USDT": fmt(r["avg24h_USDT"]),
                    "avg24h_diff": fmt(r["avg24h_diff"]),
                    "avg7d_USDC": fmt(r["avg7d_USDC"]),
                    "avg7d_USDT": fmt(r["avg7d_USDT"]),
                    "avg7d_diff": fmt(r["avg7d_diff"]),
                }
                for h in headers:
                    col_w[h] = max(col_w[h], len(str(r_view[h])))

            # 打印标题
            line = " | ".join(h.ljust(col_w[h]) for h in headers)
            sep = "-+-".join("-" * col_w[h] for h in headers)
            print(line)
            print(sep)
            for r in rows:
                r_view = {
                    "base": r["base"],
                    "USDC_symbol": r["USDC_symbol"],
                    "USDT_symbol": r["USDT_symbol"],
                    "now_USDC": fmt(r["now_USDC"]),
                    "now_USDT": fmt(r["now_USDT"]),
                    "now_diff": fmt(r["now_diff(USDC-USDT)"]),
                    "avg24h_USDC": fmt(r["avg24h_USDC"]),
                    "avg24h_USDT": fmt(r["avg24h_USDT"]),
                    "avg24h_diff": fmt(r["avg24h_diff"]),
                    "avg7d_USDC": fmt(r["avg7d_USDC"]),
                    "avg7d_USDT": fmt(r["avg7d_USDT"]),
                    "avg7d_diff": fmt(r["avg7d_diff"]),
                }
                print(" | ".join(str(r_view[h]).ljust(col_w[h]) for h in headers))
            return rows



if __name__ == "__main__":
    # """
    # 简单自检：
    #   - 读取 config.json/.env
    #   - 拉取最新 2000 根 15m 的 BTCUSDT K 线（需要分批）
    #   - 展示前2/后2条
    # """
    q = QuantInfoModule("config.json")
    kls = q.get_klines("BTCUSDT", n=20000, interval="15m")
    print(f"拉取到 {len(kls)} 条")
    if kls:
        print("前两条：", kls[:2])
        print("后两条：", kls[-2:])
        print("合约过滤器：", q.get_contract_filters("BTCUSDT"))
    q = QuantInfoModule("config.json")

    # 1) 所有正在交易的永续合约对
    perp_syms = q.get_trading_symbols(contract_type="PERPETUAL")

    # 2) 当前资金费率（例如 BTCUSDT）
    cur_fr = q.get_current_funding_rate("BTCUSDT")  # e.g. 0.0001

    # 3) 历史资金费率（最近100条）
    hist = q.get_funding_rate_history("BTCUSDT", limit=100)

    # 3.1 指定时间区间（例如最近 7 天）
    import time

    now_ms = int(time.time() * 1000)
    seven_days_ms = 7 * 24 * 60 * 60 * 1000
    hist_7d = q.get_funding_rate_history("BTCUSDT", start_time=now_ms - seven_days_ms, end_time=now_ms, limit=1000)
    print()

    # 直接得到 pandas.DataFrame（若环境装了 pandas）
    df = q.usdc_usdt_funding_diff_table(export_path="funding_diff.csv")
    try:
        # 若 df 是 DataFrame：展示前几行
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            print(df.head(20).to_string(index=False))
    except Exception:
        pass