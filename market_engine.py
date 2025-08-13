# market_engine.py  —— Futures Only
import os
import time
import threading
from collections import deque
from datetime import datetime
from typing import Dict, Deque, Tuple, List
import queue
import logging

from binance.client import Client
from binance import ThreadedWebsocketManager

MAX_LEN = 50
MIN_MA_LEN = 28

class MarketDataEngine:
    """
    仅支持币安 U 本位合约（UM Futures）：
      - 历史：futures_klines
      - 实时：start_kline_futures_socket
      - WebSocket: ThreadedWebsocketManager（futures/testnet 由 Client 构造参数决定）
    """
    def __init__(self,
                 symbols: List[str],
                 intervals: List[str],
                 api_key: str,
                 api_secret: str,
                 max_len: int = MAX_LEN,
                 testnet: bool = False):
        self.symbols = [s.upper() for s in symbols]
        self.intervals = intervals
        self.client = Client(api_key, api_secret, testnet=testnet)

        # 仅用于 Futures 的 WS 管理器
        self.twm = ThreadedWebsocketManager(
            api_key=api_key,
            api_secret=api_secret,
            testnet=testnet
        )

        # {symbol: {interval: deque[(closeTimeMS, closePrice), ...]}}
        self.klines_data: Dict[str, Dict[str, Deque[Tuple[int, float]]]] = {
            s: {i: deque(maxlen=max_len) for i in intervals} for s in self.symbols
        }

        self.lock = threading.RLock()
        self.event_queue: queue.Queue = queue.Queue(maxsize=1000)
        self._running = False

        # 就绪状态（是否≥28根）
        self._min_len = MIN_MA_LEN
        self._ready = {s: {iv: False for iv in self.intervals} for s in self.symbols}

        # 日志
        self.logger = logging.getLogger("MarketDataEngine")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
            self.logger.addHandler(ch)

    # ---------- 工具 ----------
    def is_ready(self, symbol: str, interval: str) -> bool:
        return self._ready.get(symbol.upper(), {}).get(interval, False)

    def ready_report(self, symbol: str) -> str:
        s = symbol.upper()
        lines = [f"[Engine Ready] {s}"]
        with self.lock:
            for iv in self.intervals:
                buf = self.klines_data[s][iv]
                rdy = len(buf) >= self._min_len
                last_ts = buf[-1][0] if buf else None
                last_str = datetime.fromtimestamp(last_ts/1000).strftime("%Y-%m-%d %H:%M:%S") if last_ts else "None"
                lines.append(f"{iv:>7}: len={len(buf):>3} ready={str(rdy):5} last={last_str}")
        return "\n".join(lines)

    @staticmethod
    def calc_ma_from_closes(closes: List[float], period: int):
        if len(closes) < period:
            return None
        return sum(closes[-period:]) / period

    def compute_ma_values(self, symbol: str, interval: str):
        with self.lock:
            closes = [c for _, c in self.klines_data[symbol][interval]]
        ma7  = self.calc_ma_from_closes(closes, 7)
        ma14 = self.calc_ma_from_closes(closes, 14)
        ma28 = self.calc_ma_from_closes(closes, 28)
        return ma7, ma14, ma28

    # ---------- 事件 ----------
    def put_event(self, event: dict, block: bool = False, timeout: float = 0.1):
        try:
            self.event_queue.put(event, block=block, timeout=timeout)
        except queue.Full:
            self.logger.warning("event_queue is full, drop event")

    def _update_and_publish(self, symbol: str, interval: str, ts: int, close_price: float):
        with self.lock:
            dq = self.klines_data[symbol][interval]
            dq.append((ts, close_price))
            self._ready[symbol][interval] = (len(dq) >= self._min_len)

        self.put_event({
            "etype": "kline_close",
            "symbol": symbol,
            "interval": interval,
            "timestamp": ts,
            "close": close_price,
            "time_str": datetime.fromtimestamp(ts/1000).strftime("%Y-%m-%d %H:%M:%S")
        }, block=False)

    # ---------- 历史回填（仅合约） ----------
    def fetch_initial_data(self, limit: int = None):
        limit = limit or max(MAX_LEN, MIN_MA_LEN)
        for sym in self.symbols:
            for interval in self.intervals:
                try:
                    raw = self.client.futures_klines(symbol=sym, interval=interval, limit=limit)
                    with self.lock:
                        dq = self.klines_data[sym][interval]
                        dq.clear()
                        for k in raw:
                            ts = int(k[6])      # close time ms (UTC)
                            close = float(k[4]) # close price
                            dq.append((ts, close))
                        self._ready[sym][interval] = (len(dq) >= self._min_len)
                    self.logger.info(f"[init] {sym} {interval} loaded {len(raw)} bars (futures)")
                except Exception as e:
                    self.logger.error(f"[init] error fetching {sym} {interval}: {e}")

    # ---------- WS 回调（仅合约） ----------
    def _on_kline_message(self, msg):
        try:
            if msg.get("e") != "kline":
                return
            k = msg["k"]
            if not k.get("x", False):  # 只在收盘时触发
                return
            symbol = msg.get("s", "").upper()
            interval = k.get("i")
            if symbol not in self.symbols or interval not in self.intervals:
                return
            ts = int(k.get("T"))
            close_price = float(k.get("c"))
            self._update_and_publish(symbol, interval, ts, close_price)
            # 可选：打点
            # self.logger.info(f"[ws] close {symbol} {interval} {datetime.fromtimestamp(ts/1000)} cp={close_price}")
        except Exception as e:
            self.logger.error(f"[err] on_kline_message: {e}")

    # ---------- 生命周期 ----------
    def start(self):
        if self._running:
            return
        self._running = True
        self.fetch_initial_data()

        # 启动 WS（合约流）
        self.twm.start()  # futures/testnet 由构造参数决定
        for sym in self.symbols:
            for iv in self.intervals:
                self.twm.start_kline_futures_socket(
                    callback=self._on_kline_message,
                    symbol=sym,
                    interval=iv
                )
        self.logger.info("[engine] Futures WebSocket started")

    def stop(self):
        if not self._running:
            return
        try:
            self.twm.stop()
        except Exception:
            pass
        self._running = False

    # ---------- 辅助 ----------
    def get_latest_closes(self, symbol: str, interval: str) -> List[Tuple[int, float]]:
        with self.lock:
            return list(self.klines_data[symbol][interval])
