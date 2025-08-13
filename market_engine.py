# market_engine.py —— Futures Only + Watchdog + Poller
import time
import threading
from collections import deque
from datetime import datetime
from typing import Dict, Deque, Tuple, List
import queue
import logging

from binance.client import Client
from binance import ThreadedWebsocketManager

# 本地缓冲长度与 MA 最小需求
MAX_LEN = 50
MIN_MA_LEN = 28

# 周期秒数映射（看门狗判断与节流用）
_INTERVAL_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800, "12h": 43200,
    "1d": 86400, "3d": 259200, "1w": 604800, "1M": 2592000
}

class MarketDataEngine:
    """
    币安 U 本位合约（UM Futures）行情引擎：
      - 历史K线: futures_klines
      - 实时K线: start_kline_futures_socket（symbol 必须小写）
      - 看门狗: 监控每条流最后事件时间，超时自动重连
      - 兜底轮询: 固定间隔主动拉最新一根收盘K线，避免 WS 抖动导致指标停更
    线程安全：所有写缓冲的操作均加锁（RLock）。
    """
    def __init__(self,
                 symbols: List[str],
                 intervals: List[str],
                 api_key: str,
                 api_secret: str,
                 max_len: int = MAX_LEN,
                 testnet: bool = False,
                 poll_interval_sec: int = 10):
        # 统一大写供上层使用，小写用于WS订阅
        self.symbols = [s.upper() for s in symbols]
        self.ws_symbols = [s.lower() for s in self.symbols]
        self.intervals = intervals

        self.client = Client(api_key, api_secret, testnet=testnet)
        self.twm = ThreadedWebsocketManager(
            api_key=api_key,
            api_secret=api_secret,
            testnet=testnet
        )

        # {symbolUpper: {interval: deque[(closeTimeMS, closePrice), ...]}}
        self.klines_data: Dict[str, Dict[str, Deque[Tuple[int, float]]]] = {
            s: {iv: deque(maxlen=max_len) for iv in intervals} for s in self.symbols
        }

        self.lock = threading.RLock()
        self.event_queue: queue.Queue = queue.Queue(maxsize=2000)
        self._running = False

        # 就绪状态（达到至少 28 根）
        self._min_len = MIN_MA_LEN
        self._ready = {s: {iv: False for iv in self.intervals} for s in self.symbols}

        # 订阅管理
        # conn_keys[(symbol_lower, interval)] = conn_key
        self.conn_keys: Dict[Tuple[str, str], str] = {}
        # 最近一次收到该流的事件时间戳（秒）
        self.last_event_at: Dict[Tuple[str, str], float] = {}

        # 轮询兜底配置
        self._poll_interval = max(5, int(poll_interval_sec))
        self._poll_stop = threading.Event()
        self._poller = threading.Thread(target=self._poll_latest_loop, daemon=True)

        # 看门狗线程
        self._wd_stop = threading.Event()
        self._watchdog = threading.Thread(target=self._watchdog_loop, daemon=True)

        # 日志
        self.logger = logging.getLogger("MarketDataEngine")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
            self.logger.addHandler(ch)

    # ------------------- 工具/状态 -------------------
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

    def wait_ready(self, symbol: str, timeout: float = 10.0) -> bool:
        """等待某 symbol 的所有 interval 都达到 MA 计算所需的最少根数；超时返回 False。"""
        s = symbol.upper()
        deadline = time.time() + timeout
        while time.time() < deadline:
            all_ok = True
            with self.lock:
                for iv in self.intervals:
                    if len(self.klines_data[s][iv]) < self._min_len:
                        all_ok = False
                        break
            if all_ok:
                return True
            time.sleep(0.1)
        return False

    @staticmethod
    def calc_ma_from_closes(closes: List[float], period: int):
        if len(closes) < period:
            return None
        return sum(closes[-period:]) / period

    def compute_ma_values(self, symbol: str, interval: str):
        with self.lock:
            closes = [c for _, c in self.klines_data[symbol][interval]]
        ma7 = self.calc_ma_from_closes(closes, 7)
        ma14 = self.calc_ma_from_closes(closes, 14)
        ma28 = self.calc_ma_from_closes(closes, 28)
        return ma7, ma14, ma28

    def get_latest_closes(self, symbol: str, interval: str) -> List[Tuple[int, float]]:
        with self.lock:
            return list(self.klines_data[symbol][interval])

    # ------------------- 事件 -------------------
    def put_event(self, event: dict, block: bool = False, timeout: float = 0.05):
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
            "symbol": symbol,               # 大写
            "interval": interval,
            "timestamp": ts,
            "close": close_price,
            "time_str": datetime.fromtimestamp(ts/1000).strftime("%Y-%m-%d %H:%M:%S")
        }, block=False)

    # ------------------- 历史回填（仅合约） -------------------
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
                            ts = int(k[6])      # close time (ms)
                            close = float(k[4]) # close price
                            dq.append((ts, close))
                        self._ready[sym][interval] = (len(dq) >= self._min_len)
                    self.logger.info(f"[init] {sym} {interval} loaded {len(raw)} bars (futures)")
                except Exception as e:
                    self.logger.error(f"[init] error fetching {sym} {interval}: {e}")

    # ------------------- WS 回调（仅合约） -------------------
    def _on_kline_message(self, msg):
        try:
            if msg.get("e") != "kline":
                return
            k = msg.get("k", {})
            # 只在收盘触发
            if not k.get("x"):
                return
            symbol_u = msg.get("s", "").upper()
            interval = k.get("i", "")
            # 更新该流活动时间
            self.last_event_at[(symbol_u.lower(), interval)] = time.time()

            if symbol_u not in self.symbols or interval not in self.intervals:
                return
            ts = int(k.get("T"))
            close_price = float(k.get("c"))
            self._update_and_publish(symbol_u, interval, ts, close_price)
            # 如需更细粒度日志可打开：
            # self.logger.info(f"[ws] {symbol_u} {interval} closed @{datetime.fromtimestamp(ts/1000)} = {close_price}")
        except Exception as e:
            self.logger.error(f"[err] on_kline_message: {e}")

    # ------------------- 看门狗：自动重连 -------------------
    def _interval_sec(self, iv: str) -> int:
        return _INTERVAL_SECONDS.get(iv, 60)

    def _subscribe_one(self, symbol_lower: str, interval: str):
        # 停旧
        old_key = self.conn_keys.get((symbol_lower, interval))
        if old_key:
            try:
                self.twm.stop_socket(old_key)
                self.logger.info(f"[ws] stop old socket: {symbol_lower} {interval}")
            except Exception:
                pass
        # 订新
        new_key = self.twm.start_kline_futures_socket(
            callback=self._on_kline_message,
            symbol=symbol_lower,
            interval=interval
        )
        self.conn_keys[(symbol_lower, interval)] = new_key
        self.last_event_at[(symbol_lower, interval)] = time.time()
        self.logger.info(f"[ws] subscribed: {symbol_lower} {interval} -> {new_key}")

    def _watchdog_loop(self):
        self.logger.info("[watchdog] started")
        while not self._wd_stop.is_set():
            try:
                now = time.time()
                for s_lower in self.ws_symbols:
                    for iv in self.intervals:
                        key = (s_lower, iv)
                        last = self.last_event_at.get(key, 0.0)
                        # 容忍 2×周期 + 10s 无消息则重订阅
                        thresh = 2 * self._interval_sec(iv) + 10
                        if last > 0 and (now - last) > thresh:
                            self.logger.warning(f"[watchdog] stale stream -> resubscribe {s_lower} {iv} (idle={int(now-last)}s)")
                            self._subscribe_one(s_lower, iv)
                time.sleep(30)
            except Exception as e:
                self.logger.error(f"[watchdog] error: {e}")
                time.sleep(5)

    # ------------------- 兜底轮询：强制推进最新收盘K -------------------
    def _poll_latest_loop(self):
        self.logger.info("[poller] started")
        while not self._poll_stop.is_set():
            try:
                for sym in self.symbols:
                    for iv in self.intervals:
                        # 本地最后一根
                        with self.lock:
                            dq = self.klines_data[sym][iv]
                            last_ts_local = dq[-1][0] if dq else 0

                        # 拉取最新一根合约K线
                        try:
                            raw = self.client.futures_klines(symbol=sym, interval=iv, limit=1)
                        except Exception as e:
                            self.logger.warning(f"[poller] fetch {sym} {iv} error: {e}")
                            continue
                        if not raw:
                            continue

                        ts_remote = int(raw[0][6])      # close time
                        close_remote = float(raw[0][4]) # close price

                        # 远端比本地新 -> 注入 & 广播事件
                        if ts_remote > last_ts_local:
                            self._update_and_publish(sym, iv, ts_remote, close_remote)
                            # 更新活动时间，避免 watchdog 误判
                            self.last_event_at[(sym.lower(), iv)] = time.time()
                            self.logger.info(f"[poller] inject {sym} {iv} close={close_remote} @ {datetime.fromtimestamp(ts_remote/1000)}")
            except Exception as e:
                self.logger.error(f"[poller] loop error: {e}")

            time.sleep(self._poll_interval)

    # ------------------- 生命周期 -------------------
    def start(self):
        if self._running:
            return
        self._running = True

        # 先回填历史
        self.fetch_initial_data()

        # 启动 WS
        self.twm.start()
        for s_lower in self.ws_symbols:
            for iv in self.intervals:
                self._subscribe_one(s_lower, iv)
        self.logger.info(f"[engine] Futures WS started, streams={len(self.conn_keys)}")

        # 看门狗
        if not self._watchdog.is_alive():
            self._wd_stop.clear()
            self._watchdog = threading.Thread(target=self._watchdog_loop, daemon=True)
            self._watchdog.start()

        # 轮询兜底
        if not self._poller.is_alive():
            self._poll_stop.clear()
            self._poller = threading.Thread(target=self._poll_latest_loop, daemon=True)
            self._poller.start()

    def stop(self):
        if not self._running:
            return
        try:
            self._wd_stop.set()
        except Exception:
            pass
        try:
            self._poll_stop.set()
        except Exception:
            pass
        try:
            for ck in list(self.conn_keys.values()):
                try:
                    self.twm.stop_socket(ck)
                except Exception:
                    pass
            self.conn_keys.clear()
            self.twm.stop()
        except Exception:
            pass
        self._running = False
        self.logger.info("[engine] stopped")
