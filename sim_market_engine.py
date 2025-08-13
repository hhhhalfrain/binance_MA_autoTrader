# sim_market_engine.py
import time
import threading
import queue
from collections import deque
from datetime import datetime
import random
from typing import Dict, List, Tuple, Deque, Optional

MIN_MA_LEN = 28
MAX_LEN = 50

def _ma(arr: List[float], n: int) -> Optional[float]:
    if len(arr) < n:
        return None
    return sum(arr[-n:]) / n

class SimMarketDataEngine:
    """
    仅用于本地模拟行情：
      - 按 symbols/intervals 往 event_queue 推送“已收盘K线事件”
      - 内部维护 closes，用于 compute_ma_values()
      - 不依赖任何交易所客户端
    """
    def __init__(
        self,
        symbols: List[str],
        intervals: List[str],
        base_price: Dict[str, float] = None,
        profile_map: Dict[str, str] = None,
        tick_interval_sec: float = 0.5,
        ticks_per_interval: Dict[str, int] = None,
        seed: int = 1234,
    ):
        """
        symbols: 例如 ["BTCUSDT","ETHUSDT"]
        intervals: 例如 ["5m","15m","1h","4h","1d"]
        base_price: 每个symbol的初始价格
        profile_map: 每个symbol的走势画像 {'BTCUSDT':'bull','ETHUSDT':'bear'|'neutral'}
        tick_interval_sec: 模拟时钟每tick的间隔（秒）
        ticks_per_interval: 各周期多少tick出一根K线（为了加速测试，默认很小）
        """
        self.symbols = [s.upper() for s in symbols]
        self.intervals = intervals
        self.base_price = base_price or {}
        self.profile_map = {s.upper(): (profile_map.get(s.upper(), 'neutral') if profile_map else 'neutral')
                            for s in self.symbols}
        self.tick_interval_sec = tick_interval_sec
        # 为了测试更快看到效果，这里设置较小的tick倍数（可自行调大）
        self.ticks_per_interval = ticks_per_interval or {
            "5m": 3, "15m": 5, "1h": 12, "4h": 24, "1d": 48
        }

        random.seed(seed)

        # 内存数据：closes[symbol][interval] = deque([float,...], maxlen=50)
        self.closes: Dict[str, Dict[str, Deque[float]]] = {
            s: {iv: deque(maxlen=MAX_LEN) for iv in self.intervals} for s in self.symbols
        }
        # 上一价格（用于随机游走）
        self.last_price: Dict[str, float] = {s: float(self.base_price.get(s, 1000.0)) for s in self.symbols}
        # 每个 interval 的 tick 计数器
        self._tick_count: Dict[str, int] = {iv: 0 for iv in self.intervals}
        # 事件队列（供 TradingManager 消费）
        self.event_queue: queue.Queue = queue.Queue(maxsize=10000)

        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)

        # 启动前先暖机，保证 MA 可用
        self._warmup_all()

    # --------- 外部接口 ---------
    def start(self):
        if not self._thread.is_alive():
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._stop.clear()
            self._thread.start()

    def stop(self):
        self._stop.set()

    def compute_ma_values(self, symbol: str, interval: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        s = symbol.upper()
        arr = list(self.closes[s][interval])
        ma7 = _ma(arr, 7)
        ma14 = _ma(arr, 14)
        ma28 = _ma(arr, 28)
        return ma7, ma14, ma28

    # --------- 内部：行情生成 ---------
    def _warmup_all(self):
        """每个 symbol/interval 预先填充 ≥ MIN_MA_LEN 根"""
        for s in self.symbols:
            # 先生成一条连续价格序列作为“母线”，再复制到各 interval
            seq = self._gen_series_for_profile(s, length=MIN_MA_LEN)
            for iv in self.intervals:
                self.closes[s][iv].clear()
                self.closes[s][iv].extend(seq)

    def _gen_series_for_profile(self, symbol: str, length: int) -> List[float]:
        """根据 bull/bear/neutral 画像生成 length 个连续收盘价"""
        p = self.profile_map.get(symbol, 'neutral')
        price = self.last_price[symbol]
        out = []
        for i in range(length):
            if p == 'bull':
                drift = 0.0008   # 向上微漂移
                vol = 0.0025
            elif p == 'bear':
                drift = -0.0008  # 向下微漂移
                vol = 0.0025
            else:  # neutral
                drift = 0.0
                vol = 0.0030
            shock = random.gauss(0, vol)
            price = max(0.01, price * (1 + drift + shock))
            out.append(price)
        # 更新 last
        self.last_price[symbol] = price
        return out

    def _next_price(self, symbol: str) -> float:
        """单步随机游走，保持与画像一致的期望方向"""
        price = self.last_price[symbol]
        p = self.profile_map.get(symbol, 'neutral')
        if p == 'bull':
            drift = 0.0008
            vol = 0.0025
        elif p == 'bear':
            drift = -0.0008
            vol = 0.0025
        else:
            drift = 0.0
            vol = 0.0030
        shock = random.gauss(0, vol)
        price = max(0.01, price * (1 + drift + shock))
        self.last_price[symbol] = price
        return price

    def _emit_close(self, symbol: str, interval: str, close_price: float):
        """写入 deque + 推送队列事件"""
        dq = self.closes[symbol][interval]
        dq.append(close_price)
        evt = {
            "symbol": symbol,
            "interval": interval,
            "timestamp": int(time.time() * 1000),
            "time_str": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "close": float(close_price),
        }
        try:
            self.event_queue.put_nowait(evt)
        except queue.Full:
            # 丢弃最旧的一条再放（极端情况下）
            _ = self.event_queue.get_nowait()
            self.event_queue.put_nowait(evt)

    def _run_loop(self):
        """
        模拟主循环：
          - 每 tick 为每个 symbol 产生一个“最新价”
          - 各 interval 累积到对应 ticks_per_interval 就视为“收盘”，推送事件
        """
        # 每个 interval 的累积器
        acc: Dict[str, Dict[str, float]] = {s: {iv: self.closes[s][iv][-1] for iv in self.intervals}
                                            for s in self.symbols}

        while not self._stop.is_set():
            # 生成“分时最新价”
            for s in self.symbols:
                px = self._next_price(s)
                # 可扩展成OHLC，这里简化为收盘=最新价
                for iv in self.intervals:
                    acc[s][iv] = px

            # interval tick 推进
            for iv in self.intervals:
                self._tick_count[iv] += 1
                if self._tick_count[iv] >= max(1, int(self.ticks_per_interval.get(iv, 3))):
                    self._tick_count[iv] = 0
                    # 对每个 symbol 生成“收盘事件”
                    for s in self.symbols:
                        self._emit_close(s, iv, acc[s][iv])

            time.sleep(self.tick_interval_sec)
