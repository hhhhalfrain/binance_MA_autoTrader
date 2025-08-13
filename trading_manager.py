# trading_manager.py
import time
import threading
import logging
from typing import Dict, Any, Optional, List, Tuple, Callable
from decimal import Decimal, ROUND_DOWN

from binance.client import Client
from binance.exceptions import BinanceAPIException

logger = logging.getLogger("TradingManager")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
logger.addHandler(ch)


def _floor_to_step(value: float, step: float) -> float:
    """按 step 向下取整，返回 float"""
    if step <= 0:
        return value
    d = Decimal(str(value)) / Decimal(str(step))
    d = d.quantize(Decimal("1."), rounding=ROUND_DOWN)
    return float(d * Decimal(str(step)))


# 默认基础权重（可在 TradingManager 初始化时覆盖）
DEFAULT_BASE_WEIGHTS = {
    "5m": 0.1,
    "15m": 0.15,
    "1h": 0.25,
    "4h": 0.25,
    "1d": 0.25,
}


class OrderTracker(threading.Thread):
    """
    跟踪委托：挂单在买一/卖一（best bid/ask），如果买一/卖一变化则移动挂单。
    一个 OrderTracker 实例管理一个活跃挂单（symbol, side）。
    """
    def __init__(self, client: Client, symbol: str, side: str, qty: float,
                 reduce_only: bool = False, poll_interval: float = 0.6, dry_run: bool = False,
                 order_params: Dict[str, Any] = None,
                 on_finish: Optional[Callable[[str, str, bool], None]] = None):
        super().__init__(daemon=True)
        self.client = client
        self.symbol = symbol
        self.side = side.upper()
        self.qty = qty
        self.reduce_only = reduce_only
        self.poll_interval = poll_interval
        self._stop = threading.Event()
        self.order_id: Optional[int] = None
        self.current_price: Optional[float] = None
        self.dry_run = dry_run
        self.order_params = order_params or {}
        self.on_finish = on_finish
        self._filled = False  # 成交标记

    def run(self):
        logger.info(f"[Tracker] start tracking {self.symbol} {self.side} qty={self.qty}")
        try:
            while not self._stop.is_set() and self.qty > 0:
                # 获取最新买一/卖一
                try:
                    ob = self.client.futures_order_book(symbol=self.symbol, limit=5)
                except Exception as e:
                    logger.error(f"[Tracker] order_book error: {e}")
                    time.sleep(self.poll_interval)
                    continue

                bids = ob.get("bids", [])
                asks = ob.get("asks", [])
                if self.side == "BUY":
                    if not bids:
                        time.sleep(self.poll_interval); continue
                    best_price = float(bids[0][0])
                else:
                    if not asks:
                        time.sleep(self.poll_interval); continue
                    best_price = float(asks[0][0])

                # 若无挂单则下限价单；若价格变动则取消并重下
                if self.order_id is None:
                    price_str = format(best_price, "f")
                    if self.dry_run:
                        logger.info(f"[Tracker DRY] Would place LIMIT {self.side} {self.qty} @ {price_str} {self.symbol}")
                        self.current_price = best_price
                        time.sleep(self.poll_interval); continue
                    try:
                        order = self.client.futures_create_order(
                            symbol=self.symbol,
                            side=self.side,
                            type="LIMIT",
                            timeInForce="GTC",
                            quantity=self.qty,
                            price=price_str,
                            reduceOnly=self.reduce_only,
                            **self.order_params
                        )
                        self.order_id = order.get("orderId")
                        self.current_price = float(price_str)
                        logger.info(f"[Tracker] placed orderId={self.order_id} {self.side} {self.qty}@{price_str} {self.symbol}")
                    except BinanceAPIException as e:
                        logger.error(f"[Tracker] place order error: {e}")
                        time.sleep(self.poll_interval); continue
                else:
                    try:
                        if self.current_price is None:
                            resp = self.client.futures_get_order(symbol=self.symbol, orderId=self.order_id)
                            self.current_price = float(resp.get("price") or 0.0)
                    except Exception:
                        # 无法获取订单信息，重置以便重下
                        self.order_id = None
                        self.current_price = None
                        time.sleep(self.poll_interval); continue

                    # 若挂单不再最优价（浮动），取消并重放
                    if (self.side == "BUY" and best_price > self.current_price + 1e-12) or \
                       (self.side == "SELL" and best_price < self.current_price - 1e-12):
                        try:
                            if self.dry_run:
                                logger.info(f"[Tracker DRY] Would cancel orderId={self.order_id}")
                                self.order_id = None; self.current_price = None
                                time.sleep(self.poll_interval); continue
                            self.client.futures_cancel_order(symbol=self.symbol, orderId=self.order_id)
                            logger.info(f"[Tracker] canceled orderId={self.order_id}, repositioning to price={best_price}")
                            self.order_id = None; self.current_price = None
                            time.sleep(0.05)
                        except BinanceAPIException as e:
                            logger.error(f"[Tracker] cancel error: {e}")
                            time.sleep(self.poll_interval); continue

                # 检查是否成交
                if self.order_id is not None and not self.dry_run:
                    try:
                        o = self.client.futures_get_order(symbol=self.symbol, orderId=self.order_id)
                        status = o.get("status", "")
                        executed_qty = float(o.get("executedQty", 0))
                        if status == "FILLED" or executed_qty >= self.qty - 1e-8:
                            logger.info(f"[Tracker] order filled orderId={self.order_id}")
                            self._filled = True
                            # 成交后清空，避免 finally 再撤单
                            self.order_id = None
                            self.current_price = None
                            break
                    except BinanceAPIException as e:
                        logger.error(f"[Tracker] get order status error: {e}")

                time.sleep(self.poll_interval)
        finally:
            # 仅当未成交且仍有挂单时才撤单
            if (not self._filled) and (self.order_id is not None) and (not self.dry_run):
                try:
                    self.client.futures_cancel_order(symbol=self.symbol, orderId=self.order_id)
                    logger.info(f"[Tracker] cleaned up cancel orderId={self.order_id}")
                except Exception as e:
                    logger.warning(f"[Tracker] cleanup cancel failed: {e}")
            else:
                logger.info(f"[Tracker] finished for {self.symbol} {self.side} (filled={self._filled})")

            # 完成回调
            try:
                if callable(self.on_finish):
                    self.on_finish(self.symbol, self.side, self._filled)
            except Exception:
                pass

    def stop(self):
        self._stop.set()


class TradingManager:
    def __init__(self, engine, client: Client, allocations: Dict[str, float],
                 mode: str = "both", dry_run: bool = True, testnet: bool = True,
                 base_weights: Dict[str, float] = None, debug: bool = False):
        """
        engine: MarketDataEngine 实例（用于 query MA 以及事件队列）
        client: python-binance Client（已配置 API）
        allocations: {"BTCUSDT": 0.3, "ETHUSDT": 0.2}
        mode: "both" | "long" | "short"
        base_weights: 周期基础权重字典，若 None 使用 DEFAULT_BASE_WEIGHTS
        debug: 打开更详细的运行时日志
        """
        self.engine = engine
        self.client = client
        self.allocations = {k.upper(): v for k, v in allocations.items()}
        self.mode = mode
        self.dry_run = dry_run
        self.testnet = testnet
        self.debug = debug

        self.base_weights = base_weights or DEFAULT_BASE_WEIGHTS.copy()

        # 默认每个 symbol 使用 "market"
        self.per_symbol_order_type: Dict[str, str] = {s: "market" for s in self.allocations.keys()}

        self.trackers: Dict[str, OrderTracker] = {}
        self.consumer_thread = threading.Thread(target=self._queue_consumer_loop, daemon=True)
        self._stop = threading.Event()

        # 缓存合约 step info
        self.symbol_info = {}
        try:
            info_all = client.futures_exchange_info()
            for it in info_all.get("symbols", []):
                self.symbol_info[it["symbol"]] = it
        except Exception as e:
            logger.warning(f"[TradingManager] initial exchangeInfo fetch failed: {e}")

    # ===== 公共生命周期 =====
    def start(self):
        logger.info("[TradingManager] start consumer thread")
        if not self.consumer_thread.is_alive():
            self.consumer_thread = threading.Thread(target=self._queue_consumer_loop, daemon=True)
            self.consumer_thread.start()

    def stop(self):
        logger.info("[TradingManager] stopping")
        self._stop.set()
        for t in list(self.trackers.values()):
            try: t.stop()
            except: pass

    # ===== 账户/持仓/价格 =====
    def get_equity(self) -> float:
        try:
            acc = self.client.futures_account()
            twb = float(acc.get("totalWalletBalance", 0))
            return twb
        except Exception as e:
            logger.error(f"[TradingManager] get_equity error: {e}")
            try:
                bal = self.client.futures_account_balance()
                for x in bal:
                    if x.get("asset") == "USDT":
                        return float(x.get("balance", 0))
            except: pass
        return 0.0

    def snapshot_account(self) -> None:
        """详细输出账户&持仓快照"""
        try:
            acc = self.client.futures_account()
            assets = acc.get("assets", [])
            positions = acc.get("positions", [])
            usdt = next((a for a in assets if a.get("asset") == "USDT"), None)
            if usdt:
                logger.info(f"[Account] USDT: wallet={usdt.get('walletBalance')} available={usdt.get('availableBalance')}")
            else:
                logger.info("[Account] USDT not found in assets")

            rows = []
            for p in positions:
                amt = float(p.get("positionAmt", 0))
                if abs(amt) < 1e-12:
                    continue
                rows.append({
                    "symbol": p.get("symbol"),
                    "amt": p.get("positionAmt"),
                    "entry": p.get("entryPrice"),
                    "upl": p.get("unrealizedProfit"),
                    "leverage": p.get("leverage", "N/A"),
                })
            if rows:
                logger.info("[Positions] non-zero positions below:")
                for r in rows:
                    logger.info(f"  {r['symbol']}: amt={r['amt']} entry={r['entry']} UPL={r['upl']} lev={r['leverage']}")
            else:
                logger.info("[Positions] no open positions")
        except Exception as e:
            logger.error(f"[Account] snapshot failed: {e}")

    def get_mark_price(self, symbol: str) -> float:
        try:
            mp = self.client.futures_mark_price(symbol=symbol)
            return float(mp.get("markPrice") or mp.get("price"))
        except Exception as e:
            logger.error(f"[TradingManager] get_mark_price {symbol} error: {e}")
        try:
            t = self.client.futures_symbol_ticker(symbol=symbol)
            return float(t.get("price"))
        except:
            return 0.0

    def get_position_amount(self, symbol: str) -> float:
        try:
            pos_list = self.client.futures_position_information(symbol=symbol)
            if isinstance(pos_list, list) and len(pos_list) > 0:
                p = pos_list[0]
                return float(p.get("positionAmt", 0))
            elif isinstance(pos_list, dict):
                return float(pos_list.get("positionAmt", 0))
        except Exception as e:
            logger.error(f"[TradingManager] get_position_amount {symbol} error: {e}")
        return 0.0

    def _get_lot_step_and_min_notional(self, symbol: str):
        info = self.symbol_info.get(symbol)
        if not info:
            try:
                info_all = self.client.futures_exchange_info()
                for it in info_all.get("symbols", []):
                    if it["symbol"] == symbol:
                        info = it; self.symbol_info[symbol] = it; break
            except Exception as e:
                logger.error(f"[TradingManager] fetch exchangeInfo failed: {e}")
                return 0.000001, 0.0, 0.0
        step = 0.000001; min_notional = 0.0; min_qty = 0.0
        for f in info.get("filters", []):
            if f.get("filterType") == "LOT_SIZE":
                step = float(f.get("stepSize", step))
                min_qty = float(f.get("minQty", min_qty))
            if f.get("filterType") in ("MIN_NOTIONAL", "NOTIONAL"):
                min_notional = float(f.get("minNotional", min_notional) or f.get("notional", min_notional))
        if self.debug:
            logger.info(f"[SymbolRules] {symbol}: step={step} minQty={min_qty} minNotional={min_notional}")
        return step, min_qty, min_notional

    # ===== 信号解释与目标计算 =====
    def _calc_target_ratio_details(self, symbol: str) -> Tuple[float, List[Dict[str, Any]]]:
        """返回 target_ratio 及逐周期详细解释"""
        details: List[Dict[str, Any]] = []
        total_weighted = 0.0
        for iv in self.engine.intervals:
            ma7, ma14, ma28 = self.engine.compute_ma_values(symbol, iv)
            if None in (ma7, ma14, ma28):
                cond = 0; contrib = 0.0
                row = dict(interval=iv, ma7=ma7, ma14=ma14, ma28=ma28,
                           d7_14=None, d7_28=None, d14_28=None, cond=cond,
                           weight=self.base_weights.get(iv, 0.0), contrib=contrib)
            else:
                cond = 0
                if ma7 > ma14: cond += 1
                if ma7 > ma28: cond += 1
                if ma14 > ma28: cond += 1
                weight = self.base_weights.get(iv, 0.0)
                contrib = weight * cond
                row = dict(interval=iv, ma7=ma7, ma14=ma14, ma28=ma28,
                           d7_14=ma7 - ma14, d7_28=ma7 - ma28, d14_28=ma14 - ma28,
                           cond=cond, weight=weight, contrib=contrib)
            details.append(row)
            total_weighted += row["contrib"]
        target_ratio = total_weighted / 3.0  # 0..1
        return target_ratio, details


    def _log_signal_details(self, symbol: str, target_ratio: float, details: List[Dict[str, Any]], reason: str):
        logger.info(f"[Signal] {symbol} reason={reason} → target_ratio={target_ratio:.4f}")
        if self.debug:
            logger.info(" interval |   ma7     ma14     ma28 |  d7-14    d7-28    d14-28 | cond | weight | contrib")
            for r in details:
                fmt = lambda x: "None" if x is None else f"{x:.6f}"
                logger.info(f" {r['interval']:>6} | {fmt(r['ma7']):>8} {fmt(r['ma14']):>8} {fmt(r['ma28']):>8} |"
                            f" {fmt(r['d7_14']):>8} {fmt(r['d7_28']):>8} {fmt(r['d14_28']):>8} |"
                            f"  {r['cond']:^3} | {r['weight']:^6.2f} | {r['contrib']:^7.2f}")

    def _target_net_fraction(self, target_ratio: float) -> float:
        """将 target_ratio(0..1) 转换为净仓位比 [-1,1] 根据模式"""
        if self.mode == "both":
            return (target_ratio - 0.5) * 2.0
        elif self.mode == "long":
            return max(0.0, target_ratio)
        elif self.mode == "short":
            return -max(0.0, target_ratio)
        else:
            return (target_ratio - 0.5) * 2.0

    # ===== 下单主流程 =====
    def _reconcile_symbol(self, symbol: str, order_type: Optional[str] = None, reason: str = "kline_close"):
        symbol = symbol.upper()
        if symbol not in self.allocations:
            logger.debug(f"[reconcile] no allocation for {symbol}, skip")
            return
        if order_type is None:
            order_type = self.per_symbol_order_type.get(symbol, "market")

        # 账户快照（可开关）
        if self.debug:
            self.snapshot_account()

        # 1) 计算目标比例与解释
        target_ratio, details = self._calc_target_ratio_details(symbol)
        self._log_signal_details(symbol, target_ratio, details, reason)

        # 2) 模式映射为净仓位比
        net_frac = self._target_net_fraction(target_ratio)  # [-1,1]

        # 3) 资金与目标价值
        equity = self.get_equity()
        alloc = self.allocations.get(symbol, 0.0)
        target_value = equity * alloc * net_frac  # 可为负

        # 4) 现有仓位与价格
        mark_price = self.get_mark_price(symbol)
        if mark_price <= 0:
            logger.error(f"[reconcile] mark price invalid for {symbol}")
            return
        current_amt = self.get_position_amount(symbol)
        current_value = current_amt * mark_price

        # 5) 差值与数量
        delta_value = target_value - current_value
        qty = abs(delta_value) / mark_price

        # 6) 合约约束
        step, min_qty, min_notional = self._get_lot_step_and_min_notional(symbol)
        qty_adj = _floor_to_step(qty, step)
        notional = qty_adj * mark_price
        if self.debug:
            logger.info(f"[Compute] {symbol} equity={equity:.2f} alloc={alloc:.4f} "
                        f"net_frac={net_frac:.4f} target_val={target_value:.2f} cur_val={current_value:.2f} "
                        f"delta={delta_value:.2f} raw_qty={qty:.8f} adj_qty={qty_adj:.8f} notional={notional:.4f}")

        if qty_adj < min_qty or notional < min_notional:
            logger.info(f"[reconcile] qty too small: adj_qty={qty_adj} (minQty={min_qty}) "
                        f"notional={notional:.4f} (minNotional={min_notional}) → skip")
            return

        # 7) 决策方向
        side = "BUY" if delta_value > 0 else "SELL"
        logger.info(f"[Decision] {symbol} side={side} qty={qty_adj} order_type={order_type} @ price≈{mark_price}")

        # 8) 执行
        if self.dry_run:
            logger.info(f"[DRY_RUN] Would {order_type} {side} {qty_adj} {symbol}")
            return

        if order_type == "market":
            try:
                order = self.client.futures_create_order(
                    symbol=symbol,
                    side=side,
                    type="MARKET",
                    quantity=qty_adj
                )
                logger.info(f"[market order] placed: orderId={order.get('orderId')} status={order.get('status')}")
            except BinanceAPIException as e:
                logger.error(f"[market order] error: {e}")
        elif order_type == "tracking":
            key = f"{symbol}_{side}"
            if key in self.trackers:
                try: self.trackers[key].stop()
                except: pass
            tracker = OrderTracker(
                self.client, symbol, side, qty_adj,
                reduce_only=False, dry_run=self.dry_run,
                on_finish=self._on_tracker_finish
            )
            tracker.start()
            self.trackers[key] = tracker
            logger.info(f"[tracking] started tracker for {symbol} {side} qty={qty_adj}")
        else:
            logger.warning(f"[reconcile] unknown order_type {order_type} for {symbol}")

    # ===== 手动事件：直接使用指定净仓位比 =====
    def _manual_reconcile(self, symbol: str, net_frac: float, order_type: Optional[str] = None):
        symbol = symbol.upper()
        if symbol not in self.allocations:
            logger.info(f"[manual] {symbol} 未在 allocations 中，临时按 1.0 使用")
            self.allocations[symbol] = 1.0
        if order_type is None:
            order_type = self.per_symbol_order_type.get(symbol, "market")

        if self.debug:
            self.snapshot_account()

        equity = self.get_equity()
        alloc = self.allocations.get(symbol, 1.0)
        mark_price = self.get_mark_price(symbol)
        if mark_price <= 0:
            logger.error(f"[manual] {symbol} 无法获取价格"); return
        cur_amt = self.get_position_amount(symbol)
        cur_val = cur_amt * mark_price
        tgt_val = equity * alloc * net_frac
        delta_value = tgt_val - cur_val
        qty = abs(delta_value) / mark_price

        step, min_qty, min_notional = self._get_lot_step_and_min_notional(symbol)
        qty_adj = _floor_to_step(qty, step)
        notional = qty_adj * mark_price

        logger.info(f"[manual] {symbol} net_frac={net_frac:.4f} equity={equity:.2f} alloc={alloc:.2f} "
                    f"cur_val={cur_val:.2f} tgt_val={tgt_val:.2f} delta={delta_value:.2f} "
                    f"qty={qty:.8f} adj_qty={qty_adj:.8f} notional={notional:.4f}")

        if qty_adj < min_qty or notional < min_notional:
            logger.info(f"[manual] qty too small: adj_qty={qty_adj} (minQty={min_qty}) "
                        f"notional={notional:.4f} (minNotional={min_notional}) → skip")
            return

        side = "BUY" if delta_value > 0 else "SELL"
        if self.dry_run:
            logger.info(f"[DRY_RUN] Would {order_type} {side} {qty_adj} {symbol}")
            return
        if order_type == "market":
            try:
                order = self.client.futures_create_order(symbol=symbol, side=side, type="MARKET", quantity=qty_adj)
                logger.info(f"[manual] market placed: orderId={order.get('orderId')} status={order.get('status')}")
            except Exception as e:
                logger.error(f"[manual] 下单失败: {e}")
        else:
            key = f"{symbol}_{side}"
            if key in self.trackers:
                try: self.trackers[key].stop()
                except: pass
            tracker = OrderTracker(
                self.client, symbol, side, qty_adj,
                reduce_only=False, dry_run=self.dry_run,
                on_finish=self._on_tracker_finish
            )
            tracker.start()
            self.trackers[key] = tracker
            logger.info(f"[manual] tracking started for {symbol} {side} qty={qty_adj}")

    # ===== 跟踪单完成回调 =====
    def _on_tracker_finish(self, symbol: str, side: str, filled: bool):
        key = f"{symbol}_{side}"
        self.trackers.pop(key, None)
        logger.info(f"[tracking] finished: {symbol} {side}, filled={filled}")
        if self.debug:
            try:
                self.snapshot_account()
            except Exception:
                pass

    # ===== 事件消费 =====
    def _queue_consumer_loop(self):
        q = self.engine.event_queue
        idle_ticks = 0
        while not self._stop.is_set():
            try:
                event = q.get(timeout=1.0)
                idle_ticks = 0
            except Exception:
                idle_ticks += 1
                if self.debug and idle_ticks % 30 == 0:  # 每30秒打一条心跳日志
                    logger.info("[consumer] idle heartbeat (no events)")
                continue
            try:
                symbol = event.get("symbol")
                if not symbol:
                    logger.warning("[consumer] event missing symbol, drop")
                    q.task_done(); continue

                if event.get("manual"):
                    ratio = float(event.get("target_ratio", 0.0))
                    logger.info(f"[consumer] MANUAL {symbol} target_ratio={ratio}")
                    self._manual_reconcile(symbol, net_frac=ratio, order_type=self.per_symbol_order_type.get(symbol, "market"))
                else:
                    iv = event.get("interval", "N/A")
                    logger.info(f"[consumer] KLINE_CLOSE {symbol} interval={iv}")
                    ot = self.per_symbol_order_type.get(symbol, "market")
                    self._reconcile_symbol(symbol, order_type=ot, reason=f"kline_close:{iv}")

                q.task_done()
            except Exception as e:
                logger.error(f"[TradingManager] consumer loop error: {e}")
