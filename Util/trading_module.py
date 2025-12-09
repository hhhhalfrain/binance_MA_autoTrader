
import os
import json
import time
import logging
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional, Tuple
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
from binance.client import Client
from binance.exceptions import BinanceAPIException
from Util.config_manager import ConfigManager

def _floor_to_step(value: float, step: float) -> float:
    """按交易所给定的步进 step 向下截断（用于数量/价格截断）"""
    if step <= 0:
        return value
    d = Decimal(str(value)) / Decimal(str(step))
    d = d.quantize(Decimal("1."), rounding=ROUND_DOWN)
    return float(d * Decimal(str(step)))

def _quantize_to_step(value: float, step_str: str, mode: str = "floor") -> float:
    """
    将 value 对齐到 step 的整数倍。
    mode:
      - "floor": 向下截断（买单价/数量常用）
      - "ceil" : 向上进位（卖单价常用）
      - "round": 四舍五入
    返回 float，但构造字符串时建议用 _decimal_str() 保留精准表示。
    """
    if not step_str or float(step_str) == 0.0:
        return float(value)
    dval = Decimal(str(value))
    step = Decimal(step_str)
    q = dval / step
    if mode == "ceil":
        q = q.to_integral_value(rounding=ROUND_UP)
    elif mode == "round":
        q = q.to_integral_value()  # bankers rounding; 此处很少用
    else:
        q = q.to_integral_value(rounding=ROUND_DOWN)
    return float(q * step)


def _decimal_str(value: float) -> str:
    """
    生成不会引入多余小数且精确的字符串（去掉多余尾零），
    避免 'precision is over the maximum'。
    """
    d = Decimal(str(value)).normalize()
    # normalize 可能产生科学计数法，转成普通字符串
    s = format(d, 'f')
    # 去掉尾部无意义的 0 和小数点
    if '.' in s:
        s = s.rstrip('0').rstrip('.')
    return s if s else "0"


class TradingModule:
    """
    使用示例：
        tm = TradingModule(config_path="config.json")
        # 市价下单（按张数）
        tm.market_trade(symbol="BTCUSDT", qty=0.5, side="BUY")

        # 限价只挂单追踪（例如 20 秒内不断跟随最优价，直到成交）
        tm.limit_maker_track(symbol="BTCUSDT", qty=0.5, side="SELL",
                             duration_sec=20, poll_interval=0.5)

        # 查询持仓
        pos = tm.get_position("BTCUSDT")  # -> {"qty": 0.10, "direction": "LONG"}
        # 查询余额
        bal = tm.get_asset_balance("USDC")
        # 查询指数价
        idx = tm.get_index_price("BTCUSDT")
    """

    def __init__(self, config_path: str = "config.json"):
        # 读取配置
        self.config_mgr = ConfigManager(config_path)
        self.config = self.config_mgr.all()

        self._init_logger(self.config.get("debug", False))

        # 初始化客户端（.env 优先）
        self.client = self._init_client_from_env_or_config(self.config)

        # 缓存交易规则（步进/最小下单量/价格精度等）
        self._exchange_info_cache: Optional[Dict[str, Any]] = None
        self._refresh_exchange_info()

    # -----------------------------
    # 基础：配置 / 日志 / 客户端
    # -----------------------------


    def _init_logger(self, debug: bool):
        self.logger = logging.getLogger("TradingModule")
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

        fh = logging.FileHandler("trading_module.log", encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        self.logger.addHandler(fh)

        if debug:
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
            self.logger.addHandler(ch)

        self.logger.debug("日志初始化完成（debug=%s）", debug)

    def _init_client_from_env_or_config(self, cfg: Dict[str, Any]) -> Client:

        testnet = bool(cfg.get("testnet", False))
        api_key, api_secret = self.config_mgr.get_api_keys()
        client = Client(api_key, api_secret)

        # 切测试网（期货）
        if testnet:
            futures_base = "https://testnet.binancefuture.com"
            client.FUTURES_URL = futures_base + "/fapi"
            if hasattr(client, "futures_api_url"):
                client.futures_api_url = client.FUTURES_URL
            self.logger.info("Futures 已切换测试网：%s", client.FUTURES_URL)
        else:
            self.logger.info("Futures 使用正式网。")

        return client

    # -----------------------------
    # 交易规则工具
    # -----------------------------
    def _refresh_exchange_info(self, force: bool = False):
        if self._exchange_info_cache is not None and not force:
            return
        try:
            info = self.client.futures_exchange_info()
            self._exchange_info_cache = info
            self.logger.debug("已缓存 futures_exchange_info。")
        except Exception as e:
            self._exchange_info_cache = None
            self.logger.error("获取 futures_exchange_info 失败：%s", e)

    def _get_symbol_filters(self, symbol: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        返回 (LOT_SIZE, PRICE_FILTER)；任一不存在则返回 None
        """
        self._refresh_exchange_info()
        if not self._exchange_info_cache:
            return None, None

        for s in self._exchange_info_cache.get("symbols", []):
            if s.get("symbol") == symbol:
                lot, price = None, None
                for f in s.get("filters", []):
                    if f.get("filterType") == "LOT_SIZE":
                        lot = f
                    elif f.get("filterType") == "PRICE_FILTER":
                        price = f
                return lot, price
        return None, None

    # -----------------------------
    # 1) 市价交易（按张数，向下截断）
    # -----------------------------
    def market_trade(self, symbol: str, qty: float, side: str,
                     reduce_only: bool = False, position_side: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        市价交易（合约），qty 为“张数/合约数量”，会按 LOT_SIZE.stepSize 向下截断。
        - side: "BUY" 或 "SELL"
        - reduce_only: True 仅减仓
        - position_side: 仅在双向持仓模式下使用，如 "LONG" / "SHORT"

        返回：下单回报（dict），失败返回 None
        """
        side = side.upper()
        assert side in ("BUY", "SELL"), "side 只能是 BUY 或 SELL"

        lot, _ = self._get_symbol_filters(symbol)
        step = float(lot.get("stepSize", "0.0")) if lot else 0.0
        min_qty = float(lot.get("minQty", "0.0")) if lot else 0.0

        adj_qty = _floor_to_step(qty, step) if step > 0 else qty
        if min_qty > 0 and adj_qty < min_qty:
            self.logger.error("数量 %.10f 截断后 %.10f 小于最小下单量 %.10f，取消下单。", qty, adj_qty, min_qty)
            return None

        params = dict(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=adj_qty,
        )
        if reduce_only:
            params["reduceOnly"] = True
        if position_side:
            params["positionSide"] = position_side

        try:
            order = self.client.futures_create_order(**params)
            self.logger.info("[MARKET] %s %s qty=%s 下单成功。", symbol, side, adj_qty)
            return order
        except BinanceAPIException as e:
            self.logger.error("[MARKET] %s %s 下单失败：%s", symbol, side, e)
        except Exception as e:
            self.logger.exception("[MARKET] %s %s 异常：%s", symbol, side, e)
        return None

    # -----------------------------
    # 2) 限价只挂单追踪（Post Only GTX）
    # -----------------------------
    def limit_maker_track(self, symbol: str, qty: float, side: str,
                          duration_sec: float = 30.0, poll_interval: float = 0.5,
                          reduce_only: bool = False, position_side: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        只挂单（GTX）追踪：
          - 价格挂在盘口：BUY=买一价，SELL=卖一价
          - 严格按 tickSize/stepSize 对齐
          - 部分成交后仅追踪“剩余数量”，用增量法累计，不重复扣减
        """
        side = side.upper()
        assert side in ("BUY", "SELL")

        # === 读取精度约束 ===
        lot, price_filter = self._get_symbol_filters(symbol)
        step_str = lot.get("stepSize", "0.0") if lot else "0.0"
        tick_str = price_filter.get("tickSize", "0.0") if price_filter else "0.0"
        min_qty = float(lot.get("minQty", "0.0")) if lot else 0.0

        # === 初始化数量 ===
        initial_qty = _quantize_to_step(qty, step_str, mode="floor")
        if min_qty > 0 and initial_qty < min_qty:
            self.logger.error("数量 %.10f 截断后 %.10f 小于最小下单量 %.10f，取消追踪挂单。", qty, initial_qty, min_qty)
            return None

        remaining_qty = initial_qty  # 本轮尚未成交的数量
        cumulative_filled = 0.0  # 跨订单累计已成交
        order_reported_executed = 0.0  # 当前活动订单已经计入累计的成交量

        start = time.time()
        active_order_id: Optional[int] = None
        active_price: Optional[float] = None
        last_resp: Optional[Dict[str, Any]] = None

        while time.time() - start < duration_sec and remaining_qty >= (min_qty if min_qty > 0 else 0.0):
            try:
                # === 计算目标价格：直接挂在盘口价，并按 tick 对齐 ===
                ob = self.client.futures_orderbook_ticker(symbol=symbol)
                best_bid, best_ask = float(ob["bidPrice"]), float(ob["askPrice"])
                raw_price = best_bid if side == "BUY" else best_ask
                target_price = _quantize_to_step(raw_price, tick_str, mode="floor")
                price_str = _decimal_str(target_price)

                # 是否需要换价（首次挂单或盘口移动）
                need_replace = (active_order_id is None) or (active_price != target_price)

                # === 若需要更换挂单：先处理旧单的“增量成交”，再撤单重下 ===
                if need_replace:
                    if active_order_id is not None:
                        try:
                            od = self.client.futures_get_order(symbol=symbol, orderId=active_order_id)
                            st = od.get("status")
                            exec_now = float(od.get("executedQty", "0"))
                            # 先把未计入累计的“增量成交”补进去
                            delta = max(0.0, exec_now - order_reported_executed)
                            if delta > 0:
                                cumulative_filled += delta
                                remaining_qty = _quantize_to_step(max(0.0, initial_qty - cumulative_filled), step_str,
                                                                  mode="floor")
                                order_reported_executed = exec_now
                                self.logger.info("[TRACK] 已成交 %.8f，剩余 %.8f", delta, remaining_qty)

                            if st in ("NEW", "PARTIALLY_FILLED"):
                                # 撤单
                                try:
                                    self.client.futures_cancel_order(symbol=symbol, orderId=active_order_id)
                                    self.logger.debug("[TRACK] 撤单 orderId=%s", active_order_id)
                                except BinanceAPIException as ce:
                                    self.logger.debug("[TRACK] 撤单提示：%s", ce)

                                # 撤单后再查一次，补最后一跳增量（避免撤单瞬间撮合）
                                try:
                                    od2 = self.client.futures_get_order(symbol=symbol, orderId=active_order_id)
                                    exec_final = float(od2.get("executedQty", "0"))
                                except Exception:
                                    exec_final = exec_now
                                delta2 = max(0.0, exec_final - order_reported_executed)
                                if delta2 > 0:
                                    cumulative_filled += delta2
                                    remaining_qty = _quantize_to_step(max(0.0, initial_qty - cumulative_filled),
                                                                      step_str, mode="floor")
                                    order_reported_executed = exec_final
                                    self.logger.info("[TRACK] 部分成交 %.8f，剩余 %.8f", delta2, remaining_qty)
                        except BinanceAPIException as qe:
                            self.logger.debug("[TRACK] 查询旧单失败：%s", qe)
                        finally:
                            # 清空当前活动订单状态
                            active_order_id = None
                            active_price = None
                            order_reported_executed = 0.0

                    # 剩余数量是否还能下单
                    if min_qty > 0 and remaining_qty < min_qty:
                        self.logger.info("[TRACK] 剩余数量 %.8f 小于最小下单量 %.8f，结束追踪。", remaining_qty, min_qty)
                        break

                    # 下新单（只挂单 GTX）
                    qty_str = _decimal_str(remaining_qty)
                    params = dict(
                        symbol=symbol,
                        side=side,
                        type="LIMIT",
                        timeInForce="GTX",
                        quantity=qty_str,
                        price=price_str,
                    )
                    if reduce_only:
                        params["reduceOnly"] = True
                    if position_side:
                        params["positionSide"] = position_side

                    try:
                        resp = self.client.futures_create_order(**params)
                        last_resp = resp
                        active_order_id = resp.get("orderId")
                        active_price = target_price
                        order_reported_executed = 0.0  # 新订单从0开始累计
                        self.logger.info("[TRACK] %s %s qty=%s @%s 已挂单（GTX）。",
                                         symbol, side, qty_str, price_str)
                    except BinanceAPIException as oe:
                        self.logger.debug("[TRACK] GTX 拒单/下单失败：%s", oe)
                        active_order_id = None
                        active_price = None
                        order_reported_executed = 0.0
                        time.sleep(poll_interval)
                        continue

                # === 轮询当前活动订单的成交增量 ===
                if active_order_id is not None:
                    try:
                        od = self.client.futures_get_order(symbol=symbol, orderId=active_order_id)
                        st = od.get("status")
                        exec_now = float(od.get("executedQty", "0"))

                        delta = max(0.0, exec_now - order_reported_executed)
                        if delta > 0:
                            cumulative_filled += delta
                            remaining_qty = _quantize_to_step(max(0.0, initial_qty - cumulative_filled), step_str,
                                                              mode="floor")
                            order_reported_executed = exec_now
                            self.logger.info("[TRACK] 已成交 %.8f，剩余 %.8f", delta, remaining_qty)

                        if st == "FILLED":
                            self.logger.info("[TRACK] 订单完全成交：orderId=%s", active_order_id)
                            return od
                        elif st in ("CANCELED", "REJECTED", "EXPIRED"):
                            # 订单已失效，清空，下一轮会重下剩余
                            active_order_id = None
                            active_price = None
                            order_reported_executed = 0.0
                    except BinanceAPIException as se:
                        # 查不到等同于已失效，清空
                        self.logger.debug("[TRACK] 查询订单异常：%s", se)
                        active_order_id = None
                        active_price = None
                        order_reported_executed = 0.0

                time.sleep(poll_interval)

            except Exception as e:
                self.logger.exception("[TRACK] 追踪异常：%s", e)
                time.sleep(poll_interval)

        # === 超时收尾：撤未完成单，并将最后增量计入 ===
        if active_order_id is not None:
            try:
                od = self.client.futures_get_order(symbol=symbol, orderId=active_order_id)
                st = od.get("status")
                exec_now = float(od.get("executedQty", "0"))
                delta = max(0.0, exec_now - order_reported_executed)
                if delta > 0:
                    cumulative_filled += delta
                    remaining_qty = _quantize_to_step(max(0.0, initial_qty - cumulative_filled), step_str, mode="floor")
                    self.logger.info("[TRACK] 已成交 %.8f，剩余 %.8f", delta, remaining_qty)

                if st in ("NEW", "PARTIALLY_FILLED"):
                    try:
                        self.client.futures_cancel_order(symbol=symbol, orderId=active_order_id)
                        self.logger.info("[TRACK] 超时，撤销挂单：orderId=%s", active_order_id)
                    except Exception as e:
                        self.logger.debug("[TRACK] 超时撤单提示：%s", e)
            except Exception as e:
                self.logger.debug("[TRACK] 超时查单提示：%s", e)

        return last_resp

    # -----------------------------
    # 3) 获取当前仓位大小（张数）与方向
    # -----------------------------
    # -----------------------------
    # 3) 获取当前仓位大小（张数）、方向和开仓均价
    # -----------------------------
    def get_position(self, symbol: str) -> Dict[str, Any]:
        """
        返回：
          - qty: float
              > 0 表示多头
              < 0 表示空头
              = 0 表示无仓
          - direction: "LONG" / "SHORT" / "FLAT"
          - entry_price: float 或 None
              有仓位时为当前持仓的开仓均价（entryPrice）
              无仓位时为 None

        实现说明：
          - 基于 USDⓈ-M futures 的 futures_account().positions
          - 仅按 symbol 匹配；若 positionAmt 的绝对值非常接近 0，则视为无仓位
        """
        symbol = symbol.upper()
        try:
            acct = self.client.futures_account()
            positions = acct.get("positions", [])

            # 单向持仓模式下通常每个 symbol 只有一条；
            # 若是对冲模式，则可能有 LONG/SHORT 两条，这里简单处理为：
            #   优先返回第一条 positionAmt != 0 的记录；
            #   如果都为 0，则视为无仓位。
            candidate_zero_record = None

            for p in positions:
                if p.get("symbol") != symbol:
                    continue

                # 记录一份“确实有这个 symbol，但仓位为 0”的情况
                if candidate_zero_record is None:
                    candidate_zero_record = p

                pos_amt = float(p.get("positionAmt", "0"))

                # 有实际仓位（多或空）
                if abs(pos_amt) >= 1e-12:
                    direction = "LONG" if pos_amt > 0 else "SHORT"

                    entry_str = p.get("entryPrice")
                    entry_price: Optional[float] = None
                    if entry_str is None:
                        self.logger.warning(
                            "symbol=%s 的 position 记录中缺少 entryPrice 字段：%s",
                            symbol, p
                        )
                    else:
                        try:
                            entry_price = float(entry_str)
                        except ValueError:
                            self.logger.warning(
                                "symbol=%s 的 entryPrice 解析失败：%s",
                                symbol, entry_str
                            )
                            entry_price = None

                    self.logger.debug(
                        "symbol=%s 当前仓位: positionAmt=%s, direction=%s, entryPrice=%s",
                        symbol, pos_amt, direction, entry_price
                    )
                    return {
                        "qty": pos_amt,
                        "direction": direction,
                        "entry_price": entry_price,
                    }

            # 能走到这里有两种情况：
            # 1) 有这个 symbol 的记录，但 positionAmt == 0 -> 无仓位
            # 2) 根本没有这个 symbol 的记录           -> 无仓位
            if candidate_zero_record is not None:
                self.logger.debug(
                    "symbol=%s 存在 position 记录，但 positionAmt=0，视为无仓位。",
                    symbol
                )
            else:
                self.logger.info(
                    "futures_account.positions 中未找到 symbol=%s 的记录，视为无仓位。",
                    symbol
                )

            return {
                "qty": 0.0,
                "direction": "FLAT",
                "entry_price": None,
            }

        except BinanceAPIException as e:
            self.logger.error("获取 %s 仓位失败：%s", symbol, e)
        except Exception as e:
            self.logger.exception("解析 %s 仓位异常：%s", symbol, e)

        # 兜底：出错时也统一返回“无仓位”的结构，避免上层崩
        return {
            "qty": 0.0,
            "direction": "FLAT",
            "entry_price": None,
        }

    # -----------------------------
    # 4) 获取合约账户中某币种余额
    # -----------------------------
    def get_asset_balance(self, asset: str) -> Optional[float]:
        """
        返回 USDⓈ-M 合约账户中指定资产的【钱包余额 walletBalance】。
        注意：walletBalance ≠ availableBalance（后者为可用余额）。
        查询失败返回 None；未找到该资产时返回 0.0。
        """
        try:
            acct = self.client.futures_account()
            for a in acct.get("assets", []):
                if a.get("asset") == asset.upper():
                    return float(a.get("walletBalance", "0"))
            return 0.0
        except BinanceAPIException as e:
            self.logger.error("获取账户余额失败：%s", e)
        except Exception as e:
            self.logger.exception("解析账户余额异常：%s", e)
        return None

    # -----------------------------
    # 5) 获取某币种当前最新指数价格（Index Price）
    # -----------------------------
    def get_mark_price(self, symbol: str) -> Optional[float]:
        """
        获取 USDⓈ-M 合约【标记价格 Mark Price】。
        优先调用 python-binance 的 futures_mark_price；
        若该方法不存在，则退化为直接请求 /fapi/v1/premiumIndex。
        成功返回 float；失败返回 None。
        """
        try:
            # 1) 常规接口（新版本 python-binance）
            data = self.client.futures_mark_price(symbol=symbol)


            # 单符号通常返回 dict，多符号返回 list
            if isinstance(data, dict):
                mp = data.get("markPrice")
                return float(mp) if mp is not None else None
            if isinstance(data, list):
                for d in data:
                    if d.get("symbol") == symbol and d.get("markPrice") is not None:
                        return float(d["markPrice"])
            return None

        except BinanceAPIException as e:
            self.logger.error("获取标记价格失败：%s", e)
        except Exception as e:
            self.logger.exception("解析标记价格异常：%s", e)
        return None

    def adjust_position(self,
                        symbol: str,
                        target_qty: float,
                        mode: str = "market",
                        duration_sec: float = 30.0,
                        poll_interval: float = 0.5,
                        position_side: Optional[str] = None) -> Dict[str, Any]:
        """
        将当前仓位调整到目标仓位（单向持仓）：
          - target_qty < 0 视为清仓（目标=0）
          - target_qty >= 0 视为目标多头张数
          - mode="market" 走市价；mode="track" 走只挂单追踪限价（GTX）
          - 最终返回前/后仓位、是否达到目标、下单回报等信息

        注意：
          - 若现有空仓且目标为正，本函数会自动先减空再增多（一次性按 delta 下单）。
          - 若 delta 截断至步进后 < minQty，将不会下单并给出日志提示。
          - 若账户为双向持仓，可传 position_side="LONG"/"SHORT"；默认按单向口径处理。
        """
        mode_l = (mode or "market").lower()
        if mode_l not in ("market", "track", "gtx", "maker", "limit"):
            raise ValueError("mode 必须为 'market' 或 'track'（别名: gtx/maker/limit）")

        # 读取交易规则精度
        lot, _pf = self._get_symbol_filters(symbol)
        step_str = lot.get("stepSize", "0.0") if lot else "0.0"
        min_qty  = float(lot.get("minQty", "0.0")) if lot else 0.0

        # 目标口径：负数表示清仓
        target =  float(target_qty)

        # 读取当前仓位（单向：>0 多头，<0 空头）
        cur = self.get_position(symbol)    # {'qty': float, 'direction': ...}
        cur_qty_signed = float(cur.get("qty", 0.0))

        # 需要调整的净变化量（+ 买入增多/减空；- 卖出减多/增空）
        delta = target - cur_qty_signed

        # 对齐到步进
        trade_qty = _quantize_to_step(abs(delta), step_str, mode="floor")

        # 小于最小下单量则不动作
        if trade_qty <= 0 or (min_qty > 0 and trade_qty < min_qty):
            self.logger.info(
                "[ADJUST] %s 目标=%.8f, 当前=%.8f, delta=%.8f 截断后=%.8f (< minQty=%.8f)，无需/无法下单。",
                symbol, target, cur_qty_signed, delta, trade_qty, min_qty
            )
            return {
                "symbol": symbol,
                "mode": mode_l,
                "before_qty": cur_qty_signed,
                "target_qty": target,
                "delta_to_order": 0.0,
                "after_qty": cur_qty_signed,
                "achieved": abs(_quantize_to_step(cur_qty_signed - target, step_str, "floor")) == 0.0,
                "order": None
            }

        side = "BUY" if delta > 0 else "SELL"

        # 下单
        if mode_l == "market":
            resp = self.market_trade(
                symbol=symbol,
                qty=trade_qty,
                side=side,
                reduce_only=False,             # 允许翻转仓位；若只想减仓可设 True
                position_side=position_side
            )
        else:
            # 走只挂单追踪限价（你已有的函数会自动“部分成交只追踪剩余数量”）
            resp = self.limit_maker_track(
                symbol=symbol,
                qty=trade_qty,
                side=side,
                duration_sec=duration_sec,
                poll_interval=poll_interval,
                reduce_only=False,
                position_side=position_side
            )

        # 下单后再读一次仓位，判断是否到达目标
        new_pos = self.get_position(symbol)
        new_qty_signed = float(new_pos.get("qty", 0.0))
        gap = _quantize_to_step(new_qty_signed - target, step_str, "floor")
        achieved = abs(gap) == 0.0

        self.logger.info(
            "[ADJUST] %s 模式=%s 目标=%.8f, 之前=%.8f, 实际=%.8f, 下单方向=%s, 下单数量=%.8f, 达标=%s",
            symbol, mode_l, target, cur_qty_signed, new_qty_signed, side, trade_qty, achieved
        )

        return {
            "symbol": symbol,
            "mode": mode_l,
            "before_qty": cur_qty_signed,
            "target_qty": target,
            "delta_to_order": trade_qty if side == "BUY" else -trade_qty,
            "after_qty": new_qty_signed,
            "achieved": achieved,
            "order": resp
        }

    def last_price_offset_maker_track(
            self,
            symbol: str,
            qty: float,
            side: str,
            offset: float,
            poll_interval: float = 0.5,
            reduce_only: bool = False,
            position_side: Optional[str] = None,
            min_query_delay: Optional[float] = None,
            max_2013_retries: int = 6,
    ) -> bool:
        """
        盘口成交价挂单交易（Post Only，阻塞直到完全成交才返回 True）
        - 锚点：最新成交价 last price ± offset；BUY 用 last-offset，SELL 用 last+offset
        - 价格对齐：BUY 向下取 tick，SELL 向上取 tick；再与 bestBid/bestAsk 夹紧，降低 GTX 拒单
        - 仅当活动订单“未成交量 >= minQty”时才撤单换价；否则保留等待，避免把剩余打到 <minQty 的死局
        - 仅当订单状态 FILLED 才返回 True；其它情况（含 <minQty 余量）均不返回 True
        - 下单后等待 min_query_delay 再查，规避期货端 -2013 竞态；查单优先用 origClientOrderId
        - 发生无法继续推进且无活动订单时（例如余量 < minQty 且无单可等），抛异常
        """
        import uuid
        side = (side or "").upper()
        if side not in ("BUY", "SELL"):
            raise ValueError("side 只能为 BUY 或 SELL")
        if qty <= 0:
            raise ValueError("qty 必须为正数")
        if offset < 0:
            raise ValueError("offset(偏差值) 不能为负")

        lot, price_filter = self._get_symbol_filters(symbol)
        if not lot or not price_filter:
            raise RuntimeError(f"未找到 {symbol} 的交易规则（LOT_SIZE/PRICE_FILTER）")

        step_str = lot.get("stepSize", "0.0")
        tick_str = price_filter.get("tickSize", "0.0")
        min_qty = float(lot.get("minQty", "0.0"))

        initial_qty = _quantize_to_step(qty, step_str, "floor")
        if min_qty > 0 and initial_qty < min_qty:
            raise ValueError(f"下单数量 {qty} 截断后 {initial_qty} 小于最小下单量 {min_qty}")

        remaining_qty = initial_qty
        cumulative_filled = 0.0
        order_reported_executed = 0.0

        active_order_id: Optional[int] = None
        active_client_oid: Optional[str] = None
        active_price: Optional[float] = None

        # 下单后至少等待这么久再查，避免 -2013；默认取 max(0.35, poll_interval)
        min_q_delay = max(0.35, poll_interval if (min_query_delay is None) else min_query_delay)
        last_place_ts: float = 0.0
        retry_2013 = 0

        def _get_last_price() -> Optional[float]:
            try:
                t = self.client.futures_symbol_ticker(symbol=symbol)
                if t and t.get("price") is not None:
                    return float(t["price"])
            except Exception:
                pass
            try:
                t24 = self.client.futures_ticker(symbol=symbol)
                if t24 and t24.get("lastPrice") is not None:
                    return float(t24["lastPrice"])
            except Exception:
                pass
            return None

        while True:
            # 如果刚刚下单，先等一等再查，避免 -2013
            if active_order_id is not None and (time.time() - last_place_ts) < min_q_delay:
                time.sleep(min_q_delay - (time.time() - last_place_ts))

            # 读取价格参考
            last = _get_last_price()
            if last is None:
                self.logger.debug("[LPO] 获取最新成交价失败，稍后重试")
                time.sleep(poll_interval)
                continue

            ob = self.client.futures_orderbook_ticker(symbol=symbol)
            best_bid = float(ob["bidPrice"]);
            best_ask = float(ob["askPrice"])
            try:
                mark = self.get_mark_price(symbol)
            except Exception:
                mark = None

            # 目标价
            if side == "BUY":
                raw = max(0.0, last - float(offset))
                q_price = _quantize_to_step(raw, tick_str, "floor")
                target_price = q_price if q_price <= best_bid else best_bid
            else:
                raw = last + float(offset)
                q_price = _quantize_to_step(raw, tick_str, "ceil")
                target_price = q_price if q_price >= best_ask else best_ask
            price_str = _decimal_str(target_price)

            # 首次挂单
            if active_order_id is None:
                if remaining_qty <= 0:
                    # 只有状态=FILLED 才会走 return True，这里代表我们没有活动单且余量=0 -> 不应出现
                    raise RuntimeError("[LPO] 余量为 0 但无活动订单，状态不一致")
                if min_qty > 0 and remaining_qty < min_qty:
                    # 无法新挂（数量不达标）且没有老单可等，视为无法继续推进
                    raise RuntimeError(
                        f"[LPO] 剩余 {remaining_qty} < minQty {min_qty}，且无活动订单可等待，无法完全成交"
                    )

                qty_str = _decimal_str(remaining_qty)
                params = dict(
                    symbol=symbol,
                    side=side,
                    type="LIMIT",
                    timeInForce="GTX",
                    quantity=qty_str,
                    price=price_str,
                    newClientOrderId=f"LPO_{uuid.uuid4().hex[:20]}",
                )
                if reduce_only:   params["reduceOnly"] = True
                if position_side: params["positionSide"] = position_side

                try:
                    resp = self.client.futures_create_order(**params)
                    active_order_id = resp.get("orderId")
                    active_client_oid = resp.get("clientOrderId") or params["newClientOrderId"]
                    active_price = target_price
                    order_reported_executed = 0.0
                    last_place_ts = time.time()
                    retry_2013 = 0
                    self.logger.info(
                        "[LPO] %s %s qty=%s @%s 已挂单（last=%.8f, bid=%.8f, ask=%.8f, mark=%s）。",
                        symbol, side, qty_str, price_str, last, best_bid, best_ask,
                        f"{mark:.8f}" if mark is not None else "NA"
                    )
                except BinanceAPIException as oe:
                    # GTX 拒单或精度报错 -> 下一轮重算价再试
                    self.logger.debug("[LPO] 下单失败/GTX拒单：%s", oe)
                    time.sleep(poll_interval)
                    continue

            # 轮询活动订单
            try:
                # 用 origClientOrderId 优先查，规避 -2013
                od = None
                if active_client_oid:
                    try:
                        od = self.client.futures_get_order(symbol=symbol, origClientOrderId=active_client_oid)
                    except BinanceAPIException as e1:
                        if e1.code != -2013:
                            raise
                if od is None and active_order_id is not None:
                    od = self.client.futures_get_order(symbol=symbol, orderId=active_order_id)

                st = od.get("status")
                exec_now = float(od.get("executedQty", "0"))
                orig_qty = float(od.get("origQty", remaining_qty))

                # 增量成交
                delta = max(0.0, exec_now - order_reported_executed)
                if delta > 0:
                    cumulative_filled += delta
                    remaining_qty = _quantize_to_step(max(0.0, initial_qty - cumulative_filled), step_str, "floor")
                    order_reported_executed = exec_now
                    self.logger.info("[LPO] 已成交 %.10f，剩余 %.10f", delta, remaining_qty)

                if st == "FILLED":
                    self.logger.info("[LPO] 订单完全成交：orderId=%s", active_order_id)
                    return True

                if st in ("CANCELED", "REJECTED", "EXPIRED"):
                    # 单子确实没了；清空，下一轮会重挂剩余（若剩余>=minQty，否则抛错）
                    active_order_id = None
                    active_client_oid = None
                    active_price = None
                    order_reported_executed = 0.0
                    continue

                # 计算是否需要换价：仅当未成交量 >= minQty 才允许撤单
                unfilled_active = max(0.0, float(orig_qty) - exec_now)
                need_replace = (active_price != target_price) and (unfilled_active >= (min_qty if min_qty > 0 else 0.0))

                if need_replace:
                    # 撤单 -> 可能再补一跳增量
                    try:
                        if active_order_id:
                            self.client.futures_cancel_order(symbol=symbol, orderId=active_order_id)
                        elif active_client_oid:
                            self.client.futures_cancel_order(symbol=symbol, origClientOrderId=active_client_oid)
                        self.logger.debug("[LPO] 撤单进行换价（未成交量=%.10f）", unfilled_active)
                    except BinanceAPIException as ce:
                        self.logger.debug("[LPO] 撤单提示：%s", ce)

                    # 撤单后补最后一跳
                    try:
                        od2 = None
                        if active_client_oid:
                            od2 = self.client.futures_get_order(symbol=symbol, origClientOrderId=active_client_oid)
                        if od2 is None and active_order_id:
                            od2 = self.client.futures_get_order(symbol=symbol, orderId=active_order_id)
                        exec_final = float((od2 or od).get("executedQty", exec_now))
                    except Exception:
                        exec_final = exec_now

                    delta2 = max(0.0, exec_final - order_reported_executed)
                    if delta2 > 0:
                        cumulative_filled += delta2
                        remaining_qty = _quantize_to_step(max(0.0, initial_qty - cumulative_filled), step_str, "floor")
                        order_reported_executed = exec_final
                        self.logger.info("[LPO] 撤单后补记增量 %.10f，剩余 %.10f", delta2, remaining_qty)

                    # 清空活动单，下一轮按新价重挂“剩余部分”
                    active_order_id = None
                    active_client_oid = None
                    active_price = None
                    order_reported_executed = 0.0
                    continue

                # 不换价就等下一轮
                time.sleep(poll_interval)
                retry_2013 = 0

            except BinanceAPIException as e:
                if e.code == -2013:
                    # 订单短暂不可见：保持活动状态不要清空，退避等待后重查
                    retry_2013 += 1
                    self.logger.debug("[LPO] 查询订单 -2013（第 %d 次），等待后重试", retry_2013)
                    if retry_2013 > max_2013_retries:
                        # 超过重试仍不可见，视为异常而非成交
                        raise RuntimeError("[LPO] 多次 -2013 后订单仍不可见，放弃并中止")
                    time.sleep(min_q_delay)
                    continue
                else:
                    self.logger.exception("[LPO] 查单异常：%s", e)
                    raise
            except Exception as e:
                self.logger.exception("[LPO] 异常：%s", e)
                raise
    # -----------------------------
    # 6) 获取某合约当前杠杆倍数
    # -----------------------------
    def get_symbol_leverage(self, symbol: str) -> Optional[float]:
        """
        获取当前账户在指定合约上的杠杆倍数（例如返回 5.0 表示 5x）。

        逻辑：
          1) 优先调用 futures_position_information(symbol=symbol)
             - 某些环境会返回空列表，这种情况继续走兜底方案。
          2) 若为空，则调用 futures_account()，在返回的 positions 里按 symbol 查找杠杆。
             - positions 通常会列出所有合约的杠杆设置，即便当前无持仓。

        参数：
          - symbol: 合约交易对名称，如 "BTCUSDT", "ETHUSDC" 等（注意要用 Binance 官方的 futures 符号）

        返回：
          - float 杠杆倍数，例如 5.0
          - 查询失败或未找到时返回 None
        """
        symbol = symbol.upper()

        try:
            acct = self.client.futures_account()
            positions = acct.get("positions", [])
            for p in positions:
                if p.get("symbol") == symbol:
                    lev_str = p.get("leverage")
                    if lev_str is None:
                        self.logger.warning(
                            "futures_account.positions 中缺少 leverage 字段：%s", p
                        )
                        return None
                    lev = float(lev_str)
                    self.logger.debug(
                        "通过 futures_account.positions 获取到 %s 杠杆=%s",
                        symbol, lev
                    )
                    return lev

            # 如果走到这里，说明两个地方都没找到这个 symbol
            self.logger.warning("账户中未找到该合约的杠杆信息：%s", symbol)
            return None

        except BinanceAPIException as e:
            self.logger.error("获取 %s 杠杆失败（BinanceAPIException）：%s", symbol, e)
        except Exception as e:
            self.logger.exception("获取 %s 杠杆时发生异常：%s", symbol, e)

        return None



if __name__ == "__main__":
    tm = TradingModule(config_path="./config.json")
    sym = "ETHUSDC"
    ok = tm.get_symbol_leverage("ETHUSDC")
    tm.adjust_position(
        symbol="ETHUSDC",
        target_qty=0.01,
        mode="track",
        duration_sec=60,
        poll_interval=0.5,
    )
    pos_info = tm.get_position(sym)
    print(f"{sym} position:", pos_info)



