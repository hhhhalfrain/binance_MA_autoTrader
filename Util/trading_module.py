"""
量化交易 - 交易模块（单线程）
功能：
  1) 市价交易（按合约张数，向下截断最小步进）
  2) 限价只挂单追踪交易（Post Only，价格变动则撤单重下）
  3) 获取当前仓位大小与方向
  4) 获取合约账户中某币种余额
  5) 获取某币种最新指数价格（Index Price）

说明：
  - 使用 .env 存放 API_KEY / API_SECRET（testnet 用 TEST_API_KEY/TEST_API_SECRET）
  - 若 .env 存在则忽略 JSON 中的 API 字段
"""

import os
import json
import time
import logging
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional, Tuple
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
from binance.client import Client
from binance.exceptions import BinanceAPIException
from config_manager import ConfigManager

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
    def get_position(self, symbol: str) -> Dict[str, Any]:
        """
        返回：
          - qty: float（>0 表示多头，<0 表示空头，=0 表示无仓）
          - direction: "LONG" / "SHORT" / "FLAT"
        说明：基于 USDⓈ-M futures 的 positionAmt 字段。
        """
        try:
            info = self.client.futures_position_information(symbol=symbol)
            if not info:
                return {"qty": 0.0, "direction": "FLAT"}

            # 单向持仓模式：通常只会有一条记录
            pos_amt = float(info[0].get("positionAmt", "0"))
            if pos_amt > 0:
                return {"qty": pos_amt, "direction": "LONG"}
            elif pos_amt < 0:
                return {"qty": pos_amt, "direction": "SHORT"}
            else:
                return {"qty": 0.0, "direction": "FLAT"}

        except BinanceAPIException as e:
            self.logger.error("获取持仓失败：%s", e)
        except Exception as e:
            self.logger.exception("解析持仓异常：%s", e)
        return {"qty": 0.0, "direction": "FLAT"}

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
        target = 0.0 if target_qty < 0 else float(target_qty)

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


if __name__ == "__main__":
    tm = TradingModule(config_path="config.json")
