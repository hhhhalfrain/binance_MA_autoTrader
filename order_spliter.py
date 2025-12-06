from dataclasses import dataclass
from typing import List
from Util.trading_module import TradingModule

@dataclass
class Order:
    symbol: str
    margin: float       # 剩余名义价值/数量（按你的业务定义）
    side: str           # "BUY" / "SELL"
    step: float         # 每次尝试的下单数量上限
    offset: float


POLL_INTERVAL = 0.3

def process_orders(tm: TradingModule, orders: List[Order]) -> None:
    # 基础校验
    for o in orders:
        if o.step <= 0:
            raise ValueError(f"step 必须为正数: {o.symbol} step={o.step}")
        if o.margin < 0:
            raise ValueError(f"margin 不能为负: {o.symbol} margin={o.margin}")
        if o.side not in {"BUY", "SELL"}:
            raise ValueError(f"非法 side: {o.side}")

    # 只要还有订单有剩余 margin，就继续轮询
    while any(o.margin > 0 for o in orders):
        for o in orders:
            if o.margin <= 0:
                continue
            qty = min(o.margin, o.step)

            try:
                ok = tm.last_price_offset_maker_track(
                    symbol=o.symbol,
                    qty=qty,
                    side=o.side,
                    offset=o.offset,
                    poll_interval=POLL_INTERVAL,
                )
            except Exception as e:
                # 记录异常，下一轮重试（不退出整个批处理）
                print(f"[WARN] {o.symbol} 下单异常: {e}")
                continue

            if ok:
                # 成功成交，扣减剩余
                o.margin -= qty
                # 防止浮点尾差
                if o.margin < 1e-12:
                    o.margin = 0.0
            else:
                # 未成交：不扣减，等待下一轮
                pass

if __name__ == "__main__":
    orders = [
        Order(symbol="BIOUSDT", margin=1000, side="BUY", step=1000, offset=0.00001),
        Order(symbol="BIOUSDC", margin=1000, side="SELL", step=1000,offset=0.00001),
    ]
    tm = TradingModule(r"Util/config.json")
    process_orders(tm, orders)
    print("[DONE] 全部订单处理完成")
