from quant_info_module import QuantInfoModule


q = QuantInfoModule("config.json")

bars = q.get_klines("BTCUSDT", n=1500, interval="15m")
from strategy_ema_sar_boll import EMASARBollBandwidthStrategy, StrategyConfig

klines = q.get_klines("BTCUSDT", 1500, "15m")  # 返回含 high/low/close 的字典列表

cfg = StrategyConfig(
    ema_periods=(20,30,60),
    ema_order_weight=0.6, ema_slope_weight=0.4,
    f1_weight_ema=0.7, f1_weight_sar=0.3,
    boll_period=20, boll_mult=2.0,
    lookback_for_percentile=500,
    f2_width_base="ma",          # "ma" | "price" | "none"
    f2_level_invert=False,        # 宽→大→仓位小
    f2_weight_level=0.7,
    f2_weight_slope=0.3,
    f2_slope_scale=3.0
)
strategy = EMASARBollBandwidthStrategy(cfg)

pos, factors = strategy.position_size(klines, return_factors=True)
print("持仓大小(0~1):", pos)
print("因子1(EMA+SAR):", factors["f1"], "映射:", "因子2(Boll正态CDF):", factors["f2"])
