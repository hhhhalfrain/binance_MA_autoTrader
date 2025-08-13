# run_trader.py —— 与 Futures-only MarketDataEngine 对接
import os
import time
import json
import logging
from dotenv import load_dotenv
from binance.client import Client
from tabulate import tabulate

from market_engine import MarketDataEngine
from trading_manager import TradingManager

load_dotenv()
MAX_LEN = 50

def load_config(path="config.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_config(cfg, path="config.json"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)

def print_status(client: Client, trader: TradingManager, cfg: dict):
    try:
        acc = client.futures_account()
        equity = float(acc.get("totalWalletBalance", 0.0))
    except Exception:
        equity = 0.0
    print("\n=== 当前配置与账户状态 ===")
    print(f"Mode: {trader.mode} | DryRun: {trader.dry_run} | Testnet: {trader.testnet}")
    print(f"Allocations: {trader.allocations}")
    print(f"Order types: {trader.per_symbol_order_type}")
    print(f"Equity(USDT): {equity:.2f}")
    try:
        poss = client.futures_position_information()
        for p in poss:
            amt = float(p.get("positionAmt", 0))
            if abs(amt) > 1e-12:
                sym = p.get("symbol")
                entry = float(p.get("entryPrice", 0))
                print(f"- {sym}: amt={amt} entry={entry}")
    except Exception as e:
        print(f"取仓位出错: {e}")
    print("=========================\n")

def compute_and_print_ma_table(engine: MarketDataEngine, symbol: str, intervals, base_weights: dict):
    rows = []
    total_weighted = 0.0
    all_ready = True
    for iv in intervals:
        ma7, ma14, ma28 = engine.compute_ma_values(symbol, iv)
        if ma7 is None or ma14 is None or ma28 is None:
            all_ready = False
            cond = 0
            d714 = d728 = d1428 = None
            contrib = 0.0
        else:
            cond = (1 if ma7 > ma14 else 0) + (1 if ma7 > ma28 else 0) + (1 if ma14 > ma28 else 0)
            d714 = ma7 - ma14
            d728 = ma7 - ma28
            d1428 = ma14 - ma28
            contrib = base_weights.get(iv, 0.0) * cond
        total_weighted += contrib
        rows.append([
            iv, None if ma7 is None else f"{ma7:.6f}",
            None if ma14 is None else f"{ma14:.6f}",
            None if ma28 is None else f"{ma28:.6f}",
            None if d714 is None else f"{d714:.6f}",
            None if d728 is None else f"{d728:.6f}",
            None if d1428 is None else f"{d1428:.6f}",
            cond, f"{base_weights.get(iv,0.0):.2f}", f"{contrib:.2f}"
        ])
    headers = ["interval","ma7","ma14","ma28","d7-14","d7-28","d14-28","cond","weight","contrib"]
    print("INFO:  " + " | ".join(headers))
    for r in rows:
        print("INFO:  " + " | ".join([str(c) if c is not None else "None" for c in r]))
    target_ratio = total_weighted / 3.0
    print(f"INFO:  最终加权建议持仓比例 target_ratio = {target_ratio:.4f} ({target_ratio*100:.2f}%)\n")
    return all_ready, target_ratio

def main():
    api_key = os.getenv("API_KEY")
    api_secret = os.getenv("API_SECRET")
    if not api_key or not api_secret:
        print("请先在 .env 中配置 API_KEY/API_SECRET")
        return

    cfg = load_config()
    symbols   = [s.upper() for s in cfg.get("symbols", ["BTCUSDT"])]
    intervals = cfg.get("intervals", ["5m", "15m", "1h", "4h", "1d"])
    allocations = {k.upper(): v for k, v in cfg.get("allocations", {"BTCUSDT": 1.0}).items()}
    mode = cfg.get("mode", "both")
    dry_run = cfg.get("dry_run", True)
    testnet = cfg.get("testnet", True)
    weights = cfg.get("weights", None)
    per_symbol_order_type = {k.upper(): v for k, v in cfg.get("per_symbol_order_type", {}).items()}

    client = Client(api_key, api_secret, testnet=testnet)

    # 仅合约的行情引擎
    engine = MarketDataEngine(symbols, intervals, api_key, api_secret,
                              max_len=MAX_LEN, testnet=testnet)

    trader = TradingManager(engine, client, allocations, mode=mode, dry_run=dry_run, testnet=testnet, base_weights=weights)
    trader.per_symbol_order_type.update(per_symbol_order_type)

    # 1) 启动行情引擎（回填 + WS）
    engine.start()

    # 2) 阻塞等待：所有周期 MA28 可算
    def _all_ma_ready(sym: str, ivs: list) -> bool:
        for iv in ivs:
            _, _, ma28 = engine.compute_ma_values(sym, iv)
            if ma28 is None:
                return False
        return True

    READY_TIMEOUT = 120
    for sym in symbols:
        deadline = time.time() + READY_TIMEOUT
        while True:
            if _all_ma_ready(sym, intervals):
                break
            if time.time() > deadline:
                raise TimeoutError(f"[Boot] 等待 {sym} 的 MA 数据超时（>{READY_TIMEOUT}s）。")
            time.sleep(0.5)

    # 3) 打印 ready 报告
    for sym in symbols:
        print(engine.ready_report(sym))

    # 4) 首轮对账（不等事件）
    for sym in symbols:
        try:
            ot = trader.per_symbol_order_type.get(sym, "market")
            trader._reconcile_symbol(sym, order_type=ot)
        except Exception as e:
            print(f"[Boot] 首轮对账失败 {sym}: {e}")

    # 5) 启动交易消费者线程
    trader.start()

    # 6) CLI
    print("\n=== 交互命令 ===")
    print(" manual                -> 人工下单测试（输入交易对与目标净仓位比 -1.0~1.0）")
    print(" set_order_type        -> 设置某交易对下单类型 (market/tracking)")
    print(" set_alloc             -> 设置某交易对资金分配比例 (可 > 1 用于加杠杆)")
    print(" show                  -> 显示当前配置与账户状态摘要")
    print(" show_ma               -> 打印 MA 调试表格（单个或 ALL）")
    print(" save                  -> 保存运行期变更到 config.json")
    print(" quit                  -> 退出程序")
    print("=================\n")

    try:
        while True:
            cmd = input("输入命令: ").strip().lower()
            if cmd in ("quit", "exit", "q"):
                break

            elif cmd == "manual":
                try:
                    sym = input("交易对(如 BTCUSDT): ").strip().upper()
                    ratio = float(input("目标净仓位比(-1.0~1.0): ").strip())
                    if sym not in trader.allocations:
                        trader.allocations[sym] = 1.0
                        print(f"提示: {sym} 不在 allocations 中，临时设为 1.0")
                    engine.event_queue.put({
                        "etype": "manual",
                        "symbol": sym,
                        "target_ratio": (ratio + 1.0) / 2.0,  # -1..1 映射到 0..1
                        "timestamp": int(time.time()*1000),
                        "time_str": time.strftime("%F %T"),
                    })
                    print(f"✅ 已提交手动事件: {sym} target_net_frac={ratio}")
                except Exception as e:
                    print(f"手动事件提交失败: {e}")

            elif cmd == "set_order_type":
                sym = input("交易对: ").strip().upper()
                ot = input("下单类型(market/tracking): ").strip().lower()
                if ot not in ("market", "tracking"):
                    print("❌ 仅支持 market 或 tracking")
                    continue
                trader.per_symbol_order_type[sym] = ot
                print(f"✅ 已设置 {sym} 下单类型为 {ot}")

            elif cmd == "set_alloc":
                sym = input("交易对: ").strip().upper()
                try:
                    trader.allocations[sym] = float(input("资金分配比例(可 >1): ").strip())
                    print(f"✅ 已设置 {sym} 分配比例为 {trader.allocations[sym]}")
                except ValueError:
                    print("❌ 请输入合法数字")

            elif cmd == "show":
                print_status(client, trader, cfg)

            elif cmd == "show_ma":
                target = input("交易对(输入 ALL 打印全部): ").strip().upper()
                if target in ("ALL", "*"):
                    for s in symbols:
                        print(f"\n=== {s} ===")
                        compute_and_print_ma_table(engine, s, intervals, trader.base_weights)
                else:
                    if target not in symbols:
                        print(f"❌ {target} 不在 symbols 列表: {symbols}")
                    else:
                        compute_and_print_ma_table(engine, target, intervals, trader.base_weights)

            elif cmd == "save":
                cfg["per_symbol_order_type"] = trader.per_symbol_order_type
                cfg["allocations"] = trader.allocations
                save_config(cfg)
                print("✅ 配置已保存到 config.json")

            elif cmd == "":
                continue
            else:
                print("未知命令。可用命令：manual | set_order_type | set_alloc | show | show_ma | save | quit")
    except KeyboardInterrupt:
        pass
    finally:
        print("停止中...")
        trader.stop()
        engine.stop()

if __name__ == "__main__":
    logging.getLogger("binance").setLevel(logging.WARNING)
    main()
