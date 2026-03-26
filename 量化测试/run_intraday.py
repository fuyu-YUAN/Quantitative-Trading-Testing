# -*- coding: utf-8 -*-
"""
run_intraday.py
盘中实时监控流程:
- 每隔3分钟刷新实时行情
- 结合历史指标进行选股
- 实时输出候选清单
"""

import os
import sys
import time
import pandas as pd
from datetime import datetime
from tqdm import tqdm

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from _01_data_history import load_kline, get_stock_pool
from _02_data_realtime import get_realtime_all, is_trading_time, get_market_overview
from _03_features import calc_all_features
from _04_strategy_rules import quick_screen_with_realtime
from _06_report import format_stock_list, save_to_csv


def load_history_data():
    """
    预加载历史数据到内存
    """
    print("[INFO] 正在预加载历史数据...")

    pool_path = os.path.join(config.DATA_DIR, "stock_pool.csv")
    if os.path.exists(pool_path):
        stock_pool = pd.read_csv(pool_path, encoding="utf-8-sig")
        stock_pool["code"] = stock_pool["code"].astype(str).str.zfill(6)
    else:
        stock_pool = get_stock_pool()

    history_dict = {}
    for _, row in tqdm(stock_pool.iterrows(), total=len(stock_pool), desc="加载历史"):
        code = row["code"]
        df = load_kline(code, "daily")
        if not df.empty and len(df) >= 60:
            # 预计算指标
            df = calc_all_features(df)
            history_dict[code] = df

    print(f"[INFO] 预加载完成: {len(history_dict)} 只股票")
    return history_dict


def run_one_scan(history_dict, scan_count=0):
    """
    执行一次扫描
    """
    top_n = config.INTRADAY["top_n"]

    print(f"\n{'─'*60}")
    print(f"  [第 {scan_count} 次扫描]  {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'─'*60}")

    # 1. 获取市场概况
    overview = get_market_overview()
    if overview:
        print(f"  上涨: {overview.get('up_count', 0)} | "
              f"下跌: {overview.get('down_count', 0)} | "
              f"涨停: {overview.get('limit_up', 0)} | "
              f"跌停: {overview.get('limit_down', 0)} | "
              f"成交: {overview.get('total_amount', 0):.0f}亿")

    # 2. 获取实时行情
    realtime_df = get_realtime_all()
    if realtime_df.empty:
        print("[WARN] 获取实时行情失败, 等待下次扫描")
        return pd.DataFrame()

    # 3. 价格预过滤 (提速)
    p = config.STRATEGY
    realtime_df = realtime_df[
        (realtime_df["price"] >= p["price_min"]) &
        (realtime_df["price"] <= p["price_max"]) &
        (realtime_df["pct_change"] > -5) &
        (realtime_df["pct_change"] < 9.5) &
        (realtime_df["volume"] > 0)
    ]

    # 只对有历史数据的股票筛选
    available_codes = set(history_dict.keys())
    realtime_df = realtime_df[realtime_df["code"].isin(available_codes)]

    print(f"  候选池: {len(realtime_df)} 只 (有历史数据)")

    # 4. 快速选股
    result = quick_screen_with_realtime(realtime_df, history_dict, top_n=top_n)

    # 5. 输出
    if not result.empty:
        format_stock_list(result, title=f"盘中候选 {datetime.now().strftime('%H:%M')}")
    else:
        print("  [暂无符合条件的股票]")

    return result


def run_intraday_loop():
    """
    盘中循环监控
    """
    print(f"\n{'='*60}")
    print(f"  量化选股系统 - 盘中实时监控")
    print(f"  启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  刷新间隔: {config.INTRADAY['refresh_interval']} 秒")
    print(f"{'='*60}")

    # 预加载历史数据
    history_dict = load_history_data()

    if not history_dict:
        print("[ERROR] 没有历史数据! 请先运行 run_daily.py 下载数据")
        return

    scan_count = 0
    last_result = pd.DataFrame()

    print("\n[INFO] 开始盘中监控... (按 Ctrl+C 停止)")

    while True:
        try:
            # 检查是否在交易时间
            if not is_trading_time():
                now = datetime.now()
                current_time = now.strftime("%H:%M")

                if current_time < "09:30":
                    print(f"\r  等待开盘... 当前 {current_time}", end="", flush=True)
                elif current_time > "15:00":
                    print(f"\n[INFO] 今日交易已结束 ({current_time})")

                    # 保存最后一次结果
                    if not last_result.empty:
                        timestamp = now.strftime("%Y%m%d_%H%M")
                        save_to_csv(last_result, filename=f"intraday_final_{timestamp}.csv")

                    break
                else:
                    print(f"\r  午间休市... 当前 {current_time}", end="", flush=True)

                time.sleep(30)
                continue

            # 执行扫描
            scan_count += 1
            last_result = run_one_scan(history_dict, scan_count)

            # 保存结果 (每10次保存一次)
            if scan_count % 10 == 0 and not last_result.empty:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                save_to_csv(last_result, filename=f"intraday_{timestamp}.csv")

            # 等待
            interval = config.INTRADAY["refresh_interval"]
            print(f"\n  下次扫描: {interval}秒后...")
            time.sleep(interval)

        except KeyboardInterrupt:
            print(f"\n\n[INFO] 用户中断, 停止监控")

            if not last_result.empty:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                save_to_csv(last_result, filename=f"intraday_stop_{timestamp}.csv")

            break

        except Exception as e:
            print(f"\n[ERROR] 异常: {e}")
            print("[INFO] 60秒后重试...")
            time.sleep(60)


def run_single_scan():
    """
    单次扫描 (不循环, 适合手动测试)
    """
    history_dict = load_history_data()
    if not history_dict:
        print("[ERROR] 没有历史数据!")
        return

    result = run_one_scan(history_dict, scan_count=1)

    if not result.empty:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_to_csv(result, filename=f"scan_{timestamp}.csv")

    return result


# ==================== 入口 ====================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="量化选股 - 盘中监控")
    parser.add_argument("--once", action="store_true", help="只扫描一次")
    parser.add_argument("--force", action="store_true", help="强制运行(忽略交易时间检查)")

    args = parser.parse_args()

    if args.once:
        run_single_scan()
    elif args.force:
        # 强制模式: 跳过交易时间检查, 直接扫描一次
        history_dict = load_history_data()
        if history_dict:
            run_one_scan(history_dict, scan_count=1)
    else:
        run_intraday_loop()