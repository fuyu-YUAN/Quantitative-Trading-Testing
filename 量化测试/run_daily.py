# -*- coding: utf-8 -*-

"""
run_daily.py
每日收盘后运行的主流程:
1. 更新历史K线数据
2. 计算技术指标
3. 选股打分
4. 回测验证
5. 输出报告
"""

import os
import sys
import pandas as pd
from datetime import datetime
from tqdm import tqdm

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from _01_data_history import (
    get_stock_pool, update_all_history, load_kline, download_full_history
)
from _03_features import calc_all_features
from _04_strategy_rules import screen_stocks
from _05_backtest import BacktestEngine
from _06_report import (
    generate_daily_report, save_backtest_report,
    plot_equity_curve, format_stock_list
)


def step1_update_data():
    """
    步骤1: 更新历史数据
    """
    print(f"\n{'#'*60}")
    print(f"# 步骤1: 更新历史K线数据")
    print(f"{'#'*60}")

    stock_pool = get_stock_pool()
    pool_path = os.path.join(config.DATA_DIR, "stock_pool.csv")
    stock_pool.to_csv(pool_path, index=False, encoding="utf-8-sig")

    # 更新日K
    update_all_history(stock_pool, freq="daily", sleep_interval=0.2)

    # 更新周K
    if config.WEEKLY_ENABLED:
        update_all_history(stock_pool, freq="weekly", sleep_interval=0.2)

    return stock_pool


def step2_load_and_calc(stock_pool):
    """
    步骤2: 加载数据 + 计算指标
    """
    print(f"\n{'#'*60}")
    print(f"# 步骤2: 加载数据并计算技术指标")
    print(f"{'#'*60}")

    data_dict = {}
    for _, row in tqdm(stock_pool.iterrows(), total=len(stock_pool), desc="加载数据"):
        code = row["code"]
        df = load_kline(code, "daily")
        if not df.empty and len(df) >= 60:
            df["name"] = row.get("name", "")
            data_dict[code] = df

    print(f"[INFO] 成功加载 {len(data_dict)} 只股票数据")
    return data_dict


def step3_screen(data_dict, top_n=20):
    """
    步骤3: 选股打分
    """
    print(f"\n{'#'*60}")
    print(f"# 步骤3: 选股打分")
    print(f"{'#'*60}")

    result = screen_stocks(data_dict, top_n=top_n)
    return result


def step4_backtest(data_dict):
    """
    步骤4: 回测验证
    """
    print(f"\n{'#'*60}")
    print(f"# 步骤4: 策略回测验证")
    print(f"{'#'*60}")

    # 随机抽样回测 (全量太慢,取100只)
    if len(data_dict) > 100:
        import random
        sample_codes = random.sample(list(data_dict.keys()), 100)
        sample_dict = {k: data_dict[k] for k in sample_codes}
    else:
        sample_dict = data_dict

    engine = BacktestEngine()
    stats = engine.run_backtest(sample_dict)
    engine.print_report(stats)

    # 保存回测结果
    trades_df = engine.get_trades_df()
    equity_df = engine.get_equity_df()
    timestamp = datetime.now().strftime("%Y%m%d")
    save_backtest_report(stats, trades_df, equity_df, filename=f"bt_{timestamp}")

    # 绘制净值曲线
    if not equity_df.empty:
        chart_path = os.path.join(config.BACKTEST_DIR, f"bt_{timestamp}_curve.png")
        plot_equity_curve(equity_df, save_path=chart_path)

    return stats


def step5_report(result_df, backtest_stats=None):
    """
    步骤5: 生成报告
    """
    print(f"\n{'#'*60}")
    print(f"# 步骤5: 生成报告")
    print(f"{'#'*60}")

    generate_daily_report(result_df, backtest_stats)


def run_daily_pipeline(skip_update=False, top_n=20, run_backtest=True):
    """
    完整的每日流程
    
    参数:
        skip_update: 是否跳过数据更新 (如果今天已经更新过)
        top_n: 输出前N只候选
        run_backtest: 是否运行回测
    """
    start_time = datetime.now()
    print(f"\n{'='*60}")
    print(f"  量化选股系统 - 每日流程")
    print(f"  运行时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # Step 1: 更新数据
    if not skip_update:
        stock_pool = step1_update_data()
    else:
        pool_path = os.path.join(config.DATA_DIR, "stock_pool.csv")
        if os.path.exists(pool_path):
            stock_pool = pd.read_csv(pool_path, encoding="utf-8-sig")
            # 确保code列为字符串并补零
            stock_pool["code"] = stock_pool["code"].astype(str).str.zfill(6)
        else:
            stock_pool = get_stock_pool()
        print(f"[INFO] 跳过数据更新, 使用本地数据")

    # Step 2: 加载+指标
    data_dict = step2_load_and_calc(stock_pool)

    if not data_dict:
        print("[ERROR] 没有可用数据, 请先下载历史数据")
        return

    # Step 3: 选股
    result = step3_screen(data_dict, top_n=top_n)

    # Step 4: 回测
    bt_stats = None
    if run_backtest:
        bt_stats = step4_backtest(data_dict)

    # Step 5: 报告
    step5_report(result, bt_stats)

    # 完成
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n{'='*60}")
    print(f"  流程完成! 耗时: {elapsed:.1f} 秒")
    print(f"{'='*60}")


# ==================== 入口 ====================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="量化选股 - 每日流程")
    parser.add_argument("--skip-update", action="store_true", help="跳过数据更新")
    parser.add_argument("--top", type=int, default=20, help="输出前N只候选")
    parser.add_argument("--no-backtest", action="store_true", help="跳过回测")
    parser.add_argument("--download-only", action="store_true", help="仅下载数据")

    args = parser.parse_args()

    if args.download_only:
        download_full_history()
    else:
        run_daily_pipeline(
            skip_update=args.skip_update,
            top_n=args.top,
            run_backtest=not args.no_backtest
        )