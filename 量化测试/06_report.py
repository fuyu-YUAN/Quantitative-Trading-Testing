# -*- coding: utf-8 -*-

"""
06_report.py
报告输出模块
- 选股清单生成
- 保存到CSV/TXT
- 控制台美化输出
- 可选：绘制图表
"""

import os
import pandas as pd
from datetime import datetime
from tabulate import tabulate

import config


def format_stock_list(result_df, title="选股结果"):
    """
    格式化输出选股清单
    """
    if result_df.empty:
        print(f"\n[{title}] 无符合条件的股票")
        return ""

    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"  生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  共 {len(result_df)} 只候选股票")
    print(f"{'='*80}")

    # 选择要显示的列
    display_cols_map = {
        "code": "代码",
        "name": "名称",
        "close": "收盘价",
        "price": "最新价",
        "pct_change": "涨跌%",
        "vol_ratio": "量比",
        "vol_ratio_rt": "实时量比",
        "rsi": "RSI",
        "golden_cross": "金叉",
        "breakout": "突破",
        "score_total": "总分",
        "score_ma_cross": "金叉分",
        "score_volume": "量分",
        "score_breakout": "突破分",
        "score_rsi": "RSI分",
        "score_trend": "趋势分",
        "score_macd": "MACD分",
    }

    # 只选存在的列
    show_cols = [c for c in display_cols_map.keys() if c in result_df.columns]
    show_names = [display_cols_map[c] for c in show_cols]

    display_df = result_df[show_cols].copy()
    display_df.columns = show_names

    # 序号
    display_df.insert(0, "排名", range(1, len(display_df) + 1))

    # 格式化数值
    for col in display_df.columns:
        if col in ["涨跌%", "RSI", "量比", "实时量比"]:
            display_df[col] = display_df[col].apply(
                lambda x: f"{x:.2f}" if pd.notna(x) else "-"
            )
        elif col in ["总分", "金叉分", "量分", "突破分", "RSI分", "趋势分", "MACD分"]:
            display_df[col] = display_df[col].apply(
                lambda x: f"{x:.1f}" if pd.notna(x) else "-"
            )
        elif col in ["收盘价", "最新价"]:
            display_df[col] = display_df[col].apply(
                lambda x: f"{x:.2f}" if pd.notna(x) else "-"
            )

    table_str = tabulate(
        display_df,
        headers="keys",
        tablefmt="simple",
        showindex=False,
        numalign="right",
        stralign="center"
    )

    print(table_str)
    print(f"{'='*80}\n")

    return table_str


def save_to_csv(result_df, filename=None, subdir="reports"):
    """
    保存结果到CSV
    """
    if result_df.empty:
        return None

    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"stock_picks_{timestamp}.csv"

    save_dir = os.path.join(config.DATA_DIR, subdir)
    os.makedirs(save_dir, exist_ok=True)

    filepath = os.path.join(save_dir, filename)
    result_df.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"[INFO] 结果已保存: {filepath}")

    return filepath


def save_to_txt(result_df, table_str="", filename=None, extra_info=""):
    """
    保存结果到TXT (人工可读格式)
    """
    if result_df.empty:
        return None

    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"stock_picks_{timestamp}.txt"

    save_dir = os.path.join(config.DATA_DIR, "reports")
    os.makedirs(save_dir, exist_ok=True)

    filepath = os.path.join(save_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"量化选股报告\n")
        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"候选数量: {len(result_df)} 只\n")
        f.write(f"\n")

        if extra_info:
            f.write(extra_info)
            f.write(f"\n")

        if table_str:
            f.write(table_str)
        else:
            f.write(result_df.to_string(index=False))

        f.write(f"\n\n--- END ---\n")

    print(f"[INFO] 报告已保存: {filepath}")
    return filepath


def save_backtest_report(stats, trades_df, equity_df, filename=None):
    """
    保存回测报告
    """
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"backtest_{timestamp}"

    save_dir = os.path.join(config.DATA_DIR, "backtest")
    os.makedirs(save_dir, exist_ok=True)

    # 保存统计摘要
    stats_path = os.path.join(save_dir, f"{filename}_stats.txt")
    with open(stats_path, "w", encoding="utf-8") as f:
        f.write("回测统计报告\n")
        f.write(f"{'='*40}\n")
        for k, v in stats.items():
            f.write(f"{k}: {v}\n")
    print(f"[INFO] 统计报告: {stats_path}")

    # 保存交易明细
    if not trades_df.empty:
        trades_path = os.path.join(save_dir, f"{filename}_trades.csv")
        trades_df.to_csv(trades_path, index=False, encoding="utf-8-sig")
        print(f"[INFO] 交易明细: {trades_path}")

    # 保存净值曲线
    if not equity_df.empty:
        equity_path = os.path.join(save_dir, f"{filename}_equity.csv")
        equity_df.to_csv(equity_path, index=False, encoding="utf-8-sig")
        print(f"[INFO] 净值曲线: {equity_path}")


def plot_equity_curve(equity_df, save_path=None):
    """
    绘制净值曲线 (可选)
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib

        # 设置中文字体
        matplotlib.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "Arial"]
        matplotlib.rcParams["axes.unicode_minus"] = False

        if equity_df.empty:
            print("[WARN] 无净值数据")
            return

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), gridspec_kw={"height_ratios": [3, 1]})

        # 净值曲线
        ax1.plot(equity_df["date"], equity_df["equity"], "b-", linewidth=1.5, label="净值")
        ax1.axhline(y=config.BACKTEST["initial_capital"], color="gray", linestyle="--", alpha=0.5, label="初始资金")
        ax1.set_title("回测净值曲线", fontsize=14)
        ax1.set_ylabel("净值 (元)")
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # 持仓数量
        ax2.fill_between(equity_df["date"], equity_df["positions"], alpha=0.3, color="orange")
        ax2.plot(equity_df["date"], equity_df["positions"], "orange", linewidth=1)
        ax2.set_ylabel("持仓数")
        ax2.set_xlabel("日期")
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches="tight")
            print(f"[INFO] 净值曲线图: {save_path}")
        else:
            plt.show()

        plt.close()

    except ImportError:
        print("[WARN] matplotlib 未安装, 跳过图表绘制")
    except Exception as e:
        print(f"[WARN] 绘图失败: {e}")


def generate_daily_report(result_df, backtest_stats=None):
    """
    生成完整的每日报告
    """
    timestamp = datetime.now().strftime("%Y%m%d")

    # 1. 控制台输出
    table_str = format_stock_list(result_df, title=f"{timestamp} 选股清单")

    # 2. 额外信息
    extra = ""
    if backtest_stats:
        extra += "\n--- 回测验证 ---\n"
        for k, v in backtest_stats.items():
            extra += f"  {k}: {v}\n"

    # 3. 保存文件
    save_to_csv(result_df, filename=f"picks_{timestamp}.csv")
    save_to_txt(result_df, table_str, filename=f"picks_{timestamp}.txt", extra_info=extra)

    return result_df


# ==================== 测试入口 ====================
if __name__ == "__main__":
    # 模拟数据测试
    test_data = pd.DataFrame({
        "code": ["000001", "600519", "000858", "002415", "601318"],
        "name": ["平安银行", "贵州茅台", "五粮液", "海康威视", "中国平安"],
        "close": [12.50, 1680.00, 155.30, 35.80, 48.20],
        "pct_change": [2.13, 1.05, -0.38, 3.25, 0.84],
        "vol_ratio": [1.8, 1.2, 0.9, 2.5, 1.1],
        "rsi": [55.3, 48.2, 42.1, 62.5, 51.8],
        "golden_cross": [1, 0, 0, 1, 0],
        "breakout": [1, 0, 0, 1, 1],
        "score_total": [85.5, 62.3, 45.0, 78.2, 58.7],
        "score_ma_cross": [25, 8, 0, 25, 0],
        "score_volume": [18, 12, 6, 20, 10],
        "score_breakout": [20, 10, 0, 15, 20],
        "score_rsi": [15, 15, 12, 8, 15],
        "score_trend": [5, 10, 10, 5, 8],
        "score_macd": [2.5, 7.3, 17, 5.2, 5.7],
    })

    generate_daily_report(test_data)