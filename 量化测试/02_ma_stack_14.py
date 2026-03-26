"""
02_ma_stack_14.py
全市场信号扫描 - 从已下载的历史数据中选股（专用版）

策略信号:
  MA(5,10,20) 多头排列 连续 14 天：
    对最近14个交易日，每天满足 MA5 > MA10 > MA20

排序:
  按最近1日涨幅（pctChg 或用 close 计算）降序

范围:
  仅主板（沪A 60xxxx + 深A 00xxxx）
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

# ============================================================
#  配置
# ============================================================
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'history')
RESULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'results')

# 均线参数
MA_PERIODS = (5, 10, 20)
STACK_DAYS = 14

# 输出数量
TOP_N = 50

# 主板代码前缀
MAIN_BOARD_PREFIX = ('600', '601', '603', '605',
                     '000', '001', '002', '003')


# ============================================================
#  工具函数
# ============================================================
def calc_ma(series, period):
    return series.rolling(window=period).mean()


def get_pct_change(df):
    """优先使用pctChg字段，否则用close计算"""
    if len(df) < 2:
        return 0.0
    last = df.iloc[-1]
    prev = df.iloc[-2]

    if 'pctChg' in df.columns:
        v = pd.to_numeric(last.get('pctChg', 0), errors='coerce')
        if pd.isna(v):
            v = 0.0
        return float(v)

    if pd.notna(prev.get('close', np.nan)) and prev['close'] > 0:
        return float((last['close'] - prev['close']) / prev['close'] * 100)
    return 0.0


def is_ma_bull_stack(df, periods=(5, 10, 20), days=14):
    """
    最近days个交易日是否每天满足 MA短 > MA中 > MA长（periods按短到长排列）
    返回: (True/False, last_ma_dict)
    """
    need = max(periods) + days + 2
    if len(df) < need:
        return False, None

    mas = {p: calc_ma(df['close'], p) for p in periods}

    cond = pd.Series(True, index=df.index)
    for i in range(len(periods) - 1):
        p1, p2 = periods[i], periods[i + 1]
        cond = cond & (mas[p1] > mas[p2])

    tail = cond.tail(days)

    # 任意NaN都视为不满足（说明均线不够数据或数据缺失）
    if tail.isna().any():
        return False, None

    ok = bool(tail.all())
    if not ok:
        return False, None

    last_ma = {f"ma{p}": float(mas[p].iloc[-1]) if pd.notna(mas[p].iloc[-1]) else np.nan
               for p in periods}
    return True, last_ma


# ============================================================
#  分析单只股票
# ============================================================
def analyze_stock(code, filepath):
    try:
        df = pd.read_csv(filepath)
    except Exception:
        return None

    if df is None or df.empty:
        return None

    # 标准化字段为数值
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['close'])

    # 数据不足直接跳过
    if len(df) < max(MA_PERIODS) + STACK_DAYS + 2:
        return None

    ok, last_ma = is_ma_bull_stack(df, MA_PERIODS, STACK_DAYS)
    if not ok:
        return None

    last = df.iloc[-1]
    pct_change = get_pct_change(df)

    # 额外展示：MA5- MA20乖离（可帮助看强度）
    ma5 = last_ma.get('ma5', np.nan)
    ma20 = last_ma.get('ma20', np.nan)
    bias_5_20 = (ma5 - ma20) / ma20 * 100 if pd.notna(ma5) and pd.notna(ma20) and ma20 != 0 else np.nan

    return {
        'code': code,
        'date': last.get('date', ''),
        'close': float(last['close']),
        'pct_change': round(float(pct_change), 2),
        'stack_days': STACK_DAYS,
        'ma5': round(last_ma['ma5'], 4),
        'ma10': round(last_ma['ma10'], 4),
        'ma20': round(last_ma['ma20'], 4),
        'bias_5_20_pct': round(float(bias_5_20), 2) if pd.notna(bias_5_20) else np.nan,
        'signal': f"MA5>MA10>MA20 连续{STACK_DAYS}天"
    }


# ============================================================
#  主函数
# ============================================================
def main():
    today = datetime.now().strftime('%Y-%m-%d')

    print("=" * 70)
    print("  全市场量化选股信号扫描（专用版）")
    print(f"  扫描日期: {today}")
    print(f"  策略: MA{MA_PERIODS} 多头排列连续 {STACK_DAYS} 天")
    print("  排序: 最近涨幅（降序）")
    print("  范围: 仅主板（沪A 60xxxx + 深A 00xxxx）")
    print("=" * 70)

    if not os.path.exists(DATA_DIR):
        print(f"\n❌ 数据目录不存在: {DATA_DIR}")
        print("请先运行 01_data_history.py 下载数据!")
        return

    all_files = [f for f in os.listdir(DATA_DIR) if f.endswith('_daily.csv')]
    files = []
    for f in all_files:
        code = f.replace('_daily.csv', '')
        if code.startswith(MAIN_BOARD_PREFIX):
            files.append((code, os.path.join(DATA_DIR, f)))

    total = len(files)
    if total == 0:
        print("\n❌ 没有找到主板股票数据，请先运行 01_data_history.py!")
        return

    print(f"\n  找到 {len(all_files)} 个数据文件，其中主板 {total} 只")
    print("  开始扫描...\n")

    results = []
    errors = 0

    for i, (code, filepath) in enumerate(files):
        if (i + 1) % 500 == 0 or (i + 1) == total:
            print(f"  进度: [{i+1}/{total}]  已命中: {len(results)} 只")

        try:
            r = analyze_stock(code, filepath)
            if r:
                results.append(r)
        except Exception:
            errors += 1

    # 按最近涨幅排序（降序）
    results.sort(key=lambda x: x.get('pct_change', -9999), reverse=True)

    if not results:
        print("\n" + "=" * 70)
        print("  今日未发现符合条件的股票")
        print(f"  条件: MA{MA_PERIODS} 多头排列连续 {STACK_DAYS} 天")
        print("=" * 70)
        return

    # ============================================================
    #  打印 TOP
    # ============================================================
    show_n = min(TOP_N, len(results))
    print(f"\n\n{'#'*70}")
    print(f"#  MA{MA_PERIODS} 多头排列连续{STACK_DAYS}天 - TOP {show_n}")
    print(f"#  命中总数: {len(results)} 只 | 扫描: {total} 只 | 错误: {errors} 只")
    print(f"{'#'*70}\n")

    print(f"  {'排名':<5}{'代码':<11}{'现价':<10}{'涨幅':<10}"
          f"{'MA5':<10}{'MA10':<10}{'MA20':<10}{'乖离(5-20)':<12}{'信号'}")
    print("  " + "-" * 105)

    for rank, r in enumerate(results[:show_n], 1):
        pct_str = f"{r['pct_change']:+.2f}%"
        bias_str = f"{r['bias_5_20_pct']:+.2f}%" if pd.notna(r.get('bias_5_20_pct', np.nan)) else "N/A"
        print(f"  {rank:<5}{r['code']:<11}{r['close']:<10.2f}{pct_str:<10}"
              f"{r['ma5']:<10.2f}{r['ma10']:<10.2f}{r['ma20']:<10.2f}{bias_str:<12}{r['signal']}")

    print("  " + "-" * 105)

    # ============================================================
    #  保存结果
    # ============================================================
    os.makedirs(RESULT_DIR, exist_ok=True)

    csv_file = os.path.join(RESULT_DIR, f"ma_stack_{STACK_DAYS}d_{today}.csv")
    df_all = pd.DataFrame(results)
    df_all.to_csv(csv_file, index=False, encoding='utf-8-sig')

    excel_file = os.path.join(RESULT_DIR, f"ma_stack_{STACK_DAYS}d_{today}.xlsx")
    try:
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            df_all.to_excel(writer, sheet_name=f"MA堆叠{STACK_DAYS}天", index=False)
        print(f"\n  💾 Excel结果: {excel_file}")
        print(f"     Sheet: MA堆叠{STACK_DAYS}天 ({len(results)}只)")
    except ImportError:
        print(f"\n  💾 CSV结果: {csv_file}")
        print("     (安装 openpyxl 可输出Excel: pip install openpyxl)")

    print(f"\n{'='*70}")


if __name__ == '__main__':
    main()