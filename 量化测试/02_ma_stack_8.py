"""
02_ma_stack_14.py
全市场信号扫描 - 从已下载的历史数据中选股（专用版）

策略信号:
  1. MA(5,10,20) 多头排列 连续 N 天
  2. 最近30天内涨停次数 ≥ 3 且 < 5 次
  3. 不能有连续涨停（相邻两天都涨停则排除）

排序:
  按涨停次数(降序) → 最近涨幅(降序)

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
STACK_DAYS = 8

# ---- 涨停筛选参数 ----
LIMIT_UP_LOOKBACK = 40          # 回看天数
LIMIT_UP_MIN_COUNT = 2          # 最少涨停次数 (>=3)
LIMIT_UP_MAX_COUNT = 5          # 最多涨停次数 (<5，即3次或4次)
LIMIT_UP_THRESHOLD = 9.8        # 涨幅>=9.8%视为涨停
NO_CONSECUTIVE_LIMIT_UP = True  # True=排除连续涨停

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


def count_limit_up(df, lookback=30, threshold=9.8):
    """
    统计最近lookback个交易日内涨停次数

    返回: (涨停次数, 涨停日期列表, 涨停位置的布尔Series)
    """
    if len(df) < 2:
        return 0, [], pd.Series(dtype=bool)

    if len(df) < lookback + 1:
        tail = df.copy()
    else:
        tail = df.iloc[-(lookback + 1):].copy()

    # 计算每日涨幅
    if 'pctChg' in tail.columns:
        tail = tail.copy()
        tail['_pct'] = pd.to_numeric(tail['pctChg'], errors='coerce')
    else:
        tail = tail.copy()
        tail['_pct'] = tail['close'].pct_change() * 100

    # 只看最后 lookback 天
    recent = tail.iloc[-lookback:].copy()

    # 判定涨停
    limit_up_mask = recent['_pct'] >= threshold
    count = int(limit_up_mask.sum())

    dates = []
    if count > 0 and 'date' in recent.columns:
        dates = recent.loc[limit_up_mask, 'date'].tolist()

    return count, dates, limit_up_mask


def has_consecutive_limit_up(limit_up_mask):
    """
    检查是否存在连续涨停（相邻两天都是涨停）

    参数:
        limit_up_mask: 布尔Series，True表示当天涨停

    返回:
        True = 存在连续涨停, False = 不存在
    """
    if limit_up_mask.sum() < 2:
        return False

    # 将布尔值转为整数，相邻两天都为1则存在连续涨停
    arr = limit_up_mask.astype(int).values
    for i in range(1, len(arr)):
        if arr[i] == 1 and arr[i - 1] == 1:
            return True
    return False


def is_ma_bull_stack(df, periods=(5, 10, 20), days=14):
    """
    最近days个交易日是否每天满足 MA短 > MA中 > MA长
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

    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['close'])

    if len(df) < max(MA_PERIODS) + STACK_DAYS + 2:
        return None

    # ---- 条件1: MA多头排列 ----
    ok, last_ma = is_ma_bull_stack(df, MA_PERIODS, STACK_DAYS)
    if not ok:
        return None

    # ---- 条件2: 涨停次数在 [MIN, MAX) 范围内 ----
    lu_count, lu_dates, lu_mask = count_limit_up(df, LIMIT_UP_LOOKBACK, LIMIT_UP_THRESHOLD)

    if lu_count < LIMIT_UP_MIN_COUNT:
        return None

    if lu_count >= LIMIT_UP_MAX_COUNT:
        return None

    # ---- 条件3: 不能有连续涨停 ----
    if NO_CONSECUTIVE_LIMIT_UP and has_consecutive_limit_up(lu_mask):
        return None

    last = df.iloc[-1]
    pct_change = get_pct_change(df)

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
        'limit_up_count': lu_count,
        'has_consecutive': '否',
        'limit_up_dates': ', '.join(str(d) for d in lu_dates),
        'signal': f"MA多头{STACK_DAYS}天 + 涨停{lu_count}次(非连续)/{LIMIT_UP_LOOKBACK}天"
    }


# ============================================================
#  主函数
# ============================================================
def main():
    today = datetime.now().strftime('%Y-%m-%d')

    print("=" * 80)
    print("  全市场量化选股信号扫描（专用版）")
    print(f"  扫描日期: {today}")
    print(f"  策略条件:")
    print(f"    ① MA{MA_PERIODS} 多头排列连续 {STACK_DAYS} 天")
    print(f"    ② 最近 {LIMIT_UP_LOOKBACK} 天涨停 ≥{LIMIT_UP_MIN_COUNT} 且 <{LIMIT_UP_MAX_COUNT} 次（涨幅≥{LIMIT_UP_THRESHOLD}%）")
    print(f"    ③ 不允许连续涨停（间隔分布）: {'是' if NO_CONSECUTIVE_LIMIT_UP else '否'}")
    print(f"  排序: 涨停次数(降序) → 最近涨幅(降序)")
    print(f"  范围: 仅主板（沪A 60xxxx + 深A 00xxxx）")
    print("=" * 80)

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
    ma_pass_count = 0
    lu_range_pass = 0
    errors = 0

    for i, (code, filepath) in enumerate(files):
        if (i + 1) % 500 == 0 or (i + 1) == total:
            print(f"  进度: [{i+1}/{total}]  MA通过: {ma_pass_count}  "
                  f"涨停范围通过: {lu_range_pass}  最终命中: {len(results)}")

        try:
            # 读取数据做中间统计
            try:
                df_tmp = pd.read_csv(filepath)
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in df_tmp.columns:
                        df_tmp[col] = pd.to_numeric(df_tmp[col], errors='coerce')
                df_tmp = df_tmp.dropna(subset=['close'])
                ma_ok, _ = is_ma_bull_stack(df_tmp, MA_PERIODS, STACK_DAYS)
                if ma_ok:
                    ma_pass_count += 1
                    # 检查涨停次数范围（不含连续判断）
                    cnt, _, _ = count_limit_up(df_tmp, LIMIT_UP_LOOKBACK, LIMIT_UP_THRESHOLD)
                    if LIMIT_UP_MIN_COUNT <= cnt < LIMIT_UP_MAX_COUNT:
                        lu_range_pass += 1
            except Exception:
                pass

            r = analyze_stock(code, filepath)
            if r:
                results.append(r)
        except Exception:
            errors += 1

    # 排序: 先按涨停次数降序，再按涨幅降序
    results.sort(key=lambda x: (-x.get('limit_up_count', 0), -x.get('pct_change', -9999)))

    print(f"\n  📊 筛选漏斗统计:")
    print(f"     全部主板股票:          {total} 只")
    print(f"     ① MA多头排列通过:      {ma_pass_count} 只")
    print(f"     ② 涨停{LIMIT_UP_MIN_COUNT}~{LIMIT_UP_MAX_COUNT-1}次通过:       {lu_range_pass} 只")
    print(f"     ③ 排除连续涨停后:      {len(results)} 只 ← 最终结果")

    if not results:
        print("\n" + "=" * 80)
        print("  今日未发现符合条件的股票")
        print(f"  条件: MA多头{STACK_DAYS}天 + {LIMIT_UP_LOOKBACK}天涨停{LIMIT_UP_MIN_COUNT}~{LIMIT_UP_MAX_COUNT-1}次(非连续)")
        print(f"  提示: 可尝试调整参数:")
        print(f"    - LIMIT_UP_MIN_COUNT 降为 2")
        print(f"    - LIMIT_UP_MAX_COUNT 升为 6")
        print(f"    - STACK_DAYS 降为 5")
        print("=" * 80)
        return

    # ============================================================
    #  打印结果
    # ============================================================
    show_n = min(TOP_N, len(results))
    print(f"\n\n{'#'*80}")
    print(f"#  MA多头{STACK_DAYS}天 + {LIMIT_UP_LOOKBACK}天涨停{LIMIT_UP_MIN_COUNT}~{LIMIT_UP_MAX_COUNT-1}次(非连续) - TOP {show_n}")
    print(f"#  命中: {len(results)} 只 | 扫描: {total} 只")
    print(f"{'#'*80}\n")

    header = (f"  {'排名':<5}{'代码':<10}{'现价':<9}{'涨幅':<9}"
              f"{'涨停数':<8}{'连续':<6}{'MA5':<9}{'MA10':<9}{'MA20':<9}"
              f"{'乖离':<10}{'涨停日期'}")
    print(header)
    print("  " + "-" * 130)

    for rank, r in enumerate(results[:show_n], 1):
        pct_str = f"{r['pct_change']:+.2f}%"
        bias_str = f"{r['bias_5_20_pct']:+.2f}%" if pd.notna(r.get('bias_5_20_pct', np.nan)) else "N/A"
        lu_str = f"{r['limit_up_count']}次"
        consec_str = r.get('has_consecutive', '否')
        dates_str = r.get('limit_up_dates', '')
        if len(dates_str) > 45:
            dates_str = dates_str[:42] + "..."

        print(f"  {rank:<5}{r['code']:<10}{r['close']:<9.2f}{pct_str:<9}"
              f"{lu_str:<8}{consec_str:<6}{r['ma5']:<9.2f}{r['ma10']:<9.2f}{r['ma20']:<9.2f}"
              f"{bias_str:<10}{dates_str}")

    print("  " + "-" * 130)

    # ============================================================
    #  保存结果
    # ============================================================
    os.makedirs(RESULT_DIR, exist_ok=True)

    csv_file = os.path.join(RESULT_DIR, f"ma_stack_limitup_{today}.csv")
    df_all = pd.DataFrame(results)
    df_all.to_csv(csv_file, index=False, encoding='utf-8-sig')

    excel_file = os.path.join(RESULT_DIR, f"ma_stack_limitup_{today}.xlsx")
    try:
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            df_all.to_excel(writer, sheet_name=f"MA多头+涨停非连续", index=False)
        print(f"\n  💾 Excel结果: {excel_file}")
        print(f"     Sheet: MA多头+涨停非连续 ({len(results)}只)")
    except ImportError:
        print(f"\n  💾 CSV结果: {csv_file}")
        print("     (安装 openpyxl 可输出Excel: pip install openpyxl)")

    print(f"\n{'='*80}")


if __name__ == '__main__':
    main()