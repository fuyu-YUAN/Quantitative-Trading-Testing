"""
02_deep_v_rebound.py
全市场信号扫描 - 涨停洗盘深V反弹策略

策略信号:
  1. 近N天有≥2次涨停（涨幅≥9.8%）
  2. 涨停后出现深度回调（从高点回撤≥12%）
  3. 当前正在V型反弹（从低点反弹≥5% 或 当日大阳≥5%）
  4. 下跌缩量（洗盘特征，非出货）
  5. 当前价格仍低于最高点（有上涨空间）

排序:
  反弹力度(降序) → 涨停次数(降序)

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

# ---- 涨停参数 ----
LOOKBACK_DAYS = 20              # 回看天数
LIMIT_UP_MIN_COUNT = 2          # 最少涨停次数
LIMIT_UP_THRESHOLD = 9.8        # 涨幅≥9.8%视为涨停

# ---- 深V参数 ----
DRAWDOWN_MIN_PCT = 12.0         # 从高点回撤至少12%才算"深砸"
DRAWDOWN_MAX_PCT = 35.0         # 回撤超过35%可能是基本面问题，排除

# ---- 反弹确认参数 ----
REBOUND_MIN_PCT = 5.0           # 从最低点反弹至少5%
TODAY_BIG_YANG_PCT = 5.0        # 或者今天涨幅≥5%（大阳线确认）

# ---- 缩量判断 ----
VOLUME_SHRINK_RATIO = 0.70      # 低点附近成交量 < 涨停日量的70%

# ---- 空间判断 ----
BELOW_HIGH_PCT = 95.0           # 当前价 < 最高价 * 95%（还有空间）

# ---- 输出 ----
TOP_N = 50

# 主板代码前缀
MAIN_BOARD_PREFIX = ('600', '601', '603', '605',
                     '000', '001', '002', '003')


# ============================================================
#  工具函数
# ============================================================
def get_pct_change(close_series):
    """计算每日涨跌幅(%)"""
    return close_series.pct_change() * 100


def find_limit_up_days(df, lookback, threshold):
    """
    找出最近lookback天内的涨停日

    返回: [(index位置, 日期, 涨幅, 成交量), ...]
    """
    if len(df) < lookback + 1:
        recent = df.copy()
    else:
        recent = df.iloc[-(lookback + 1):].copy()

    recent = recent.copy()
    recent['_pct'] = get_pct_change(recent['close'])

    # 只看最后 lookback 天
    tail = recent.iloc[-lookback:]

    results = []
    for idx, row in tail.iterrows():
        pct = row.get('_pct', 0)
        if pd.notna(pct) and pct >= threshold:
            vol = row.get('volume', 0) if pd.notna(row.get('volume', 0)) else 0
            date_val = row.get('date', '')
            results.append({
                'idx': idx,
                'date': date_val,
                'pct': float(pct),
                'volume': float(vol),
                'close': float(row['close']),
                'high': float(row.get('high', row['close']))
            })

    return results


# ============================================================
#  核心分析函数
# ============================================================
def analyze_stock(code, filepath):
    """分析单只股票是否符合深V反弹策略"""
    try:
        df = pd.read_csv(filepath)
    except Exception:
        return None

    if df is None or df.empty:
        return None

    # 类型转换
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['close'])

    if len(df) < LOOKBACK_DAYS + 5:
        return None

    # ================================================================
    #  条件①: 近N天有≥2次涨停
    # ================================================================
    limit_ups = find_limit_up_days(df, LOOKBACK_DAYS, LIMIT_UP_THRESHOLD)

    if len(limit_ups) < LIMIT_UP_MIN_COUNT:
        return None

    # ================================================================
    #  定位关键价格点
    # ================================================================
    # 取最近 lookback 天的数据
    if len(df) < LOOKBACK_DAYS:
        period_df = df.copy()
    else:
        period_df = df.iloc[-LOOKBACK_DAYS:].copy()

    # 最后一次涨停的位置
    last_limit_up = limit_ups[-1]
    last_lu_idx = last_limit_up['idx']

    # 最后一次涨停之后（含当天）到最新的数据
    # 先找到 last_lu_idx 在 period_df 中的位置
    if last_lu_idx not in period_df.index:
        # 涨停在更早的位置，用全部period_df
        after_lu = period_df
    else:
        after_lu_pos = period_df.index.get_loc(last_lu_idx)
        after_lu = period_df.iloc[after_lu_pos:]

    if len(after_lu) < 2:
        return None

    # 关键价格
    high_after_lu = after_lu['high'].max()       # 涨停后区间最高价
    high_idx = after_lu['high'].idxmax()         # 最高价位置

    # 最高价之后的最低价（深V的底）
    high_pos = after_lu.index.get_loc(high_idx)
    after_high = after_lu.iloc[high_pos:]

    if len(after_high) < 1:
        return None

    low_after_high = after_high['low'].min()     # 深V最低价
    low_idx = after_high['low'].idxmin()         # 最低价位置

    # ================================================================
    #  条件②: 深度回调 (从高点到低点回撤≥12%)
    # ================================================================
    if high_after_lu <= 0:
        return None

    drawdown_pct = (high_after_lu - low_after_high) / high_after_lu * 100

    if drawdown_pct < DRAWDOWN_MIN_PCT:
        return None

    if drawdown_pct > DRAWDOWN_MAX_PCT:
        return None

    # ================================================================
    #  条件③: 当前正在V型反弹
    # ================================================================
    last_row = df.iloc[-1]
    current_close = float(last_row['close'])
    prev_close = float(df.iloc[-2]['close']) if len(df) >= 2 else current_close

    # 从最低点的反弹幅度
    if low_after_high <= 0:
        return None

    rebound_from_low = (current_close - low_after_high) / low_after_high * 100

    # 今日涨幅
    today_pct = (current_close - prev_close) / prev_close * 100 if prev_close > 0 else 0

    # 条件: 反弹≥5% 或 今日大阳≥5%
    rebound_ok = rebound_from_low >= REBOUND_MIN_PCT
    big_yang_ok = today_pct >= TODAY_BIG_YANG_PCT

    if not (rebound_ok or big_yang_ok):
        return None

    # 确保低点在高点之后（V型结构）
    if low_idx <= high_idx:
        # 如果最低点就是最高点（同一天），不算V型
        if low_idx == high_idx:
            return None

    # 确保当前价 > 最低价（已经在反弹）
    if current_close <= low_after_high:
        return None

    # ================================================================
    #  条件④: 下跌缩量（洗盘特征）
    # ================================================================
    if 'volume' in df.columns:
        # 涨停日平均成交量
        lu_volumes = [lu['volume'] for lu in limit_ups if lu['volume'] > 0]
        avg_lu_volume = np.mean(lu_volumes) if lu_volumes else 0

        # 最低点附近（前后各1天）的成交量
        low_pos_in_df = df.index.get_loc(low_idx) if low_idx in df.index else None

        if low_pos_in_df is not None and avg_lu_volume > 0:
            start_pos = max(0, low_pos_in_df - 1)
            end_pos = min(len(df), low_pos_in_df + 2)
            low_area_vol = df.iloc[start_pos:end_pos]['volume'].mean()

            if pd.notna(low_area_vol) and low_area_vol > 0:
                vol_ratio = low_area_vol / avg_lu_volume
                if vol_ratio > VOLUME_SHRINK_RATIO:
                    return None  # 没有缩量，可能是放量出货
            else:
                vol_ratio = np.nan
        else:
            vol_ratio = np.nan
    else:
        vol_ratio = np.nan

    # ================================================================
    #  条件⑤: 当前价格仍低于最高点（有上涨空间）
    # ================================================================
    if current_close >= high_after_lu * (BELOW_HIGH_PCT / 100):
        return None  # 已经涨回去了，空间不大

    # ================================================================
    #  计算辅助指标
    # ================================================================
    # 距离最高点还有多少空间
    upside_pct = (high_after_lu - current_close) / current_close * 100

    # 均线
    ma5 = df['close'].rolling(5).mean().iloc[-1] if len(df) >= 5 else np.nan
    ma10 = df['close'].rolling(10).mean().iloc[-1] if len(df) >= 10 else np.nan
    ma20 = df['close'].rolling(20).mean().iloc[-1] if len(df) >= 20 else np.nan

    # V型深度得分 = 回撤深度 × 反弹力度（越深越弹得高）
    v_score = drawdown_pct * rebound_from_low / 100

    # 涨停日期字符串
    lu_dates_str = ', '.join(str(lu['date']) for lu in limit_ups)

    # 最低点日期
    low_date = df.loc[low_idx, 'date'] if low_idx in df.index and 'date' in df.columns else ''
    high_date = df.loc[high_idx, 'date'] if high_idx in df.index and 'date' in df.columns else ''

    return {
        'code': code,
        'date': last_row.get('date', ''),
        'close': round(current_close, 2),
        'today_pct': round(today_pct, 2),
        'limit_up_count': len(limit_ups),
        'high_price': round(high_after_lu, 2),
        'high_date': high_date,
        'low_price': round(low_after_high, 2),
        'low_date': low_date,
        'drawdown_pct': round(drawdown_pct, 2),
        'rebound_pct': round(rebound_from_low, 2),
        'upside_pct': round(upside_pct, 2),
        'vol_shrink_ratio': round(float(vol_ratio), 2) if pd.notna(vol_ratio) else 'N/A',
        'v_score': round(v_score, 2),
        'ma5': round(float(ma5), 2) if pd.notna(ma5) else np.nan,
        'ma10': round(float(ma10), 2) if pd.notna(ma10) else np.nan,
        'ma20': round(float(ma20), 2) if pd.notna(ma20) else np.nan,
        'limit_up_dates': lu_dates_str,
        'signal': f"涨停{len(limit_ups)}次→回撤{drawdown_pct:.1f}%→反弹{rebound_from_low:.1f}%"
    }


# ============================================================
#  主函数
# ============================================================
def main():
    today = datetime.now().strftime('%Y-%m-%d')

    print("=" * 90)
    print("  📊 全市场量化选股 - 涨停洗盘深V反弹策略")
    print(f"  扫描日期: {today}")
    print(f"  策略条件:")
    print(f"    ① 近 {LOOKBACK_DAYS} 天有 ≥{LIMIT_UP_MIN_COUNT} 次涨停（涨幅≥{LIMIT_UP_THRESHOLD}%）")
    print(f"    ② 涨停后深度回调 ≥{DRAWDOWN_MIN_PCT}%（最多{DRAWDOWN_MAX_PCT}%）")
    print(f"    ③ 从低点反弹 ≥{REBOUND_MIN_PCT}% 或 当日大阳 ≥{TODAY_BIG_YANG_PCT}%")
    print(f"    ④ 下跌缩量（低点量 < 涨停量的{int(VOLUME_SHRINK_RATIO*100)}%）")
    print(f"    ⑤ 当前价 < 最高价的{BELOW_HIGH_PCT}%（仍有上涨空间）")
    print(f"  排序: V型得分(降序) → 反弹力度(降序)")
    print(f"  范围: 仅主板（沪A 60xxxx + 深A 00xxxx）")
    print("=" * 90)

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
    # 统计各阶段通过数
    stats = {
        'total': total,
        'has_limit_up': 0,
        'has_drawdown': 0,
        'has_rebound': 0,
        'has_vol_shrink': 0,
        'has_space': 0,
        'final': 0
    }
    errors = 0

    for i, (code, filepath) in enumerate(files):
        if (i + 1) % 500 == 0 or (i + 1) == total:
            print(f"  进度: [{i+1}/{total}]  "
                  f"涨停≥{LIMIT_UP_MIN_COUNT}: {stats['has_limit_up']}  "
                  f"深V: {stats['has_drawdown']}  "
                  f"反弹: {stats['has_rebound']}  "
                  f"命中: {len(results)}")

        try:
            # ---- 快速预筛（减少完整分析次数）----
            try:
                df_tmp = pd.read_csv(filepath)
                for col in ['close', 'volume']:
                    if col in df_tmp.columns:
                        df_tmp[col] = pd.to_numeric(df_tmp[col], errors='coerce')
                df_tmp = df_tmp.dropna(subset=['close'])

                if len(df_tmp) < LOOKBACK_DAYS + 5:
                    continue

                # 快速检查涨停
                lu_check = find_limit_up_days(df_tmp, LOOKBACK_DAYS, LIMIT_UP_THRESHOLD)
                if len(lu_check) >= LIMIT_UP_MIN_COUNT:
                    stats['has_limit_up'] += 1
                else:
                    continue
            except Exception:
                continue

            r = analyze_stock(code, filepath)
            if r:
                results.append(r)
        except Exception:
            errors += 1

    stats['final'] = len(results)

    # 排序: V型得分降序 → 反弹力度降序
    results.sort(key=lambda x: (-x.get('v_score', 0), -x.get('rebound_pct', 0)))

    print(f"\n  📊 筛选漏斗统计:")
    print(f"     全部主板股票:          {total} 只")
    print(f"     ① 有≥{LIMIT_UP_MIN_COUNT}次涨停:          {stats['has_limit_up']} 只")
    print(f"     ⑤ 最终命中:            {stats['final']} 只 ← 最终结果")

    if not results:
        print("\n" + "=" * 90)
        print("  今日未发现符合条件的股票")
        print(f"  提示: 可尝试调整参数:")
        print(f"    - DRAWDOWN_MIN_PCT 降为 10")
        print(f"    - REBOUND_MIN_PCT 降为 3")
        print(f"    - VOLUME_SHRINK_RATIO 升为 0.8")
        print(f"    - LOOKBACK_DAYS 升为 40")
        print("=" * 90)
        return

    # ============================================================
    #  打印结果
    # ============================================================
    show_n = min(TOP_N, len(results))
    print(f"\n\n{'#'*90}")
    print(f"#  涨停洗盘深V反弹 - TOP {show_n}")
    print(f"#  命中: {len(results)} 只 | 扫描: {total} 只")
    print(f"{'#'*90}\n")

    header = (f"  {'排名':<4}{'代码':<9}{'现价':<7}{'今涨':<8}"
              f"{'涨停':<5}{'高点':<7}{'低点':<7}{'回撤%':<7}"
              f"{'反弹%':<7}{'上方空间':<8}{'缩量比':<7}{'V分':<6}"
              f"{'涨停日期'}")
    print(header)
    print("  " + "-" * 140)

    for rank, r in enumerate(results[:show_n], 1):
        today_str = f"{r['today_pct']:+.1f}%"
        dd_str = f"{r['drawdown_pct']:.1f}%"
        rb_str = f"{r['rebound_pct']:.1f}%"
        up_str = f"{r['upside_pct']:.1f}%"
        vol_str = str(r['vol_shrink_ratio'])
        dates_str = r.get('limit_up_dates', '')
        if len(dates_str) > 30:
            dates_str = dates_str[:27] + "..."

        print(f"  {rank:<4}{r['code']:<9}{r['close']:<7.2f}{today_str:<8}"
              f"{r['limit_up_count']:<5}{r['high_price']:<7.2f}{r['low_price']:<7.2f}"
              f"{dd_str:<7}{rb_str:<7}{up_str:<8}{vol_str:<7}"
              f"{r['v_score']:<6.1f}{dates_str}")

    print("  " + "-" * 140)

    # ============================================================
    #  信号说明
    # ============================================================
    print(f"\n  📌 信号说明:")
    print(f"     回撤%  = 从涨停后高点到最低点的跌幅")
    print(f"     反弹%  = 从最低点到当前价的涨幅")
    print(f"     上方空间 = 距离前高还能涨多少")
    print(f"     缩量比 = 低点成交量/涨停成交量（越小=洗盘越充分）")
    print(f"     V分   = 回撤深度×反弹力度（越大=V型越强烈）")
    print(f"     ⚠️ 买入参考: V分高 + 缩量明显 + 上方空间大")

    # ============================================================
    #  保存结果
    # ============================================================
    os.makedirs(RESULT_DIR, exist_ok=True)

    csv_file = os.path.join(RESULT_DIR, f"deep_v_rebound_{today}.csv")
    df_all = pd.DataFrame(results)
    df_all.to_csv(csv_file, index=False, encoding='utf-8-sig')

    excel_file = os.path.join(RESULT_DIR, f"deep_v_rebound_{today}.xlsx")
    try:
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Sheet1: 完整结果
            df_all.to_excel(writer, sheet_name="深V反弹信号", index=False)

            # Sheet2: 精简看板
            df_simple = df_all[['code', 'date', 'close', 'today_pct',
                                'limit_up_count', 'drawdown_pct', 'rebound_pct',
                                'upside_pct', 'v_score', 'signal']].copy()
            df_simple.to_excel(writer, sheet_name="精简看板", index=False)

        print(f"\n  💾 Excel结果: {excel_file}")
        print(f"     Sheet1: 深V反弹信号 ({len(results)}只)")
        print(f"     Sheet2: 精简看板")
    except ImportError:
        print(f"\n  💾 CSV结果: {csv_file}")
        print("     (安装 openpyxl 可输出Excel: pip install openpyxl)")

    print(f"\n{'='*90}")


if __name__ == '__main__':
    main()