"""
02_data_realtime.py
全市场信号扫描 - 从已下载的历史数据中选股
策略信号:
  1. MA5 上穿 MA20（金叉）
  2. 放量上涨（今日成交量 > 20日均量 * 1.5）
  3. 突破前20日高点
  4. RSI 不超买（RSI14 < 70）
  5. 综合评分排序
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

# ============================================================
#  配置
# ============================================================
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'history')

# 策略参数
MA_SHORT = 5
MA_LONG = 20
VOL_RATIO_THRESH = 1.5
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
LOOKBACK_HIGH = 20

# 评分权重
WEIGHT_GOLDEN_CROSS = 30
WEIGHT_VOLUME = 25
WEIGHT_BREAKOUT = 25
WEIGHT_RSI = 20

# 输出数量
TOP_N = 20

# 主板代码前缀
MAIN_BOARD_PREFIX = ('600', '601', '603', '605',
                     '000', '001', '002', '003')


# ============================================================
#  技术指标计算
# ============================================================
def calc_ma(series, period):
    return series.rolling(window=period).mean()


def calc_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calc_vol_ratio(volume, period=20):
    avg_vol = volume.rolling(window=period).mean()
    return volume / avg_vol


# ============================================================
#  分析单只股票
# ============================================================
def analyze_stock(code, filepath):
    try:
        df = pd.read_csv(filepath)
    except Exception:
        return None

    if len(df) < MA_LONG + 5:
        return None

    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['close', 'volume'])

    if df.iloc[-1]['volume'] == 0:
        return None

    if len(df) < MA_LONG + 5:
        return None

    # 计算指标
    df['ma_short'] = calc_ma(df['close'], MA_SHORT)
    df['ma_long'] = calc_ma(df['close'], MA_LONG)
    df['rsi'] = calc_rsi(df['close'], RSI_PERIOD)
    df['vol_ratio'] = calc_vol_ratio(df['volume'], 20)
    df['high_max'] = df['high'].rolling(window=LOOKBACK_HIGH).max().shift(1)

    last = df.iloc[-1]
    prev = df.iloc[-2]

    # ---- 信号判断 ----
    score = 0
    signals = []

    # ---- 单独记录每种信号的触发状态 ----
    is_golden_cross = False
    is_volume_up = False
    is_breakout = False         # 收盘价突破
    is_breakout_intraday = False  # 盘中触及
    breakout_pct = 0.0          # 突破幅度

    # 1. 金叉
    if (pd.notna(last['ma_short']) and pd.notna(last['ma_long']) and
        pd.notna(prev['ma_short']) and pd.notna(prev['ma_long'])):

        golden_cross = (last['ma_short'] > last['ma_long']) and (prev['ma_short'] <= prev['ma_long'])
        ma_above = last['ma_short'] > last['ma_long']
        ma_diff_pct = (last['ma_short'] - last['ma_long']) / last['ma_long'] * 100

        if golden_cross:
            score += WEIGHT_GOLDEN_CROSS
            signals.append('★金叉(今日)')
            is_golden_cross = True
        elif ma_above and ma_diff_pct < 2.0:
            score += WEIGHT_GOLDEN_CROSS * 0.6
            signals.append(f'金叉(近期,差{ma_diff_pct:.1f}%)')
            is_golden_cross = True

    # 2. 放量上涨
    if pd.notna(last['vol_ratio']):
        if last['vol_ratio'] > VOL_RATIO_THRESH and last['close'] > last['open']:
            score += WEIGHT_VOLUME
            signals.append(f'放量↑({last["vol_ratio"]:.1f}倍)')
            is_volume_up = True
        elif last['vol_ratio'] > 1.2 and last['close'] > last['open']:
            score += WEIGHT_VOLUME * 0.5
            signals.append(f'温和放量({last["vol_ratio"]:.1f}倍)')
            is_volume_up = True

    # 3. 突破前期高点
    if pd.notna(last['high_max']) and last['high_max'] > 0:
        if last['close'] > last['high_max']:
            score += WEIGHT_BREAKOUT
            signals.append(f'突破{LOOKBACK_HIGH}日高点')
            is_breakout = True
            breakout_pct = (last['close'] - last['high_max']) / last['high_max'] * 100
        elif last['high'] > last['high_max']:
            score += WEIGHT_BREAKOUT * 0.5
            signals.append(f'盘中触及{LOOKBACK_HIGH}日高点')
            is_breakout_intraday = True
            breakout_pct = (last['high'] - last['high_max']) / last['high_max'] * 100

    # 4. RSI
    rsi_val = 0
    if pd.notna(last['rsi']):
        rsi_val = last['rsi']
        if RSI_OVERSOLD < last['rsi'] < RSI_OVERBOUGHT:
            score += WEIGHT_RSI
            signals.append(f'RSI={last["rsi"]:.1f}(适中)')
        elif last['rsi'] <= RSI_OVERSOLD:
            score += WEIGHT_RSI * 0.8
            signals.append(f'RSI={last["rsi"]:.1f}(超卖)')
        else:
            signals.append(f'⚠RSI={last["rsi"]:.1f}(超买)')
            score -= 10

    # 5. 额外加分
    if pd.notna(last['ma_long']) and last['close'] > last['ma_long']:
        score += 5

    if score < 30:
        return None

    # 涨跌幅
    pct_change = 0
    if 'pctChg' in df.columns:
        pct_change = pd.to_numeric(last.get('pctChg', 0), errors='coerce')
        if pd.isna(pct_change):
            pct_change = 0
    else:
        if len(df) >= 2 and prev['close'] > 0:
            pct_change = (last['close'] - prev['close']) / prev['close'] * 100

    return {
        'code': code,
        'close': last['close'],
        'pct_change': round(pct_change, 2),
        'vol_ratio': round(last.get('vol_ratio', 0), 2) if pd.notna(last.get('vol_ratio', 0)) else 0,
        'rsi': round(rsi_val, 1),
        'score': score,
        'signals': ' | '.join(signals),
        'date': last.get('date', ''),
        # ---- 新增：分类标记 ----
        'is_golden_cross': is_golden_cross,
        'is_volume_up': is_volume_up,
        'is_breakout': is_breakout,
        'is_breakout_intraday': is_breakout_intraday,
        'breakout_pct': round(breakout_pct, 2),
        'high_max_20': round(last['high_max'], 2) if pd.notna(last.get('high_max')) else 0,
    }


# ============================================================
#  打印表格的通用函数
# ============================================================
def print_table(title, data_list, columns, col_formats, max_rows=None):
    """
    通用表格打印
    title:       表格标题
    data_list:   字典列表
    columns:     [(显示名, 字段名), ...]
    col_formats: [格式化函数, ...]
    max_rows:    最多显示几行（None=全部）
    """
    if not data_list:
        print(f"\n  {title}")
        print("  " + "-" * 60)
        print("  （无）")
        return

    show_list = data_list[:max_rows] if max_rows else data_list

    # 表头
    header_names = [c[0] for c in columns]
    print(f"\n  {'='*70}")
    print(f"  {title}  （共 {len(data_list)} 只" +
          (f"，显示前 {max_rows}）" if max_rows and len(data_list) > max_rows else "）"))
    print(f"  {'='*70}")

    # 打印表头
    header_line = "  "
    for i, name in enumerate(header_names):
        header_line += f"{name:<{col_formats[i][1]}}"
    print(header_line)
    print("  " + "-" * 68)

    # 打印每行
    for rank, row in enumerate(show_list, 1):
        line = "  "
        for i, (_, field) in enumerate(columns):
            if field == '_rank_':
                val = str(rank)
            else:
                val = row.get(field, '')
            # 应用格式化
            formatter = col_formats[i][0]
            line += f"{formatter(val):<{col_formats[i][1]}}"
        print(line)

    print("  " + "-" * 68)


# ============================================================
#  主函数
# ============================================================
def main():
    today = datetime.now().strftime('%Y-%m-%d')

    print("=" * 70)
    print(f"  全市场量化选股信号扫描")
    print(f"  扫描日期: {today}")
    print(f"  策略: MA{MA_SHORT}/{MA_LONG}金叉 + 放量 + 突破 + RSI")
    print(f"  范围: 仅主板（沪A 60xxxx + 深A 00xxxx）")
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
    print(f"  开始扫描...\n")

    results = []
    errors = 0

    for i, (code, filepath) in enumerate(files):
        if (i + 1) % 500 == 0 or (i + 1) == total:
            print(f"  进度: [{i+1}/{total}]  已发现信号: {len(results)} 只")

        try:
            result = analyze_stock(code, filepath)
            if result:
                results.append(result)
        except Exception:
            errors += 1

    # ============================================================
    #  按得分排序
    # ============================================================
    results.sort(key=lambda x: x['score'], reverse=True)

    if not results:
        print("\n" + "=" * 70)
        print("  今日未发现符合条件的股票")
        print("  可能原因: 市场整体偏弱，无股票同时满足多个条件")
        print("=" * 70)
        return

    # ============================================================
    #  表1: 综合评分 TOP N
    # ============================================================
    fmt_str   = lambda v: str(v)
    fmt_price = lambda v: f"{v:.2f}" if isinstance(v, (int, float)) else str(v)
    fmt_pct   = lambda v: f"{v:+.2f}%" if isinstance(v, (int, float)) else str(v)
    fmt_score = lambda v: f"{v:.0f}" if isinstance(v, (int, float)) else str(v)

    print(f"\n\n{'#'*70}")
    print(f"#  表1: 综合评分 TOP {min(TOP_N, len(results))}")
    print(f"#  （共 {len(results)} 只股票触发信号）")
    print(f"{'#'*70}")
    print()
    print(f"  {'排名':<5}{'代码':<11}{'现价':<10}{'涨幅':<10}{'评分':<7}{'信号'}")
    print("  " + "-" * 68)

    for rank, r in enumerate(results[:TOP_N], 1):
        pct_str = f"{r['pct_change']:+.2f}%" if r['pct_change'] else "  N/A"
        print(f"  {rank:<5}{r['code']:<11}{r['close']:<10.2f}{pct_str:<10}{r['score']:<7.0f}{r['signals']}")

    print("  " + "-" * 68)

    # ============================================================
    #  表2: ★ 突破信号专题 ★（单独表格）
    # ============================================================
    # 筛选所有有突破信号的（收盘突破 + 盘中触及）
    breakout_list = [r for r in results if r['is_breakout'] or r['is_breakout_intraday']]
    # 细分：收盘价确认突破的
    breakout_confirmed = [r for r in results if r['is_breakout']]
    # 细分：仅盘中触及的
    breakout_intraday = [r for r in results if r['is_breakout_intraday'] and not r['is_breakout']]

    # 突破列表按突破幅度排序
    breakout_confirmed.sort(key=lambda x: x['breakout_pct'], reverse=True)
    breakout_intraday.sort(key=lambda x: x['breakout_pct'], reverse=True)

    print(f"\n\n{'#'*70}")
    print(f"#  表2: ★ 突破{LOOKBACK_HIGH}日新高 专题 ★")
    print(f"#  收盘确认突破: {len(breakout_confirmed)} 只 | 盘中触及: {len(breakout_intraday)} 只")
    print(f"{'#'*70}")

    # ---- 表2A: 收盘价确认突破 ----
    print(f"\n  ┌─ 2A. 收盘价站上{LOOKBACK_HIGH}日高点 ({len(breakout_confirmed)}只) ──────────────┐")
    print()

    if breakout_confirmed:
        print(f"  {'排名':<5}{'代码':<11}{'现价':<10}{'涨幅':<10}"
              f"{'前高':<10}{'突破%':<9}{'量比':<8}{'RSI':<8}{'评分':<6}")
        print("  " + "-" * 75)

        for rank, r in enumerate(breakout_confirmed[:TOP_N], 1):
            pct_str = f"{r['pct_change']:+.2f}%" if r['pct_change'] else "N/A"
            bp_str = f"+{r['breakout_pct']:.2f}%"
            print(f"  {rank:<5}{r['code']:<11}{r['close']:<10.2f}{pct_str:<10}"
                  f"{r['high_max_20']:<10.2f}{bp_str:<9}{r['vol_ratio']:<8.1f}"
                  f"{r['rsi']:<8.1f}{r['score']:<6.0f}")

        print("  " + "-" * 75)
    else:
        print("  （今日无收盘价确认突破的股票）")

    # ---- 表2B: 盘中触及但未站稳 ----
    print(f"\n  ┌─ 2B. 盘中触及{LOOKBACK_HIGH}日高点（未站稳）({len(breakout_intraday)}只) ─────┐")
    print()

    if breakout_intraday:
        print(f"  {'排名':<5}{'代码':<11}{'现价':<10}{'涨幅':<10}"
              f"{'前高':<10}{'触及%':<9}{'量比':<8}{'RSI':<8}{'评分':<6}")
        print("  " + "-" * 75)

        for rank, r in enumerate(breakout_intraday[:TOP_N], 1):
            pct_str = f"{r['pct_change']:+.2f}%" if r['pct_change'] else "N/A"
            bp_str = f"+{r['breakout_pct']:.2f}%"
            print(f"  {rank:<5}{r['code']:<11}{r['close']:<10.2f}{pct_str:<10}"
                  f"{r['high_max_20']:<10.2f}{bp_str:<9}{r['vol_ratio']:<8.1f}"
                  f"{r['rsi']:<8.1f}{r['score']:<6.0f}")

        print("  " + "-" * 75)
    else:
        print("  （今日无盘中触及的股票）")

    # ============================================================
    #  表3: 金叉 + 放量 同时触发
    # ============================================================
    combo_list = [r for r in results if r['is_golden_cross'] and r['is_volume_up']]
    combo_list.sort(key=lambda x: x['score'], reverse=True)

    print(f"\n\n{'#'*70}")
    print(f"#  表3: 金叉 + 放量 同时触发 ({len(combo_list)}只)")
    print(f"{'#'*70}")
    print()

    if combo_list:
        print(f"  {'排名':<5}{'代码':<11}{'现价':<10}{'涨幅':<10}"
              f"{'量比':<8}{'RSI':<8}{'评分':<7}{'信号'}")
        print("  " + "-" * 75)

        for rank, r in enumerate(combo_list[:TOP_N], 1):
            pct_str = f"{r['pct_change']:+.2f}%" if r['pct_change'] else "N/A"
            print(f"  {rank:<5}{r['code']:<11}{r['close']:<10.2f}{pct_str:<10}"
                  f"{r['vol_ratio']:<8.1f}{r['rsi']:<8.1f}{r['score']:<7.0f}{r['signals']}")

        print("  " + "-" * 75)
    else:
        print("  （今日无金叉+放量同时触发的股票）")

    # ============================================================
    #  汇总统计
    # ============================================================
    golden_count = sum(1 for r in results if r['is_golden_cross'])
    volume_count = sum(1 for r in results if r['is_volume_up'])
    breakout_count = len(breakout_confirmed)
    intraday_count = len(breakout_intraday)

    print(f"\n\n{'='*70}")
    print(f"  📊 扫描统计汇总")
    print(f"{'='*70}")
    print(f"     扫描主板股票:     {total} 只")
    print(f"     有效信号:         {len(results)} 只")
    print(f"     错误/跳过:        {errors} 只")
    print(f"{'─'*40}")
    print(f"     金叉信号:         {golden_count} 只")
    print(f"     放量信号:         {volume_count} 只")
    print(f"     收盘突破新高:     {breakout_count} 只  ← 强势")
    print(f"     盘中触及新高:     {intraday_count} 只  ← 关注")
    print(f"     金叉+放量组合:    {len(combo_list)} 只")
    print(f"{'='*70}")

    # ============================================================
    #  保存结果（多个sheet或多个文件）
    # ============================================================
    result_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'results')
    os.makedirs(result_dir, exist_ok=True)

    # 总表
    result_file = os.path.join(result_dir, f'signal_{today}.csv')
    df_all = pd.DataFrame(results)
    df_all.to_csv(result_file, index=False, encoding='utf-8-sig')

    # 突破专题表
    breakout_file = os.path.join(result_dir, f'breakout_{today}.csv')
    df_breakout = pd.DataFrame(breakout_confirmed + breakout_intraday)
    if not df_breakout.empty:
        df_breakout['突破类型'] = df_breakout['is_breakout'].apply(
            lambda x: '收盘确认' if x else '盘中触及')
        df_breakout.to_csv(breakout_file, index=False, encoding='utf-8-sig')

    # 尝试保存Excel（多sheet）
    excel_file = os.path.join(result_dir, f'signal_{today}.xlsx')
    try:
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            df_all.to_excel(writer, sheet_name='综合评分', index=False)
            if not df_breakout.empty:
                df_breakout.to_excel(writer, sheet_name='突破新高', index=False)
            if combo_list:
                pd.DataFrame(combo_list).to_excel(writer, sheet_name='金叉放量', index=False)
        print(f"\n  💾 Excel结果: {excel_file}")
        print(f"     ├── Sheet: 综合评分 ({len(results)}只)")
        print(f"     ├── Sheet: 突破新高 ({len(breakout_confirmed)+len(breakout_intraday)}只)")
        print(f"     └── Sheet: 金叉放量 ({len(combo_list)}只)")
    except ImportError:
        print(f"\n  💾 CSV结果:")
        print(f"     总表:   {result_file}")
        print(f"     突破表: {breakout_file}")
        print("     (安装 openpyxl 可输出Excel: pip install openpyxl)")

    print(f"\n{'='*70}")


if __name__ == '__main__':
    main()