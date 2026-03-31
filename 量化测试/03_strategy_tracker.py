"""
03_strategy_tracker.py - 策略选股回测跟踪
功能：
  1. 读取选股结果（Excel/CSV）
  2. 跟踪后续5个交易日表现
  3. 统计命中率、收益率、红盘占比
  4. 输出详细报告 + Excel

用法：
  python 03_strategy_tracker.py
  python 03_strategy_tracker.py --file signal_20250620.xlsx --days 5
"""

import pandas as pd
import numpy as np
import baostock as bs
import os
import argparse
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')


# ==================== 配置 ====================
class Config:
    # 跟踪天数
    TRACK_DAYS = 5

    # 选股结果文件（自动查找最新的 signal_*.xlsx）
    SIGNAL_DIR = "./"
    SIGNAL_FILE = None  # None = 自动查找最新

    # 历史数据目录（用于本地读取，没有则在线获取）
    LOCAL_DATA_DIR = "./stock_data"

    # 输出目录
    OUTPUT_DIR = "./track_results"

    # 买入假设
    BUY_PRICE = "open"     # "open"=次日开盘买入, "close"=选股日收盘买入
    BUY_SLIPPAGE = 0.001   # 滑点 0.1%

    # 判定标准
    WIN_THRESHOLD = 0.0    # 收益率 > 0 算盈利（红盘）


# ==================== 工具函数 ====================
def find_latest_signal_file(directory="./"):
    """自动查找最新的选股结果文件"""
    candidates = []
    for f in os.listdir(directory):
        if f.startswith("signal_") and (f.endswith(".xlsx") or f.endswith(".csv")):
            filepath = os.path.join(directory, f)
            candidates.append((filepath, os.path.getmtime(filepath)))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0]


def parse_signal_file(filepath):
    """
    读取选股结果文件，提取股票代码和选股日期
    支持多种格式
    """
    print(f"📄 读取文件: {filepath}")

    if filepath.endswith(".xlsx"):
        df = pd.read_excel(filepath, dtype=str)
    else:
        df = pd.read_csv(filepath, dtype=str)

    print(f"   列名: {list(df.columns)}")
    print(f"   共 {len(df)} 条记录")

    # 智能识别代码列
    code_col = None
    for col in df.columns:
        col_lower = col.lower()
        if any(k in col_lower for k in ['code', '代码', 'stock', '股票代码']):
            code_col = col
            break
    if code_col is None:
        code_col = df.columns[0]  # 默认第一列

    # 智能识别日期列
    date_col = None
    for col in df.columns:
        col_lower = col.lower()
        if any(k in col_lower for k in ['date', '日期', '信号日期', '选股日期']):
            date_col = col
            break

    # 智能识别名称列
    name_col = None
    for col in df.columns:
        col_lower = col.lower()
        if any(k in col_lower for k in ['name', '名称', '股票名称']):
            name_col = col
            break

    codes = df[code_col].tolist()
    names = df[name_col].tolist() if name_col else [""] * len(codes)

    # 选股日期
    if date_col and df[date_col].iloc[0]:
        signal_date = str(df[date_col].iloc[0]).strip()[:10]
    else:
        # 从文件名提取日期 signal_20250620.xlsx
        import re
        m = re.search(r'(\d{8})', os.path.basename(filepath))
        if m:
            d = m.group(1)
            signal_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        else:
            signal_date = datetime.now().strftime("%Y-%m-%d")

    print(f"   选股日期: {signal_date}")
    print(f"   股票列表: {codes[:5]}{'...' if len(codes) > 5 else ''}")

    return codes, names, signal_date


# ==================== 获取跟踪数据 ====================
def get_track_data(code, signal_date, track_days=5):
    """
    获取选股日之后 track_days 个交易日的行情数据
    先尝试本地，没有则在线获取
    """
    # 尝试本地读取
    local_file = os.path.join(Config.LOCAL_DATA_DIR, f"{code}.csv")
    if os.path.exists(local_file):
        df = pd.read_csv(local_file, dtype={'date': str})
        df['date'] = df['date'].str.strip()
        df = df[df['date'] > signal_date].head(track_days)

        if len(df) >= track_days:
            return df

    # 在线获取（多取一些天数以确保有足够交易日）
    start = signal_date
    end_dt = datetime.strptime(signal_date, "%Y-%m-%d") + timedelta(days=track_days * 3)
    end = end_dt.strftime("%Y-%m-%d")

    try:
        lg = bs.login()
        rs = bs.query_history_k_data_plus(
            code,
            "date,code,open,high,low,close,preclose,volume,amount,pctChg",
            start_date=start,
            end_date=end,
            frequency="d",
            adjustflag="2"
        )

        rows = []
        while rs.error_code == '0' and rs.next():
            rows.append(rs.get_row_data())

        bs.logout()

        if not rows:
            return None

        df = pd.DataFrame(rows, columns=rs.fields)
        # 排除选股日当天，只要之后的
        df = df[df['date'] > signal_date].head(track_days)
        return df

    except Exception as e:
        try:
            bs.logout()
        except:
            pass
        print(f"      ❌ {code} 获取数据失败: {e}")
        return None


# ==================== 获取选股日数据（用于确定买入价）====================
def get_signal_day_data(code, signal_date):
    """获取选股日当天的数据"""
    local_file = os.path.join(Config.LOCAL_DATA_DIR, f"{code}.csv")
    if os.path.exists(local_file):
        df = pd.read_csv(local_file, dtype={'date': str})
        df['date'] = df['date'].str.strip()
        row = df[df['date'] == signal_date]
        if not row.empty:
            return row.iloc[0]

    # 在线获取
    try:
        lg = bs.login()
        rs = bs.query_history_k_data_plus(
            code,
            "date,code,open,high,low,close,volume",
            start_date=signal_date,
            end_date=signal_date,
            frequency="d",
            adjustflag="2"
        )
        rows = []
        while rs.error_code == '0' and rs.next():
            rows.append(rs.get_row_data())
        bs.logout()

        if rows:
            df = pd.DataFrame(rows, columns=rs.fields)
            return df.iloc[0]
    except:
        try:
            bs.logout()
        except:
            pass
    return None


# ==================== 单只股票跟踪分析 ====================
def track_one_stock(code, name, signal_date, track_days):
    """跟踪一只股票的后续表现"""
    result = {
        'code': code,
        'name': name,
        'signal_date': signal_date,
        'buy_price': None,
        'status': 'OK'
    }

    # 初始化每日字段
    for i in range(1, track_days + 1):
        result[f'T+{i}_date'] = ''
        result[f'T+{i}_close'] = None
        result[f'T+{i}_pctChg'] = None      # 当日涨跌幅
        result[f'T+{i}_cumReturn'] = None    # 相对买入价的累计收益

    # 获取跟踪数据
    track_df = get_track_data(code, signal_date, track_days)
    if track_df is None or track_df.empty:
        result['status'] = '无数据'
        return result

    # 确定买入价
    if Config.BUY_PRICE == "open":
        # T+1 开盘买入
        buy_price = float(track_df.iloc[0]['open'])
    else:
        # 选股日收盘买入
        signal_data = get_signal_day_data(code, signal_date)
        if signal_data is not None:
            buy_price = float(signal_data['close'])
        else:
            buy_price = float(track_df.iloc[0]['open'])

    buy_price = buy_price * (1 + Config.BUY_SLIPPAGE)  # 加滑点
    result['buy_price'] = round(buy_price, 3)

    # 逐日统计
    red_days = 0
    max_return = -999
    max_drawdown = 0

    for i, (_, row) in enumerate(track_df.iterrows()):
        day_idx = i + 1
        if day_idx > track_days:
            break

        close = float(row['close'])
        pct_chg = float(row['pctChg']) if row['pctChg'] != '' else 0
        cum_return = (close - buy_price) / buy_price * 100

        result[f'T+{day_idx}_date'] = row['date']
        result[f'T+{day_idx}_close'] = round(close, 2)
        result[f'T+{day_idx}_pctChg'] = round(pct_chg, 2)
        result[f'T+{day_idx}_cumReturn'] = round(cum_return, 2)

        if pct_chg > 0:
            red_days += 1

        # 最大收益 & 最大回撤
        if cum_return > max_return:
            max_return = cum_return
        drawdown = max_return - cum_return
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    # 汇总
    actual_days = min(len(track_df), track_days)
    last_key = f'T+{actual_days}_cumReturn'
    final_return = result.get(last_key, 0) or 0

    result['actual_days'] = actual_days
    result['red_days'] = red_days
    result['red_ratio'] = round(red_days / actual_days * 100, 1) if actual_days > 0 else 0
    result['final_return'] = round(final_return, 2)
    result['max_return'] = round(max_return, 2) if max_return > -999 else 0
    result['max_drawdown'] = round(max_drawdown, 2)
    result['is_win'] = 1 if final_return > Config.WIN_THRESHOLD else 0

    return result


# ==================== 输出报告 ====================
def print_report(results, signal_date, track_days):
    """打印详细的跟踪报告"""

    valid = [r for r in results if r['status'] == 'OK']
    failed = [r for r in results if r['status'] != 'OK']

    print(f"\n{'='*90}")
    print(f"  📊 策略跟踪报告")
    print(f"  📅 选股日期: {signal_date}")
    print(f"  📈 跟踪周期: T+1 ~ T+{track_days}")
    print(f"  💰 买入方式: T+1开盘价 + {Config.BUY_SLIPPAGE*100:.1f}%滑点")
    print(f"{'='*90}")

    if not valid:
        print("\n  ❌ 没有有效跟踪数据")
        return

    # ---- 个股明细 ----
    print(f"\n{'─'*90}")
    print(f"  📋 个股明细")
    print(f"{'─'*90}")

    # 表头
    header = f"{'股票':<14} {'买入价':>7}"
    for i in range(1, track_days + 1):
        header += f" {'T+'+str(i):>7}"
    header += f" {'累计收益':>8} {'红盘':>5} {'结果':>4}"
    print(header)
    print("─" * 90)

    # 按最终收益排序
    valid_sorted = sorted(valid, key=lambda x: x.get('final_return', 0), reverse=True)

    for r in valid_sorted:
        line = f"  {r['code'][-7:]:<7} {r.get('name','')[:4]:<5} {r['buy_price']:>7.2f}"

        for i in range(1, track_days + 1):
            cum_ret = r.get(f'T+{i}_cumReturn')
            if cum_ret is not None:
                color = "🔴" if cum_ret >= 0 else "🟢"
                line += f" {cum_ret:>+6.2f}%"
            else:
                line += f" {'--':>7}"

        final_ret = r.get('final_return', 0)
        red_ratio = r.get('red_ratio', 0)
        win_mark = "✅" if r.get('is_win') else "❌"

        line += f" {final_ret:>+7.2f}% {red_ratio:>4.0f}% {win_mark}"
        print(line)

    # ---- 汇总统计 ----
    print(f"\n{'─'*90}")
    print(f"  📊 汇总统计（共 {len(valid)} 只有效标的）")
    print(f"{'─'*90}")

    final_returns = [r['final_return'] for r in valid]
    max_returns = [r['max_return'] for r in valid]
    max_drawdowns = [r['max_drawdown'] for r in valid]
    red_ratios = [r['red_ratio'] for r in valid]
    win_count = sum(1 for r in valid if r['is_win'])

    print(f"\n  🎯 胜率（T+{track_days}盈利）: "
          f"{win_count}/{len(valid)} = {win_count/len(valid)*100:.1f}%")

    print(f"\n  💰 收益统计:")
    print(f"     平均收益率:   {np.mean(final_returns):>+7.2f}%")
    print(f"     收益中位数:   {np.median(final_returns):>+7.2f}%")
    print(f"     最大盈利:     {max(final_returns):>+7.2f}%")
    print(f"     最大亏损:     {min(final_returns):>+7.2f}%")
    print(f"     收益标准差:   {np.std(final_returns):>7.2f}%")

    print(f"\n  📈 最大收益/回撤:")
    print(f"     平均最大收益: {np.mean(max_returns):>+7.2f}%")
    print(f"     平均最大回撤: {np.mean(max_drawdowns):>7.2f}%")

    print(f"\n  🔴 红盘统计:")
    print(f"     平均红盘占比: {np.mean(red_ratios):>6.1f}%")
    total_red = sum(r['red_days'] for r in valid)
    total_days = sum(r['actual_days'] for r in valid)
    print(f"     总红盘天数:   {total_red}/{total_days} = "
          f"{total_red/total_days*100:.1f}%")

    # 每日胜率
    print(f"\n  📅 逐日统计:")
    for i in range(1, track_days + 1):
        day_returns = [r[f'T+{i}_cumReturn'] for r in valid
                       if r.get(f'T+{i}_cumReturn') is not None]
        if day_returns:
            day_win = sum(1 for x in day_returns if x > 0)
            avg_ret = np.mean(day_returns)
            print(f"     T+{i}: 胜率 {day_win}/{len(day_returns)}"
                  f" ({day_win/len(day_returns)*100:.0f}%)"
                  f"  平均累计收益 {avg_ret:>+.2f}%")

    if failed:
        print(f"\n  ⚠️  无数据: {[r['code'] for r in failed]}")

    print(f"\n{'='*90}")

    return valid_sorted


# ==================== 保存 Excel ====================
def save_excel(results, signal_date, track_days):
    """保存详细结果到Excel"""
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)

    valid = [r for r in results if r['status'] == 'OK']
    if not valid:
        return

    # 构建DataFrame
    rows = []
    for r in sorted(valid, key=lambda x: x['final_return'], reverse=True):
        row = {
            '股票代码': r['code'],
            '股票名称': r['name'],
            '选股日期': r['signal_date'],
            '买入价': r['buy_price'],
        }

        for i in range(1, track_days + 1):
            row[f'T+{i}日期'] = r.get(f'T+{i}_date', '')
            row[f'T+{i}收盘'] = r.get(f'T+{i}_close', '')
            row[f'T+{i}涨跌%'] = r.get(f'T+{i}_pctChg', '')
            row[f'T+{i}累计%'] = r.get(f'T+{i}_cumReturn', '')

        row['最终收益%'] = r['final_return']
        row['最大收益%'] = r['max_return']
        row['最大回撤%'] = r['max_drawdown']
        row['红盘天数'] = r['red_days']
        row[f'红盘占比%'] = r['red_ratio']
        row['是否盈利'] = '是' if r['is_win'] else '否'

        rows.append(row)

    df = pd.DataFrame(rows)

    # 添加汇总行
    summary = pd.DataFrame([{
        '股票代码': '【汇总】',
        '股票名称': f'共{len(valid)}只',
        '最终收益%': round(np.mean([r['final_return'] for r in valid]), 2),
        '最大收益%': round(np.mean([r['max_return'] for r in valid]), 2),
        '最大回撤%': round(np.mean([r['max_drawdown'] for r in valid]), 2),
        '红盘天数': sum(r['red_days'] for r in valid),
        f'红盘占比%': round(np.mean([r['red_ratio'] for r in valid]), 1),
        '是否盈利': f"胜率{sum(r['is_win'] for r in valid)}/{len(valid)}"
    }])

    df = pd.concat([df, summary], ignore_index=True)

    filename = f"track_{signal_date.replace('-','')}_{track_days}d.xlsx"
    filepath = os.path.join(Config.OUTPUT_DIR, filename)
    df.to_excel(filepath, index=False, engine='openpyxl')
    print(f"\n  💾 结果已保存: {filepath}")

    return filepath


# ==================== 手动输入模式 ====================
def manual_input():
    """手动输入股票代码（找不到文件时使用）"""
    print("\n📝 手动输入模式")
    print("   输入股票代码（逗号分隔），例如: sh.600519,sz.000001,sz.002415")
    codes_str = input("   股票代码: ").strip()

    if not codes_str:
        return None, None, None

    codes = [c.strip() for c in codes_str.split(",") if c.strip()]

    date_str = input(f"   选股日期（回车=最近交易日）: ").strip()
    if not date_str:
        # 取最近交易日（简化处理，取昨天）
        from datetime import date
        d = date.today() - timedelta(days=1)
        while d.weekday() >= 5:  # 跳过周末
            d -= timedelta(days=1)
        date_str = d.strftime("%Y-%m-%d")

    names = [""] * len(codes)
    return codes, names, date_str


# ==================== 主函数 ====================
def main():
    parser = argparse.ArgumentParser(description='策略选股回测跟踪')
    parser.add_argument('--file', type=str, default=None, help='选股结果文件路径')
    parser.add_argument('--days', type=int, default=5, help='跟踪天数')
    parser.add_argument('--buy', type=str, default='open', choices=['open', 'close'],
                        help='买入方式: open=次日开盘, close=当日收盘')
    args = parser.parse_args()

    Config.TRACK_DAYS = args.days
    Config.BUY_PRICE = args.buy

    print("=" * 70)
    print("  📊 策略选股 - 回测跟踪系统")
    print("=" * 70)

    # 1. 读取选股结果
    signal_file = args.file or Config.SIGNAL_FILE
    if signal_file is None:
        signal_file = find_latest_signal_file(Config.SIGNAL_DIR)

    if signal_file and os.path.exists(signal_file):
        codes, names, signal_date = parse_signal_file(signal_file)
    else:
        print(f"\n  ⚠️  未找到选股结果文件")
        codes, names, signal_date = manual_input()

    if not codes:
        print("  ❌ 没有股票可跟踪")
        return

    # 2. 逐只跟踪
    print(f"\n🔍 开始跟踪 {len(codes)} 只股票（T+1 ~ T+{Config.TRACK_DAYS}）...\n")

    results = []
    for i, (code, name) in enumerate(zip(codes, names)):
        print(f"  [{i+1}/{len(codes)}] {code} {name}...", end="", flush=True)
        r = track_one_stock(code, name, signal_date, Config.TRACK_DAYS)
        results.append(r)

        if r['status'] == 'OK':
            print(f" 累计{r['final_return']:>+.2f}% 红盘{r['red_days']}天")
        else:
            print(f" {r['status']}")

    # 3. 输出报告
    print_report(results, signal_date, Config.TRACK_DAYS)

    # 4. 保存Excel
    try:
        save_excel(results, signal_date, Config.TRACK_DAYS)
    except ImportError:
        print("\n  ⚠️  安装 openpyxl 可导出Excel: pip install openpyxl")

    print("\n  💡 提示: 可用参数调整")
    print(f"     --days 10          跟踪10个交易日")
    print(f"     --buy close        改为选股日收盘买入")
    print(f"     --file xxx.xlsx    指定选股文件")


if __name__ == '__main__':
    main()