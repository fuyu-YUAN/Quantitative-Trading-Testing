"""
01_data_history_v2.py - 修复版：全市场历史数据下载
改进：
  1. 用 query_all_stock 获取列表（更可靠）
  2. 单线程顺序下载（避免多线程登录冲突）
  3. 字段用列名而非索引
  4. 增加详细日志
"""

import baostock as bs
import pandas as pd
import os
import sys
import time
from datetime import datetime, timedelta

# ============================================================
#   配置
# ============================================================
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'history')
DAYS_BACK = 500
SKIP_IF_RECENT = True

# ============================================================
#   获取最近交易日
# ============================================================
def get_latest_trade_date():
    """向前找最近的交易日"""
    for i in range(0, 10):
        test_date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        rs = bs.query_all_stock(day=test_date)
        rows = []
        while (rs.error_code == '0') and rs.next():
            rows.append(rs.get_row_data())
        if len(rows) > 0:
            print(f"  最近交易日: {test_date} ({len(rows)} 只证券)")
            return test_date, rows, rs.fields
    return None, [], []


# ============================================================
#   获取主板股票列表
# ============================================================
def get_main_board_stocks():
    """用 query_all_stock 获取，然后过滤主板"""
    trade_date, all_rows, fields = get_latest_trade_date()

    if not trade_date:
        print("❌ 找不到最近的交易日数据")
        return []

    df = pd.DataFrame(all_rows, columns=fields)
    print(f"  全市场证券数: {len(df)}")
    print(f"  字段: {list(df.columns)}")

    # 过滤出A股主板
    stocks = []
    for _, row in df.iterrows():
        code = row['code']          # sh.600519
        trade_status = row.get('tradeStatus', '1')

        if '.' not in code:
            continue

        prefix = code.split('.')[0]  # sh 或 sz
        pure_code = code.split('.')[1]  # 600519

        # 只要 sh 和 sz
        if prefix not in ('sh', 'sz'):
            continue

        # 只要6位数字代码
        if not pure_code.isdigit() or len(pure_code) != 6:
            continue

        # 主板过滤
        is_main = False
        # 上证主板: 600xxx, 601xxx, 603xxx, 605xxx
        if pure_code.startswith('60'):
            is_main = True
        # 深证主板: 000xxx, 001xxx
        elif pure_code.startswith('000') or pure_code.startswith('001'):
            is_main = True
        # 中小板（现并入深证主板）: 002xxx, 003xxx
        elif pure_code.startswith('002') or pure_code.startswith('003'):
            is_main = True

        if is_main:
            code_name = row.get('code_name', '')
            # 排除ST
            if 'ST' in str(code_name).upper() or '退' in str(code_name):
                continue
            stocks.append((code, code_name))

    return stocks


# ============================================================
#   下载单只股票
# ============================================================
def download_one(code, code_name, start_date, end_date):
    """下载一只股票的日K+周K"""
    pure_code = code.split('.')[1]
    daily_file = os.path.join(DATA_DIR, f'{pure_code}_daily.csv')
    weekly_file = os.path.join(DATA_DIR, f'{pure_code}_weekly.csv')

    # 断点续传
    if SKIP_IF_RECENT and os.path.exists(daily_file):
        mtime = os.path.getmtime(daily_file)
        if datetime.fromtimestamp(mtime).date() == datetime.now().date():
            return 'skip'

    fields = 'date,open,high,low,close,volume,amount,turn,pctChg'

    try:
        # 日K
        rs = bs.query_history_k_data_plus(
            code, fields,
            start_date=start_date,
            end_date=end_date,
            frequency='d',
            adjustflag='2'
        )

        if rs.error_code != '0':
            return 'fail'

        rows = []
        while (rs.error_code == '0') and rs.next():
            rows.append(rs.get_row_data())

        if len(rows) == 0:
            return 'fail'

        df_daily = pd.DataFrame(rows, columns=rs.fields)
        df_daily.to_csv(daily_file, index=False, encoding='utf-8-sig')

        # 周K
        rs_w = bs.query_history_k_data_plus(
            code, fields,
            start_date=start_date,
            end_date=end_date,
            frequency='w',
            adjustflag='2'
        )

        rows_w = []
        while (rs_w.error_code == '0') and rs_w.next():
            rows_w.append(rs_w.get_row_data())

        if len(rows_w) > 0:
            df_weekly = pd.DataFrame(rows_w, columns=rs_w.fields)
            df_weekly.to_csv(weekly_file, index=False, encoding='utf-8-sig')

        return 'ok'

    except Exception as e:
        print(f"    ❌ {code} 异常: {e}")
        return 'fail'


# ============================================================
#   主函数
# ============================================================
def main():
    print("=" * 60)
    print("  全市场历史数据下载 v2（修复版）")
    print("=" * 60)

    # 登录
    lg = bs.login()
    print(f"  登录: error_code={lg.error_code}, error_msg={lg.error_msg}")
    if lg.error_code != '0':
        print("❌ 登录失败")
        return

    # 获取股票列表
    print("\n[1] 获取主板股票列表...")
    stocks = get_main_board_stocks()
    print(f"  ✅ 主板股票: {len(stocks)} 只")

    if len(stocks) == 0:
        print("❌ 没有获取到股票，请检查网络连接")
        bs.logout()
        return

    # 显示前10只确认
    print("\n  前10只股票:")
    for code, name in stocks[:10]:
        print(f"    {code} {name}")

    # 创建目录
    os.makedirs(DATA_DIR, exist_ok=True)

    # 日期
    end_date = time.strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=int(DAYS_BACK * 1.5))).strftime('%Y-%m-%d')
    print(f"\n[2] 开始下载: {start_date} ~ {end_date}")
    print(f"    数据目录: {DATA_DIR}")
    print(f"    共 {len(stocks)} 只股票")

    # 逐只下载（单线程，稳定）
    t0 = time.time()
    ok_count = 0
    skip_count = 0
    fail_count = 0
    fail_list = []

    for i, (code, code_name) in enumerate(stocks):
        status = download_one(code, code_name, start_date, end_date)

        if status == 'ok':
            ok_count += 1
        elif status == 'skip':
            skip_count += 1
        else:
            fail_count += 1
            fail_list.append(f"{code} {code_name}")

        # 进度显示
        done = i + 1
        if done % 50 == 0 or done == len(stocks) or done <= 5:
            elapsed = time.time() - t0
            speed = done / elapsed if elapsed > 0 else 0
            eta = (len(stocks) - done) / speed / 60 if speed > 0 else 0
            print(f"  [{done}/{len(stocks)}] ✅{ok_count} ⏭{skip_count} "
                  f"❌{fail_count} | {speed:.1f}只/秒 | 剩余~{eta:.1f}分钟")

        # 每100只暂停一下，避免被限频
        if done % 100 == 0:
            time.sleep(1)

    bs.logout()

    # 汇总
    elapsed = time.time() - t0
    print()
    print("=" * 60)
    print("  下载完成!")
    print(f"  ✅ 成功: {ok_count}")
    print(f"  ⏭  跳过: {skip_count}")
    print(f"  ❌ 失败: {fail_count}")
    print(f"  ⏱  耗时: {elapsed/60:.1f} 分钟")
    print(f"  📁 目录: {DATA_DIR}")
    print("=" * 60)

    if fail_list and len(fail_list) <= 20:
        print("\n  失败列表:")
        for item in fail_list:
            print(f"    {item}")


if __name__ == '__main__':
    main()