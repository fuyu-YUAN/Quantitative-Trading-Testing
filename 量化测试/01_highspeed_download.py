"""
01_highspeed_download.py - 高速版：全市场历史数据下载
改进：
  1. 多进程并行下载（每个进程独立 bs.login）
  2. 智能增量更新（只下载缺失的新数据）
  3. 批量写入，减少IO
  4. ★ 实时进度条显示
"""

import baostock as bs
import pandas as pd
import os
import sys
import time
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count, Manager, Queue
import warnings

warnings.filterwarnings('ignore')

# ============================================================
#   配置
# ============================================================
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'history')
DAYS_BACK = 500
NUM_WORKERS = 3
INCREMENTAL = True


# ============================================================
#   进度条显示
# ============================================================
class ProgressBar:
    """终端实时进度条"""

    def __init__(self, total, bar_length=40):
        self.total = total
        self.bar_length = bar_length
        self.current = 0
        self.ok = 0
        self.skip = 0
        self.fail = 0
        self.start_time = time.time()
        self.last_code = ''
        self.last_name = ''

    def update(self, status, code='', name=''):
        self.current += 1
        self.last_code = code
        self.last_name = name

        if status == 'ok':
            self.ok += 1
        elif status == 'skip':
            self.skip += 1
        else:
            self.fail += 1

        self._draw()

    def _draw(self):
        elapsed = time.time() - self.start_time
        pct = self.current / self.total if self.total > 0 else 0
        filled = int(self.bar_length * pct)
        bar = '█' * filled + '░' * (self.bar_length - filled)

        # 计算速度和剩余时间
        speed = self.current / elapsed if elapsed > 0 else 0
        remaining = (self.total - self.current) / speed if speed > 0 else 0

        # 截断股票名称（防止过长）
        display_name = self.last_name[:6] if self.last_name else ''
        display_code = self.last_code.split('.')[-1] if self.last_code else ''

        # 构造进度行
        line = (
            f"\r  {bar} {pct*100:5.1f}% "
            f"[{self.current}/{self.total}] "
            f"✓{self.ok} ⊘{self.skip} ✗{self.fail} "
            f"| {speed:.1f}只/秒 "
            f"| 剩余{remaining:.0f}秒 "
            f"| {display_code} {display_name}    "
        )

        sys.stdout.write(line)
        sys.stdout.flush()

        # 完成时换行
        if self.current >= self.total:
            sys.stdout.write('\n')
            sys.stdout.flush()


# ============================================================
#   获取最近交易日 + 股票列表
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


def get_main_board_stocks():
    """获取主板A股列表（排除ST）"""
    trade_date, all_rows, fields = get_latest_trade_date()
    if not trade_date:
        print("❌ 找不到最近的交易日数据")
        return [], trade_date

    df = pd.DataFrame(all_rows, columns=fields)
    print(f"  全市场证券数: {len(df)}")

    stocks = []
    for _, row in df.iterrows():
        code = row['code']
        if '.' not in code:
            continue

        prefix = code.split('.')[0]
        pure_code = code.split('.')[1]

        if prefix not in ('sh', 'sz'):
            continue
        if not pure_code.isdigit() or len(pure_code) != 6:
            continue

        is_main = False
        if pure_code.startswith('60'):
            is_main = True
        elif pure_code.startswith('000') or pure_code.startswith('001'):
            is_main = True
        elif pure_code.startswith('002') or pure_code.startswith('003'):
            is_main = True

        if is_main:
            code_name = row.get('code_name', '')
            if 'ST' in str(code_name).upper() or '退' in str(code_name):
                continue
            stocks.append((code, code_name))

    return stocks, trade_date


# ============================================================
#   增量检查
# ============================================================
def get_local_last_date(pure_code):
    """读取本地CSV最后一条日期"""
    daily_file = os.path.join(DATA_DIR, f'{pure_code}_daily.csv')
    if not os.path.exists(daily_file):
        return None
    try:
        df = pd.read_csv(daily_file, usecols=['date'], dtype={'date': str})
        if len(df) == 0:
            return None
        last_date = df['date'].iloc[-1].strip()
        return last_date
    except Exception:
        return None


# ============================================================
#   ★ 子进程工作函数（带进度队列）
# ============================================================
def download_one_worker(args):
    """
    子进程工作函数
    完成每只股票后，通过 progress_queue 发送进度消息
    """
    task_list, start_date_default, end_date, incremental, progress_queue = args

    # 每个子进程独立登录
    lg = bs.login()
    if lg.error_code != '0':
        for code, name in task_list:
            progress_queue.put(('fail', code, name, 'login_failed'))
        return [('fail', code, name, 'login_failed') for code, name in task_list]

    results = []
    fields = 'date,open,high,low,close,volume,amount,turn,pctChg'

    for code, code_name in task_list:
        pure_code = code.split('.')[1]
        daily_file = os.path.join(DATA_DIR, f'{pure_code}_daily.csv')
        weekly_file = os.path.join(DATA_DIR, f'{pure_code}_weekly.csv')

        # 增量：确定起始日期
        actual_start = start_date_default
        need_merge = False

        if incremental:
            local_last = get_local_last_date(pure_code)
            if local_last and local_last >= end_date:
                results.append(('skip', code, code_name, ''))
                progress_queue.put(('skip', code, code_name, ''))  # ★ 发送进度
                continue
            elif local_last:
                try:
                    last_dt = datetime.strptime(local_last, '%Y-%m-%d')
                    actual_start = (last_dt + timedelta(days=1)).strftime('%Y-%m-%d')
                    if actual_start > end_date:
                        results.append(('skip', code, code_name, ''))
                        progress_queue.put(('skip', code, code_name, ''))  # ★
                        continue
                    need_merge = True
                except Exception:
                    pass

        try:
            # ---- 日K ----
            rs = bs.query_history_k_data_plus(
                code, fields,
                start_date=actual_start,
                end_date=end_date,
                frequency='d',
                adjustflag='2'
            )

            if rs.error_code != '0':
                results.append(('fail', code, code_name, rs.error_msg))
                progress_queue.put(('fail', code, code_name, rs.error_msg))  # ★
                continue

            rows = []
            while (rs.error_code == '0') and rs.next():
                rows.append(rs.get_row_data())

            if len(rows) == 0 and not need_merge:
                results.append(('fail', code, code_name, 'no_data'))
                progress_queue.put(('fail', code, code_name, 'no_data'))  # ★
                continue

            df_new = pd.DataFrame(rows, columns=rs.fields) if rows else pd.DataFrame()

            # 增量合并
            if need_merge and os.path.exists(daily_file) and len(df_new) > 0:
                df_old = pd.read_csv(daily_file, dtype=str)
                df_all = pd.concat([df_old, df_new], ignore_index=True)
                df_all.drop_duplicates(subset=['date'], keep='last', inplace=True)
                df_all.sort_values('date', inplace=True)
                df_all.to_csv(daily_file, index=False, encoding='utf-8-sig')
            elif len(df_new) > 0:
                df_new.to_csv(daily_file, index=False, encoding='utf-8-sig')
            elif need_merge:
                results.append(('skip', code, code_name, ''))
                progress_queue.put(('skip', code, code_name, ''))  # ★
                continue

            # ---- 周K ----
            rs_w = bs.query_history_k_data_plus(
                code, fields,
                start_date=start_date_default,
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

            results.append(('ok', code, code_name, f'{len(rows)}d'))
            progress_queue.put(('ok', code, code_name, f'{len(rows)}d'))  # ★

        except Exception as e:
            results.append(('fail', code, code_name, str(e)))
            progress_queue.put(('fail', code, code_name, str(e)))  # ★

    bs.logout()
    return results


# ============================================================
#   ★ 进度监控线程（主进程中运行）
# ============================================================
def progress_monitor(progress_queue, total_count):
    """主进程中读取队列，实时更新进度条"""
    pb = ProgressBar(total_count)

    finished = 0
    while finished < total_count:
        try:
            # 超时1秒，防止卡死
            msg = progress_queue.get(timeout=120)
            status, code, name, info = msg
            pb.update(status, code, name)
            finished += 1
        except Exception:
            # 超时，继续等
            pass

    return pb


# ============================================================
#   任务分配
# ============================================================
def split_tasks(stocks, n_workers):
    """均匀分配任务到各worker"""
    chunks = [[] for _ in range(n_workers)]
    for i, stock in enumerate(stocks):
        chunks[i % n_workers].append(stock)
    return [c for c in chunks if len(c) > 0]


# ============================================================
#   主函数
# ============================================================
def main():
    print("=" * 70)
    print("  全市场历史数据下载（高速多进程 + 进度条）")
    print(f"  并发进程数: {NUM_WORKERS} | 增量模式: {INCREMENTAL}")
    print("=" * 70)

    # 登录
    lg = bs.login()
    print(f"  登录: error_code={lg.error_code}")
    if lg.error_code != '0':
        print("❌ 登录失败")
        return

    # 获取股票列表
    print("\n[1] 获取主板股票列表...")
    stocks, trade_date = get_main_board_stocks()
    print(f"  ✅ 主板股票: {len(stocks)} 只")

    if len(stocks) == 0:
        print("❌ 没有获取到股票")
        bs.logout()
        return

    print("  示例:")
    for code, name in stocks[:5]:
        print(f"    {code} {name}")

    bs.logout()

    # 创建目录
    os.makedirs(DATA_DIR, exist_ok=True)

    # 日期
    end_date = trade_date if trade_date else time.strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=int(DAYS_BACK * 1.5))).strftime('%Y-%m-%d')

    print(f"\n[2] 下载区间: {start_date} ~ {end_date}")
    print(f"    数据目录: {DATA_DIR}")

    # 分配任务
    actual_workers = min(NUM_WORKERS, len(stocks))
    chunks = split_tasks(stocks, actual_workers)

    print(f"    分配: {len(chunks)} 个进程，每进程约 {len(stocks) // actual_workers} 只")
    print(f"\n[3] 开始并行下载...\n")

    # ★ 创建共享进度队列
    manager = Manager()
    progress_queue = manager.Queue()

    # 构造参数（把队列传给每个worker）
    pool_args = [
        (chunk, start_date, end_date, INCREMENTAL, progress_queue)
        for chunk in chunks
    ]

    t0 = time.time()

    # ★ 用 apply_async 异步启动，这样主进程可以同时读进度
    pool = Pool(processes=actual_workers)
    async_results = []
    for arg in pool_args:
        r = pool.apply_async(download_one_worker, (arg,))
        async_results.append(r)

    pool.close()  # 不再接受新任务

    # ★ 主进程实时读取进度队列并显示
    total_count = len(stocks)
    pb = ProgressBar(total_count)

    finished = 0
    while finished < total_count:
        try:
            msg = progress_queue.get(timeout=300)
            status, code, name, info = msg
            pb.update(status, code, name)
            finished += 1
        except Exception:
            # 检查是否所有worker都已结束
            all_done = all(r.ready() for r in async_results)
            if all_done:
                break

    pool.join()  # 等待所有进程结束

    # 汇总结果
    elapsed = time.time() - t0

    ok_count = pb.ok
    skip_count = pb.skip
    fail_count = pb.fail

    # 收集失败列表
    fail_list = []
    for r in async_results:
        try:
            worker_results = r.get(timeout=10)
            for status, code, name, info in worker_results:
                if status == 'fail':
                    fail_list.append(f"{code} {name} ({info})")
        except Exception:
            pass

    total = ok_count + skip_count + fail_count
    speed = total / elapsed if elapsed > 0 else 0

    print("\n" + "=" * 70)
    print("  ✅ 下载完成!")
    print(f"  📊 成功: {ok_count} | 跳过(已最新): {skip_count} | 失败: {fail_count}")
    print(f"  ⏱  耗时: {elapsed:.1f}秒 ({elapsed / 60:.1f}分钟)")
    print(f"  🚀 速度: {speed:.1f} 只/秒")
    print(f"  📁 目录: {DATA_DIR}")

    v2_estimate = total / 3.0 / 60
    print(f"\n  📈 对比单线程: ~{v2_estimate:.0f}分钟 → 本次: {elapsed / 60:.1f}分钟")
    if elapsed > 0 and v2_estimate > 0:
        print(f"     加速比: ~{v2_estimate * 60 / elapsed:.1f}x")

    print("=" * 70)

    # 失败列表
    if fail_list:
        print(f"\n  失败列表 (共{len(fail_list)}只):")
        for item in fail_list[:30]:
            print(f"    {item}")
        if len(fail_list) > 30:
            print(f"    ... 还有 {len(fail_list) - 30} 只")


if __name__ == '__main__':
    main()