"""
07_realtime_scanner.py
实盘扫描: 0轴下 + 主力净流入 选股器 v7
使用原始socket绕过深信服拦截
滚动输出 + 日志保存
"""
import socket
import json
import time
import datetime
import os
import gzip
from io import BytesIO

# ============================================================
# 配置
# ============================================================
MIN_NET_INFLOW_WAN = 3000
SCAN_INTERVAL = 60
TOP_N = 80
LOG_DIR = "scan_logs"

# ============================================================
# 全局状态：记录上一轮结果，用于对比新增/消失
# ============================================================
last_scan_codes = set()

# ============================================================
# 日志
# ============================================================
def get_log_file():
    """每天一个日志文件"""
    os.makedirs(LOG_DIR, exist_ok=True)
    today = datetime.datetime.now().strftime('%Y%m%d')
    return os.path.join(LOG_DIR, f"scan_{today}.log")

def log_write(text):
    """同时打印和写入日志"""
    print(text)
    with open(get_log_file(), 'a', encoding='utf-8') as f:
        f.write(text + '\n')

# ============================================================
# 原始socket HTTP请求（绕过深信服）
# ============================================================
def http_get(host, path, port=80, timeout=15):
    """用原始socket发HTTP请求，手动解析响应"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))

        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n"
            f"Accept: */*\r\n"
            f"Accept-Encoding: gzip\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        )
        sock.sendall(request.encode('utf-8'))

        chunks = []
        while True:
            try:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                chunks.append(chunk)
            except socket.timeout:
                break

        raw = b''.join(chunks)

        sep = raw.find(b'\r\n\r\n')
        if sep == -1:
            return None

        header_bytes = raw[:sep]
        body = raw[sep+4:]
        headers_text = header_bytes.decode('utf-8', errors='ignore').lower()

        if 'transfer-encoding: chunked' in headers_text:
            body = decode_chunked(body)

        if 'content-encoding: gzip' in headers_text:
            try:
                body = gzip.GzipFile(fileobj=BytesIO(body)).read()
            except:
                pass

        return body.decode('utf-8', errors='ignore')
    finally:
        sock.close()


def decode_chunked(data):
    """解码chunked transfer encoding"""
    decoded = bytearray()
    idx = 0
    while idx < len(data):
        end = data.find(b'\r\n', idx)
        if end == -1:
            break
        size_str = data[idx:end].decode('ascii', errors='ignore').strip()
        if not size_str:
            idx = end + 2
            continue
        try:
            chunk_size = int(size_str, 16)
        except ValueError:
            break
        if chunk_size == 0:
            break
        start = end + 2
        decoded.extend(data[start:start + chunk_size])
        idx = start + chunk_size + 2
    return bytes(decoded)


# ============================================================
# 东方财富API
# ============================================================
FLOW_HOST = "push2.eastmoney.com"

def build_flow_path(page, page_size=100):
    fields = "f2,f3,f12,f14,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87"
    path = (
        f"/api/qt/clist/get?"
        f"fid=f62&po=1&pz={page_size}&pn={page}"
        f"&np=1&fltt=2&invt=2"
        f"&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
        f"&fields={fields}"
        f"&_={int(time.time()*1000)}"
    )
    return path


def fetch_flow_page(page=1, page_size=100):
    path = build_flow_path(page, page_size)
    text = http_get(FLOW_HOST, path)
    if not text:
        return None, 0
    data = json.loads(text)
    if not data or 'data' not in data or not data['data']:
        return None, 0
    items = data['data'].get('diff', [])
    total = data['data'].get('total', 0)
    return items, total


# ============================================================
# 数据检查
# ============================================================
def first_data_check():
    print("\n📡 验证数据源...")
    items, total = fetch_flow_page(1, 5)
    if not items:
        return False

    print(f"  ✅ 连接成功! 共 {total} 只股票")
    print(f"\n  === 样本数据（前3条）===")
    print(f"  {'代码':<8} {'名称':<8} {'涨跌幅':>8} {'主力净流入':>14} {'现价':>8}")
    print(f"  {'-'*56}")

    for item in items[:3]:
        code = item.get('f12', '?')
        name = item.get('f14', '?')
        pct = item.get('f3', 0)
        inflow = item.get('f62', 0)
        price = item.get('f2', 0)

        if abs(inflow) > 1e8:
            inflow_desc = f"{inflow:.0f} (元?)"
        elif abs(inflow) > 1e4:
            inflow_desc = f"{inflow:.0f} (万?)"
        else:
            inflow_desc = f"{inflow:.2f} (亿?)"

        print(f"  {code:<8} {name:<8} {pct:>8.2f}% {inflow_desc:>14} {price:>8.2f}")

    sample_inflow = abs(items[0].get('f62', 0))
    if sample_inflow > 1e8:
        unit = "元"
    elif sample_inflow > 1e4:
        unit = "万元"
    else:
        unit = "亿元"

    print(f"\n  📊 自动判断: f62(主力净流入) 单位 = {unit}")
    return True


# ============================================================
# 扫描逻辑
# ============================================================
def scan_once():
    results = []
    page = 1
    page_size = 100
    max_pages = 20

    while page <= max_pages:
        items, total = fetch_flow_page(page, page_size)
        if not items:
            break

        for item in items:
            code = item.get('f12', '')
            name = item.get('f14', '')
            pct = item.get('f3', 0)
            price = item.get('f2', 0)
            inflow_raw = item.get('f62', 0)

            if not code or price == '-' or inflow_raw == '-':
                continue
            try:
                pct = float(pct)
                price = float(price)
                inflow_raw = float(inflow_raw)
            except (ValueError, TypeError):
                continue

            if abs(inflow_raw) > 1e7:
                inflow_wan = inflow_raw / 10000
            else:
                inflow_wan = inflow_raw

            if pct >= 0:
                continue

            if inflow_wan < MIN_NET_INFLOW_WAN:
                return results

            super_in = item.get('f66', 0)
            big_in = item.get('f72', 0)

            results.append({
                'code': code,
                'name': name,
                'price': price,
                'pct': pct,
                'inflow_wan': inflow_wan,
                'inflow_raw': inflow_raw,
                'super_in': super_in,
                'big_in': big_in,
            })

        last_inflow = items[-1].get('f62', 0)
        try:
            last_inflow = float(last_inflow)
        except:
            last_inflow = 0

        if abs(last_inflow) > 1e7:
            last_wan = last_inflow / 10000
        else:
            last_wan = last_inflow

        if last_wan < MIN_NET_INFLOW_WAN:
            break

        page += 1
        time.sleep(0.3)

    return results


# ============================================================
# 滚动显示结果（核心改动）
# ============================================================
def display_results(results, scan_count):
    """滚动追加显示，不清屏，标记新增/消失"""
    global last_scan_codes

    now = datetime.datetime.now().strftime('%H:%M:%S')
    results.sort(key=lambda x: x['inflow_wan'], reverse=True)

    current_codes = set(r['code'] for r in results)

    # 计算新增和消失
    new_codes = current_codes - last_scan_codes
    gone_codes = last_scan_codes - current_codes

    # ---- 分隔线 ----
    log_write("")
    log_write("┌" + "─" * 76 + "┐")
    log_write(f"│  📡 第{scan_count}轮扫描  {now}  "
              f"│ 命中 {len(results)} 只  "
              f"│ 🆕新增 {len(new_codes)}  "
              f"│ ❌消失 {len(gone_codes)}  │")
    log_write(f"│  条件: 下跌 + 主力净流入 > {MIN_NET_INFLOW_WAN}万"
              + " " * (76 - 30 - len(str(MIN_NET_INFLOW_WAN))) + "│")
    log_write("├" + "─" * 76 + "┤")

    if not results:
        log_write("│  ⏳ 未发现符合条件的股票"
                   + " " * 52 + "│")
        log_write("└" + "─" * 76 + "┘")
        last_scan_codes = current_codes
        return

    # 表头
    header = (f"│ {'#':>3}  {'代码':<8} {'名称':<10} "
              f"{'现价':>8} {'涨跌幅':>8} {'净流入(万)':>12} {'状态':>6} │")
    log_write(header)
    log_write("│" + "─" * 76 + "│")

    # 数据行
    for i, r in enumerate(results[:TOP_N], 1):
        # 状态标记
        if r['code'] in new_codes:
            tag = "🆕"
        else:
            tag = "  "

        line = (f"│ {i:>3}  {r['code']:<8} {r['name']:<10} "
                f"{r['price']:>8.2f} {r['pct']:>7.2f}% "
                f"{r['inflow_wan']:>11.0f}  {tag}   │")
        log_write(line)

    # 显示消失的股票
    if gone_codes and scan_count > 1:
        log_write("│" + "─" * 76 + "│")
        gone_list = ", ".join(sorted(gone_codes)[:10])
        if len(gone_codes) > 10:
            gone_list += f" ...等{len(gone_codes)}只"
        log_write(f"│  ❌ 本轮消失: {gone_list}"
                   + " " * max(0, 76 - 14 - len(gone_list)) + "│")

    if len(results) > TOP_N:
        log_write(f"│  ... 还有 {len(results)-TOP_N} 只未显示"
                   + " " * 50 + "│")

    log_write("└" + "─" * 76 + "┘")

    # 更新上轮记录
    last_scan_codes = current_codes


# ============================================================
# 累计统计（连续出现的股票更值得关注）
# ============================================================
appear_count = {}  # code -> 出现次数

def update_appear_count(results, scan_count):
    """统计每只股票累计出现轮数"""
    global appear_count
    for r in results:
        if r['code'] in appear_count:
            appear_count[r['code']]['count'] += 1
            appear_count[r['code']]['last_round'] = scan_count
            appear_count[r['code']]['name'] = r['name']
            appear_count[r['code']]['last_inflow'] = r['inflow_wan']
        else:
            appear_count[r['code']] = {
                'count': 1,
                'name': r['name'],
                'first_round': scan_count,
                'last_round': scan_count,
                'last_inflow': r['inflow_wan'],
            }


def show_summary():
    """收盘时显示累计统计"""
    if not appear_count:
        return

    log_write("")
    log_write("=" * 78)
    log_write("  📊 今日扫描累计统计（出现次数越多 → 主力持续流入越坚定）")
    log_write("=" * 78)

    # 按出现次数排序
    ranked = sorted(appear_count.items(),
                    key=lambda x: x[1]['count'], reverse=True)

    log_write(f"  {'#':>3} {'代码':<8} {'名称':<10} {'出现轮数':>8} "
              f"{'首次出现':>8} {'最后出现':>8} {'末轮净流入(万)':>14}")
    log_write(f"  {'-'*70}")

    for i, (code, info) in enumerate(ranked[:50], 1):
        log_write(f"  {i:>3} {code:<8} {info['name']:<10} "
                  f"{info['count']:>8} "
                  f"{'第'+str(info['first_round'])+'轮':>8} "
                  f"{'第'+str(info['last_round'])+'轮':>8} "
                  f"{info['last_inflow']:>13.0f}")

    log_write(f"\n  共追踪 {len(appear_count)} 只股票")
    log_write(f"  日志已保存: {get_log_file()}")


# ============================================================
# 主程序
# ============================================================
def main():
    print("=" * 60)
    print("  📡 0轴下 + 主力净流入 实盘扫描器 v7")
    print("  绕过深信服 | 原始socket直连 | 滚动输出 + 日志")
    print(f"  筛选: 今日下跌 + 主力净流入 > {MIN_NET_INFLOW_WAN}万")
    print(f"  日志目录: {os.path.abspath(LOG_DIR)}")
    print("=" * 60)

    if not first_data_check():
        print("\n  ❌ 数据源连接失败，请检查网络")
        return

    input("\n  按回车开始扫描...")

    scan_count = 0
    while True:
        try:
            scan_count += 1
            results = scan_once()
            update_appear_count(results, scan_count)
            display_results(results, scan_count)

            # 检查交易时间
            now = datetime.datetime.now()
            h, m = now.hour, now.minute
            if h < 9 or (h == 9 and m < 25):
                log_write(f"\n  ⏰ 盘前等待中... {SCAN_INTERVAL}秒后重试")
            elif h >= 15 and m > 5:
                log_write(f"\n  🔔 已收盘")
                show_summary()
                break
            else:
                log_write(f"  ⏱ {SCAN_INTERVAL}秒后第{scan_count+1}轮...")

            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log_write(f"\n\n  👋 手动停止，共扫描 {scan_count} 轮")
            show_summary()
            break
        except Exception as e:
            log_write(f"\n  ❌ 异常: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(10)


if __name__ == '__main__':
    main()