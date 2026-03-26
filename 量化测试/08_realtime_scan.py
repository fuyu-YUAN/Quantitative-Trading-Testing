"""
09_debug_http.py
精确诊断HTTP通信问题
"""
import socket
import ssl
import sys

def test1_raw_http():
    """测试1: 最基础的HTTP GET"""
    print("=" * 60)
    print("测试1: 原始HTTP请求")
    print("=" * 60)
    
    host = "push2.eastmoney.com"
    port = 80
    
    # 最简单的请求路径
    path = "/api/qt/clist/get?cb=j&pn=1&pz=5&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:1+t:2&fields=f12,f14"
    
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        f"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n"
        f"Accept: */*\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    )
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((host, port))
        print(f"  ✅ 已连接 {host}:{port}")
        
        sock.sendall(request.encode())
        print(f"  ✅ 已发送请求 ({len(request)} bytes)")
        print(f"  📤 请求内容:")
        print(f"     GET {path[:80]}...")
        
        # 分段读取响应
        chunks = []
        total = 0
        while True:
            try:
                data = sock.recv(4096)
                if not data:
                    break
                chunks.append(data)
                total += len(data)
                print(f"  📥 收到 {len(data)} bytes (累计 {total})")
            except socket.timeout:
                print(f"  ⏰ 读取超时")
                break
        
        sock.close()
        
        if total == 0:
            print(f"  ❌ 响应为空!")
            print(f"  💡 可能原因: 防火墙/深信服 截断了响应")
            return False
        
        response = b"".join(chunks)
        
        # 解析响应头
        if b"\r\n\r\n" in response:
            header_part, body_part = response.split(b"\r\n\r\n", 1)
            header_str = header_part.decode('utf-8', errors='replace')
            print(f"\n  📋 响应头:")
            for line in header_str.split("\r\n")[:10]:
                print(f"     {line}")
            print(f"\n  📋 响应体前200字符:")
            body_str = body_part.decode('utf-8', errors='replace')[:200]
            print(f"     {body_str}")
            return True
        else:
            text = response.decode('utf-8', errors='replace')[:300]
            print(f"\n  📋 原始响应前300字符:")
            print(f"     {text}")
            return True
            
    except Exception as e:
        print(f"  ❌ 异常: {e}")
        return False


def test2_https():
    """测试2: HTTPS请求"""
    print("\n" + "=" * 60)
    print("测试2: HTTPS请求")
    print("=" * 60)
    
    host = "push2.eastmoney.com"
    port = 443
    path = "/api/qt/clist/get?cb=j&pn=1&pz=5&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:1+t:2&fields=f12,f14"
    
    try:
        # 检查ssl模块
        print(f"  SSL版本: {ssl.OPENSSL_VERSION}")
        
        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw_sock.settimeout(10)
        raw_sock.connect((host, port))
        print(f"  ✅ TCP已连接 {host}:{port}")
        
        ctx = ssl.create_default_context()
        sock = ctx.wrap_socket(raw_sock, server_hostname=host)
        print(f"  ✅ SSL握手成功: {sock.version()}")
        
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n"
            f"Accept: */*\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        )
        
        sock.sendall(request.encode())
        print(f"  ✅ 已发送请求")
        
        chunks = []
        total = 0
        while True:
            try:
                data = sock.recv(4096)
                if not data:
                    break
                chunks.append(data)
                total += len(data)
                print(f"  📥 收到 {len(data)} bytes (累计 {total})")
            except socket.timeout:
                break
        
        sock.close()
        
        if total == 0:
            print(f"  ❌ HTTPS也是空响应!")
            return False
        
        response = b"".join(chunks)
        if b"\r\n\r\n" in response:
            header_part, body_part = response.split(b"\r\n\r\n", 1)
            header_str = header_part.decode('utf-8', errors='replace')
            print(f"\n  📋 响应头:")
            for line in header_str.split("\r\n")[:10]:
                print(f"     {line}")
            print(f"\n  📋 响应体前200字符:")
            body_str = body_part.decode('utf-8', errors='replace')[:200]
            print(f"     {body_str}")
            return True
        else:
            text = response.decode('utf-8', errors='replace')[:300]
            print(f"     {text}")
            return True
            
    except ssl.SSLError as e:
        print(f"  ❌ SSL错误: {e}")
        return False
    except Exception as e:
        print(f"  ❌ 异常: {type(e).__name__}: {e}")
        return False


def test3_alternative_hosts():
    """测试3: 尝试其他东财域名"""
    print("\n" + "=" * 60)
    print("测试3: 其他东财接口")
    print("=" * 60)
    
    hosts = [
        ("push2.eastmoney.com", 80),
        ("push2ex.eastmoney.com", 80),
        ("datacenter-web.eastmoney.com", 80),
        ("data.eastmoney.com", 80),
        ("quote.eastmoney.com", 80),
        ("nufm.dfcfw.com", 80),
    ]
    
    for host, port in hosts:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(8)
            sock.connect((host, port))
            
            if host == "nufm.dfcfw.com":
                path = "/EM_ChgNum498/QuantTech/StockList/getStockList?type=CT&token=894050c76af8597a853f5b408b759f5d&st=ChangePercent&sr=-1&p=1&ps=5&js=var%20data={pages:(pc),data:[(x)]}"
            else:
                path = "/api/qt/clist/get?pn=1&pz=5&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:1+t:2&fields=f12,f14"
            
            request = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {host}\r\n"
                f"User-Agent: Mozilla/5.0\r\n"
                f"Accept: */*\r\n"
                f"Connection: close\r\n"
                f"\r\n"
            )
            
            sock.sendall(request.encode())
            
            data = b""
            while True:
                try:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                except socket.timeout:
                    break
            
            sock.close()
            
            if len(data) > 0:
                first_line = data.split(b"\r\n")[0].decode('utf-8', errors='replace')
                print(f"  ✅ {host:35s} → {len(data):>6} bytes | {first_line}")
            else:
                print(f"  ❌ {host:35s} → 空响应")
                
        except Exception as e:
            print(f"  ❌ {host:35s} → {e}")


def test4_ip_direct():
    """测试4: 直接用IP访问(绕过DNS问题)"""
    print("\n" + "=" * 60)
    print("测试4: 直接IP访问")
    print("=" * 60)
    
    # 先解析IP
    try:
        ip = socket.gethostbyname("push2.eastmoney.com")
        print(f"  push2.eastmoney.com → {ip}")
    except:
        ip = "47.112.165.11"
        print(f"  DNS失败, 使用硬编码IP: {ip}")
    
    path = "/api/qt/clist/get?pn=1&pz=5&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:1+t:2&fields=f12,f14"
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((ip, 80))
        print(f"  ✅ 已连接 {ip}:80")
        
        # 注意Host头用域名
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: push2.eastmoney.com\r\n"
            f"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n"
            f"Accept: */*\r\n"
            f"Referer: https://data.eastmoney.com/\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        )
        
        sock.sendall(request.encode())
        
        data = b""
        while True:
            try:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
            except socket.timeout:
                break
        
        sock.close()
        
        if len(data) > 0:
            print(f"  📥 收到 {len(data)} bytes")
            text = data.decode('utf-8', errors='replace')[:400]
            print(f"  📋 内容:\n{text}")
        else:
            print(f"  ❌ 空响应 - 确认是被中间设备拦截!")
            
    except Exception as e:
        print(f"  ❌ {e}")


def test5_check_proxy():
    """测试5: 检查系统代理设置"""
    print("\n" + "=" * 60)
    print("测试5: 系统环境检查")
    print("=" * 60)
    
    # 检查代理环境变量
    import os
    proxy_vars = ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'no_proxy']
    for var in proxy_vars:
        val = os.environ.get(var, '')
        if val:
            print(f"  ⚠️ {var} = {val}")
    
    if not any(os.environ.get(v) for v in proxy_vars):
        print(f"  ✅ 无代理环境变量")
    
    # 检查Python版本
    print(f"  Python: {sys.version}")
    print(f"  平台: {sys.platform}")
    
    # 检查ssl
    try:
        print(f"  SSL: {ssl.OPENSSL_VERSION}")
    except:
        print(f"  ❌ SSL模块异常")
    
    # 尝试用不同方式发送
    print(f"\n  测试HTTP/1.0 (无chunked)...")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect(("push2.eastmoney.com", 80))
        
        # 用HTTP/1.0，服务器不会用chunked编码
        request = (
            f"GET /api/qt/clist/get?pn=1&pz=3&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:1+t:2&fields=f12,f14 HTTP/1.0\r\n"
            f"Host: push2.eastmoney.com\r\n"
            f"User-Agent: Mozilla/5.0\r\n"
            f"\r\n"
        )
        
        sock.sendall(request.encode())
        
        data = b""
        while True:
            try:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
            except socket.timeout:
                break
        
        sock.close()
        
        if len(data) > 0:
            print(f"  ✅ HTTP/1.0 收到 {len(data)} bytes")
            text = data.decode('utf-8', errors='replace')[:300]
            print(f"     {text}")
        else:
            print(f"  ❌ HTTP/1.0 也是空响应")
            print(f"\n  🔴 结论: 公司网络在HTTP层拦截了东财域名!")
            print(f"  💡 建议: 用手机热点试试")
            
    except Exception as e:
        print(f"  ❌ {e}")


def main():
    print("=" * 60)
    print("  🔬 HTTP通信精确诊断")
    print("=" * 60)
    
    test1_raw_http()
    test2_https()
    test3_alternative_hosts()
    test4_ip_direct()
    test5_check_proxy()
    
    print("\n" + "=" * 60)
    print("  诊断完成")
    print("=" * 60)
    input("\n按回车退出...")


if __name__ == '__main__':
    main()