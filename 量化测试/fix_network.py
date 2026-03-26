# -*- coding: utf-8 -*-
"""
网络修复模块 - 解决手机热点/代理导致akshare无法连接的问题
在其他py文件最顶部 import fix_network 即可
"""
import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========== 1. 清除所有代理 ==========
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)
    os.environ[key] = ''

# ========== 2. 给 requests 打补丁：加重试 + 伪装浏览器 ==========
_original_get = requests.Session.get
_original_post = requests.Session.post

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
}

def _patched_get(self, url, **kwargs):
    # 清除代理
    kwargs.setdefault('proxies', {'http': '', 'https': ''})
    # 加浏览器头
    if 'headers' not in kwargs or kwargs['headers'] is None:
        kwargs['headers'] = HEADERS.copy()
    else:
        for k, v in HEADERS.items():
            kwargs['headers'].setdefault(k, v)
    # 重试3次
    for attempt in range(3):
        try:
            return _original_get(self, url, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < 2:
                wait = (attempt + 1) * 2
                print(f"  连接失败，{wait}秒后重试({attempt+1}/3)...")
                time.sleep(wait)
            else:
                raise

def _patched_post(self, url, **kwargs):
    kwargs.setdefault('proxies', {'http': '', 'https': ''})
    if 'headers' not in kwargs or kwargs['headers'] is None:
        kwargs['headers'] = HEADERS.copy()
    else:
        for k, v in HEADERS.items():
            kwargs['headers'].setdefault(k, v)
    for attempt in range(3):
        try:
            return _original_post(self, url, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < 2:
                wait = (attempt + 1) * 2
                print(f"  连接失败，{wait}秒后重试({attempt+1}/3)...")
                time.sleep(wait)
            else:
                raise

requests.Session.get = _patched_get
requests.Session.post = _patched_post

print("[fix_network] 网络补丁已加载 ✅")