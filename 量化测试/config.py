# -*- coding: utf-8 -*-
"""
config.py - 全局配置文件（baostock版本）
"""
import os

# ==================== 路径配置 ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
HISTORY_DIR = os.path.join(DATA_DIR, "history")
RESULT_DIR = os.path.join(DATA_DIR, "result")

for d in [DATA_DIR, HISTORY_DIR, RESULT_DIR]:
    os.makedirs(d, exist_ok=True)

# ==================== 股票池配置 ====================
# baostock格式: sh.600000 / sz.000001
STOCK_POOL = [
    "sh.600519",  # 贵州茅台
    "sz.000001",  # 平安银行
    "sh.601318",  # 中国平安
    "sz.000858",  # 五粮液
    "sh.600036",  # 招商银行
    "sz.002594",  # 比亚迪
    "sh.601899",  # 紫金矿业
    "sz.000333",  # 美的集团
    "sh.600276",  # 恒瑞医药
    "sz.002475",  # 立讯精密
    "sh.601012",  # 隆基绿能
    "sz.300750",  # 宁德时代
    "sh.688981",  # 中芯国际
    "sz.002714",  # 牧原股份
    "sh.600900",  # 长江电力
    "sz.000651",  # 格力电器
    "sh.601166",  # 兴业银行
    "sz.002304",  # 洋河股份
    "sh.600309",  # 万华化学
    "sz.300059",  # 东方财富
]

# ==================== 数据配置 ====================
# 历史数据起始日期
HISTORY_START_DATE = "2024-01-01"

# K线类型: d=日K, w=周K, m=月K
KLINE_TYPES = ["d", "w"]

# 复权类型: 1=后复权, 2=前复权, 3=不复权
ADJUST_FLAG = "2"

# ==================== 技术指标参数 ====================
MA_SHORT = 5       # 短期均线
MA_LONG = 20       # 长期均线
RSI_PERIOD = 14    # RSI周期
RSI_OVERBOUGHT = 70   # RSI超买线
RSI_OVERSOLD = 30     # RSI超卖线
VOLUME_RATIO_THRESHOLD = 1.5  # 放量倍数阈值

# ==================== 信号评分权重 ====================
WEIGHTS = {
    "golden_cross": 30,       # 金叉
    "volume_surge": 20,       # 放量
    "breakout_high": 25,      # 突破前高
    "rsi_not_overbought": 15, # RSI不超买
    "ma_trend_up": 10,        # 均线多头排列
}

# 最低买入评分
MIN_BUY_SCORE = 60