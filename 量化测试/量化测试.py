"""
config.py — 全局配置
所有可调参数集中管理
"""

import os
from datetime import datetime, timedelta

# ==================== 路径配置 ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
HISTORY_DIR = os.path.join(DATA_DIR, "history")
REPORT_DIR = os.path.join(DATA_DIR, "reports")
BACKTEST_DIR = os.path.join(DATA_DIR, "backtest")

# 自动创建目录
for d in [DATA_DIR, HISTORY_DIR, REPORT_DIR, BACKTEST_DIR]:
    os.makedirs(d, exist_ok=True)

# ==================== 股票池配置 ====================
# 可以手动指定股票列表，或设为 None 使用自动获取的沪深A股
# 示例: STOCK_POOL = ["000001", "000002", "600000", "600519"]
STOCK_POOL = None  # None = 自动获取全部A股

# 排除ST、科创板(688)、北交所(8/4开头)
EXCLUDE_ST = True
EXCLUDE_KCB = True       # 排除科创板 688xxx
EXCLUDE_BJ = True        # 排除北交所
EXCLUDE_NEW_DAYS = 60    # 排除上市不足N天的新股

# ==================== 数据配置 ====================
# 历史数据
HISTORY_DAYS = 250         # 拉取最近N个交易日的日K
HISTORY_START_DATE = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
HISTORY_END_DATE = datetime.now().strftime("%Y%m%d")

# 周K数据
WEEKLY_ENABLED = True

# ==================== 技术指标参数 ====================
# 均线
MA_PERIODS = [5, 10, 20, 60, 120, 250]

# MACD
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# RSI
RSI_PERIOD = 14

# 布林带
BOLL_PERIOD = 20
BOLL_STD = 2

# 成交量均线
VOL_MA_PERIODS = [5, 10, 20]

# ATR
ATR_PERIOD = 14

# ==================== 选股策略参数 ====================
STRATEGY = {
    # --- 金叉信号 ---
    "ma_cross_fast": 5,          # 快线周期
    "ma_cross_slow": 20,         # 慢线周期
    "ma_cross_days": 3,          # 最近N天内发生金叉

    # --- 放量条件 ---
    "volume_ratio_min": 1.5,     # 量比最低倍数 (今日成交量 / 5日均量)
    "volume_ratio_max": 8.0,     # 量比最高倍数 (排除异常放量)

    # --- 突破前高 ---
    "breakout_lookback": 20,     # 回看N天的最高价
    "breakout_margin": 0.00,     # 突破幅度 (0 = 刚好突破)

    # --- RSI ---
    "rsi_max": 70,               # RSI上限 (不超买)
    "rsi_min": 30,               # RSI下限 (不超卖, 太弱不选)

    # --- 趋势 ---
    "trend_ma": 60,              # 价格需在此均线之上
    "price_min": 3.0,            # 最低股价
    "price_max": 100.0,          # 最高股价

    # --- MACD ---
    "macd_positive": True,       # 要求MACD柱为正

    # --- 打分权重 ---
    "weight_ma_cross": 25,       # 金叉信号权重
    "weight_volume": 20,         # 放量权重
    "weight_breakout": 20,       # 突破权重
    "weight_rsi": 15,            # RSI权重
    "weight_trend": 10,          # 趋势权重
    "weight_macd": 10,           # MACD权重
}

# ==================== 回测配置 ====================
BACKTEST = {
    "initial_capital": 1000000,  # 初始资金 100万
    "commission_rate": 0.0003,   # 佣金费率 万3
    "stamp_tax": 0.001,          # 印花税 千1 (卖出)
    "slippage": 0.002,           # 滑点 0.2%
    "max_hold_days": 10,         # 最大持仓天数
    "stop_loss": -0.05,          # 止损 -5%
    "take_profit": 0.10,         # 止盈 +10%
    "max_positions": 5,          # 最大同时持仓数
    "position_size": 0.2,        # 每只股票仓位比例
    "backtest_months": 6,        # 回测过去N个月
}

# ==================== 盘中刷新配置 ====================
INTRADAY = {
    "refresh_interval": 180,     # 刷新间隔(秒) = 3分钟
    "top_n": 10,                 # 输出前N只候选
    "trading_start": "09:30",
    "trading_end": "15:00",
}

# ==================== 日志配置 ====================
LOG_LEVEL = "INFO"  # DEBUG / INFO / WARNING / ERROR
