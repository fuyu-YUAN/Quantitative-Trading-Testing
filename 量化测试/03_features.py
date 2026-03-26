# -*- coding: utf-8 -*-

"""
03_features.py
技术指标计算引擎
- 均线 MA
- MACD
- RSI
- 布林带 BOLL
- 成交量特征
- ATR
- 金叉/死叉信号检测
"""
import pandas as pd
import numpy as np

import config


def calc_ma(df, periods=None):
    """
    计算多周期移动平均线
    """
    if periods is None:
        periods = config.MA_PERIODS

    for p in periods:
        df[f"ma{p}"] = df["close"].rolling(window=p, min_periods=1).mean().round(3)

    return df


def calc_ema(series, period):
    """
    计算指数移动平均
    """
    return series.ewm(span=period, adjust=False).mean()


def calc_macd(df, fast=None, slow=None, signal=None):
    """
    计算MACD指标
    - DIF = EMA(fast) - EMA(slow)
    - DEA = EMA(DIF, signal)
    - MACD柱 = 2 * (DIF - DEA)
    """
    if fast is None:
        fast = config.MACD_FAST
    if slow is None:
        slow = config.MACD_SLOW
    if signal is None:
        signal = config.MACD_SIGNAL

    ema_fast = calc_ema(df["close"], fast)
    ema_slow = calc_ema(df["close"], slow)

    df["macd_dif"] = (ema_fast - ema_slow).round(3)
    df["macd_dea"] = calc_ema(df["macd_dif"], signal).round(3)
    df["macd_hist"] = (2 * (df["macd_dif"] - df["macd_dea"])).round(3)

    return df


def calc_rsi(df, period=None):
    """
    计算RSI指标
    """
    if period is None:
        period = config.RSI_PERIOD

    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)

    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()

    rs = avg_gain / avg_loss
    df["rsi"] = (100 - 100 / (1 + rs)).round(2)

    return df


def calc_boll(df, period=None, std_dev=None):
    """
    计算布林带
    """
    if period is None:
        period = config.BOLL_PERIOD
    if std_dev is None:
        std_dev = config.BOLL_STD

    df["boll_mid"] = df["close"].rolling(window=period).mean().round(3)
    rolling_std = df["close"].rolling(window=period).std()
    df["boll_upper"] = (df["boll_mid"] + std_dev * rolling_std).round(3)
    df["boll_lower"] = (df["boll_mid"] - std_dev * rolling_std).round(3)

    return df


def calc_volume_features(df, periods=None):
    """
    计算成交量特征
    - 成交量均线
    - 量比 (当日量 / N日均量)
    """
    if periods is None:
        periods = config.VOL_MA_PERIODS

    for p in periods:
        df[f"vol_ma{p}"] = df["volume"].rolling(window=p, min_periods=1).mean().round(0)

    # 量比: 今日成交量 / 5日平均成交量
    df["vol_ratio"] = (df["volume"] / df["vol_ma5"].replace(0, np.nan)).round(2)

    return df


def calc_atr(df, period=None):
    """
    计算ATR (真实波幅均值)
    """
    if period is None:
        period = config.ATR_PERIOD

    high = df["high"]
    low = df["low"]
    close_prev = df["close"].shift(1)

    tr1 = high - low
    tr2 = (high - close_prev).abs()
    tr3 = (low - close_prev).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr"] = tr.rolling(window=period, min_periods=1).mean().round(3)

    return df


def detect_ma_cross(df, fast_period=None, slow_period=None):
    """
    检测均线金叉/死叉信号
    
    返回新增列:
        ma_golden_cross: 金叉 (快线上穿慢线), 1 = 是, 0 = 否
        ma_death_cross: 死叉 (快线下穿慢线)
    """
    if fast_period is None:
        fast_period = config.STRATEGY["ma_cross_fast"]
    if slow_period is None:
        slow_period = config.STRATEGY["ma_cross_slow"]

    fast_col = f"ma{fast_period}"
    slow_col = f"ma{slow_period}"

    # 确保均线已计算
    if fast_col not in df.columns:
        df[fast_col] = df["close"].rolling(window=fast_period, min_periods=1).mean()
    if slow_col not in df.columns:
        df[slow_col] = df["close"].rolling(window=slow_period, min_periods=1).mean()

    # 金叉: 前一天 fast < slow, 今天 fast >= slow
    prev_fast = df[fast_col].shift(1)
    prev_slow = df[slow_col].shift(1)

    df["ma_golden_cross"] = ((prev_fast < prev_slow) & (df[fast_col] >= df[slow_col])).astype(int)
    df["ma_death_cross"] = ((prev_fast > prev_slow) & (df[fast_col] <= df[slow_col])).astype(int)

    return df


def detect_breakout(df, lookback=None):
    """
    检测突破前期高点信号
    
    参数:
        lookback: 回看天数
    """
    if lookback is None:
        lookback = config.STRATEGY["breakout_lookback"]

    # 前N天的最高价 (不包含当天)
    df["prev_high"] = df["high"].shift(1).rolling(window=lookback, min_periods=5).max()

    # 突破信号: 今天收盘价 > 前N天最高价
    margin = config.STRATEGY["breakout_margin"]
    df["breakout"] = (df["close"] > df["prev_high"] * (1 + margin)).astype(int)

    return df


def calc_all_features(df):
    """
    一键计算所有技术指标
    
    参数:
        df: 必须包含 date, open, close, high, low, volume 列
    
    返回:
        DataFrame: 增加了所有指标列
    """
    if df.empty or len(df) < 30:
        return df

    # 确保按日期排序
    df = df.sort_values("date").reset_index(drop=True)

    # 计算各项指标
    df = calc_ma(df)
    df = calc_macd(df)
    df = calc_rsi(df)
    df = calc_boll(df)
    df = calc_volume_features(df)
    df = calc_atr(df)
    df = detect_ma_cross(df)
    df = detect_breakout(df)

    return df


def get_latest_features(df):
    """
    获取最新一天的所有特征值 (字典形式)
    """
    if df.empty:
        return {}

    featured = calc_all_features(df)
    if featured.empty:
        return {}

    return featured.iloc[-1].to_dict()


# ==================== 测试入口 ====================
if __name__ == "__main__":
    from _01_data_history import load_kline

    # 加载测试数据
    df = load_kline("000001", "daily")
    if df.empty:
        print("[WARN] 没有本地数据, 请先运行 01_data_history.py 下载数据")
    else:
        print(f"原始数据: {len(df)} 条, 列: {list(df.columns)}")

        # 计算所有指标
        df = calc_all_features(df)
        print(f"\n计算后列: {list(df.columns)}")

        # 显示最近5天
        show_cols = ["date", "close", "ma5", "ma20", "ma60",
                     "macd_dif", "macd_dea", "macd_hist",
                     "rsi", "vol_ratio", "ma_golden_cross", "breakout"]
        print(f"\n最近5天指标:")
        print(df[show_cols].tail(5).to_string(index=False))

        # 最新特征
        latest = get_latest_features(df)
        print(f"\n最新日期: {latest.get('date', 'N/A')}")
        print(f"收盘价: {latest.get('close', 'N/A')}")
        print(f"MA5: {latest.get('ma5', 'N/A')}")
        print(f"MA20: {latest.get('ma20', 'N/A')}")
        print(f"RSI: {latest.get('rsi', 'N/A')}")
        print(f"MACD柱: {latest.get('macd_hist', 'N/A')}")
        print(f"量比: {latest.get('vol_ratio', 'N/A')}")
        print(f"金叉: {latest.get('ma_golden_cross', 'N/A')}")
        print(f"突破: {latest.get('breakout', 'N/A')}")