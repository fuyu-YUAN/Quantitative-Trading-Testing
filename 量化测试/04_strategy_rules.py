# -*- coding: utf-8 -*-

"""
04_strategy_rules.py
选股规则引擎
- 条件筛选 (硬性条件, 不满足直接排除)
- 打分系统 (软性条件, 打分排序)
- 综合评分排名
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import config
from _03_features import calc_all_features


class StockScorer:
    """股票打分器"""

    def __init__(self, strategy_params=None):
        self.params = strategy_params or config.STRATEGY

    def check_hard_conditions(self, features):
        """
        硬性条件检查 (必须全部满足)
        
        参数:
            features: dict, 最新一天的指标数据
        
        返回:
            (bool, str): (是否通过, 原因)
        """
        p = self.params

        # 1. 价格范围
        price = features.get("close", 0)
        if price < p["price_min"] or price > p["price_max"]:
            return False, f"价格 {price} 不在 [{p['price_min']}, {p['price_max']}] 范围"

        # 2. RSI 不超买
        rsi = features.get("rsi", 50)
        if pd.isna(rsi):
            return False, "RSI 无数据"
        if rsi > p["rsi_max"]:
            return False, f"RSI={rsi:.1f} 超买 (>{p['rsi_max']})"
        if rsi < p["rsi_min"]:
            return False, f"RSI={rsi:.1f} 超卖 (<{p['rsi_min']})"

        # 3. 量比不能太离谱 (排除异常放量)
        vol_ratio = features.get("vol_ratio", 1)
        if pd.notna(vol_ratio) and vol_ratio > p["volume_ratio_max"]:
            return False, f"量比={vol_ratio:.1f} 异常放量 (>{p['volume_ratio_max']})"

        # 4. 日涨跌幅不能太大 (排除涨停/跌停，买不进)
        pct = features.get("pct_change", 0)
        if pd.notna(pct) and abs(pct) > 9.5:
            return False, f"涨跌幅={pct:.1f}% 涨停/跌停"

        # 5. 成交量 > 0
        volume = features.get("volume", 0)
        if volume <= 0:
            return False, "无成交量(停牌)"

        return True, "通过"

    def calc_score(self, df, features):
        """
        软性条件打分
        
        参数:
            df: DataFrame, 完整历史+指标数据
            features: dict, 最新一天的数据
        
        返回:
            dict: {总分, 各项得分明细}
        """
        p = self.params
        scores = {}

        # ========== 1. 均线金叉信号 (25分) ==========
        score_ma_cross = 0
        # 检查最近N天是否出现金叉
        cross_days = p["ma_cross_days"]
        recent = df.tail(cross_days)
        if recent["ma_golden_cross"].sum() > 0:
            score_ma_cross = p["weight_ma_cross"]
        else:
            # 接近金叉也给部分分 (快线接近慢线且向上)
            fast_col = f"ma{p['ma_cross_fast']}"
            slow_col = f"ma{p['ma_cross_slow']}"
            if fast_col in features and slow_col in features:
                fast_val = features[fast_col]
                slow_val = features[slow_col]
                if pd.notna(fast_val) and pd.notna(slow_val) and slow_val > 0:
                    gap = (fast_val - slow_val) / slow_val
                    if 0 < gap < 0.02:  # 刚刚金叉,差距<2%
                        score_ma_cross = p["weight_ma_cross"] * 0.7
                    elif -0.01 < gap <= 0:  # 即将金叉
                        score_ma_cross = p["weight_ma_cross"] * 0.3

        scores["ma_cross"] = round(score_ma_cross, 1)

        # ========== 2. 放量信号 (20分) ==========
        score_volume = 0
        vol_ratio = features.get("vol_ratio", 1)
        if pd.notna(vol_ratio):
            if vol_ratio >= p["volume_ratio_min"]:
                # 量比越大分越高,但有上限
                ratio_score = min(vol_ratio / p["volume_ratio_min"], 2.0) / 2.0
                score_volume = p["weight_volume"] * ratio_score
            elif vol_ratio >= 1.0:
                # 温和放量也给部分分
                score_volume = p["weight_volume"] * 0.3

        scores["volume"] = round(score_volume, 1)

        # ========== 3. 突破前高 (20分) ==========
        score_breakout = 0
        breakout = features.get("breakout", 0)
        if breakout == 1:
            score_breakout = p["weight_breakout"]
        else:
            # 接近前高也给分
            close = features.get("close", 0)
            prev_high = features.get("prev_high", 0)
            if pd.notna(prev_high) and prev_high > 0 and close > 0:
                ratio = close / prev_high
                if ratio >= 0.97:  # 接近前高 (差距<3%)
                    score_breakout = p["weight_breakout"] * 0.5

        scores["breakout"] = round(score_breakout, 1)

        # ========== 4. RSI 健康度 (15分) ==========
        score_rsi = 0
        rsi = features.get("rsi", 50)
        if pd.notna(rsi):
            # RSI 在 40-60 最佳 (不超买不超卖,有上涨空间)
            if 40 <= rsi <= 60:
                score_rsi = p["weight_rsi"]
            elif 30 <= rsi < 40:
                score_rsi = p["weight_rsi"] * 0.7
            elif 60 < rsi <= 70:
                score_rsi = p["weight_rsi"] * 0.5
            else:
                score_rsi = 0

        scores["rsi"] = round(score_rsi, 1)

        # ========== 5. 趋势 (10分) ==========
        score_trend = 0
        trend_ma = f"ma{p['trend_ma']}"
        if trend_ma in features:
            ma_val = features[trend_ma]
            close = features.get("close", 0)
            if pd.notna(ma_val) and ma_val > 0 and close > 0:
                if close > ma_val:
                    # 在趋势线之上
                    score_trend = p["weight_trend"]
                elif close > ma_val * 0.97:
                    # 接近趋势线
                    score_trend = p["weight_trend"] * 0.5

        scores["trend"] = round(score_trend, 1)

        # ========== 6. MACD (10分) ==========
        score_macd = 0
        macd_hist = features.get("macd_hist", 0)
        macd_dif = features.get("macd_dif", 0)
        if pd.notna(macd_hist) and pd.notna(macd_dif):
            if p["macd_positive"] and macd_hist > 0:
                score_macd = p["weight_macd"]
            elif macd_hist > 0:
                score_macd = p["weight_macd"]

            # MACD 柱递增 (动能增强)
            if len(df) >= 2:
                prev_hist = df["macd_hist"].iloc[-2]
                if pd.notna(prev_hist) and macd_hist > prev_hist:
                    score_macd = min(score_macd * 1.2, p["weight_macd"])

        scores["macd"] = round(score_macd, 1)

        # ========== 总分 ==========
        total = sum(scores.values())
        scores["total"] = round(total, 1)

        return scores


def screen_stocks(stock_data_dict, top_n=20):
    """
    对所有股票进行筛选和打分
    
    参数:
        stock_data_dict: dict, {code: DataFrame(日K+指标)}
        top_n: 返回前N只
    
    返回:
        DataFrame: 排序后的股票清单
    """
    scorer = StockScorer()
    results = []

    for code, df in stock_data_dict.items():
        if df.empty or len(df) < 60:
            continue

        # 计算指标
        df = calc_all_features(df)
        features = df.iloc[-1].to_dict()

        # 硬性条件检查
        passed, reason = scorer.check_hard_conditions(features)
        if not passed:
            continue

        # 打分
        scores = scorer.calc_score(df, features)

        # 组装结果
        result = {
            "code": code,
            "name": features.get("name", ""),
            "close": features.get("close", 0),
            "pct_change": features.get("pct_change", 0),
            "volume": features.get("volume", 0),
            "vol_ratio": features.get("vol_ratio", 0),
            "rsi": features.get("rsi", 0),
            "macd_hist": features.get("macd_hist", 0),
            "ma5": features.get("ma5", 0),
            "ma20": features.get("ma20", 0),
            "ma60": features.get("ma60", 0),
            "golden_cross": features.get("ma_golden_cross", 0),
            "breakout": features.get("breakout", 0),
        }
        result.update({f"score_{k}": v for k, v in scores.items()})
        results.append(result)

    if not results:
        print("[WARN] 没有股票通过筛选")
        return pd.DataFrame()

    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values("score_total", ascending=False).reset_index(drop=True)

    print(f"[INFO] 筛选完成: {len(result_df)} 只股票通过, 返回前 {top_n} 只")

    return result_df.head(top_n)


def quick_screen_with_realtime(realtime_df, history_data_dict, top_n=10):
    """
    盘中快速筛选: 结合实时数据和历史数据
    
    参数:
        realtime_df: 实时行情 DataFrame
        history_data_dict: {code: 历史日K DataFrame}
        top_n: 返回数量
    """
    scorer = StockScorer()
    results = []

    for _, row in realtime_df.iterrows():
        code = row["code"]

        if code not in history_data_dict:
            continue

        hist_df = history_data_dict[code].copy()
        if hist_df.empty or len(hist_df) < 60:
            continue

        # 将今日实时数据追加到历史数据末尾
        today_row = {
            "date": pd.Timestamp(datetime.now().date()),
            "open": row.get("open", row["price"]),
            "close": row["price"],
            "high": row.get("high", row["price"]),
            "low": row.get("low", row["price"]),
            "volume": row.get("volume", 0),
            "amount": row.get("amount", 0),
            "pct_change": row.get("pct_change", 0),
            "code": code,
        }

        # 如果今天的日期已经在历史数据中，替换；否则追加
        today_date = pd.Timestamp(datetime.now().date())
        if today_date in hist_df["date"].values:
            idx = hist_df[hist_df["date"] == today_date].index[0]
            for k, v in today_row.items():
                hist_df.loc[idx, k] = v
        else:
            hist_df = pd.concat([hist_df, pd.DataFrame([today_row])], ignore_index=True)

        # 计算指标
        hist_df = calc_all_features(hist_df)
        features = hist_df.iloc[-1].to_dict()
        features["name"] = row.get("name", "")

        # 筛选
        passed, reason = scorer.check_hard_conditions(features)
        if not passed:
            continue

        scores = scorer.calc_score(hist_df, features)

        result = {
            "code": code,
            "name": row.get("name", ""),
            "price": row["price"],
            "pct_change": row.get("pct_change", 0),
            "vol_ratio_rt": row.get("volume_ratio", 0),  # 实时量比
            "rsi": features.get("rsi", 0),
            "score_total": scores["total"],
            "golden_cross": features.get("ma_golden_cross", 0),
            "breakout": features.get("breakout", 0),
        }
        results.append(result)

    if not results:
        return pd.DataFrame()

    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values("score_total", ascending=False).reset_index(drop=True)

    return result_df.head(top_n)


# ==================== 测试入口 ====================
if __name__ == "__main__":
    from _01_data_history import load_kline

    # 加载几只测试股票
    test_codes = ["000001", "600519", "000858", "002415", "601318"]
    data_dict = {}
    for code in test_codes:
        df = load_kline(code, "daily")
        if not df.empty:
            data_dict[code] = df

    if not data_dict:
        print("[WARN] 没有本地数据, 请先运行 01_data_history.py")
    else:
        print(f"加载了 {len(data_dict)} 只股票数据")
        result = screen_stocks(data_dict, top_n=10)
        if not result.empty:
            show_cols = ["code", "close", "rsi", "vol_ratio",
                         "golden_cross", "breakout", "score_total"]
            cols = [c for c in show_cols if c in result.columns]
            print("\n=== 选股结果 ===")
            print(result[cols].to_string(index=False))