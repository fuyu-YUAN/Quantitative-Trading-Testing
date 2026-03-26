# -*- coding: utf-8 -*-
"""
05_backtest.py
回测引擎
- 基于历史数据验证选股策略
- 计算收益率、胜率、最大回撤等指标
- 支持止盈止损
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict

import config
from _03_features import calc_all_features
from _04_strategy_rules import StockScorer


class BacktestEngine:
    """回测引擎"""

    def __init__(self, bt_config=None):
        self.config = bt_config or config.BACKTEST
        self.scorer = StockScorer()
        self.trades = []
        self.daily_equity = []

    def run_backtest(self, stock_data_dict, start_date=None, end_date=None):
        """
        运行回测
        
        参数:
            stock_data_dict: {code: DataFrame(日K数据)}
            start_date: 回测开始日期
            end_date: 回测结束日期
        
        返回:
            dict: 回测结果统计
        """
        if start_date is None:
            start_date = datetime.now() - timedelta(days=30 * self.config["backtest_months"])
        if end_date is None:
            end_date = datetime.now()

        if isinstance(start_date, str):
            start_date = pd.Timestamp(start_date)
        if isinstance(end_date, str):
            end_date = pd.Timestamp(end_date)

        print(f"\n{'='*60}")
        print(f"[回测] 区间: {start_date.date()} ~ {end_date.date()}")
        print(f"[回测] 初始资金: {self.config['initial_capital']:,.0f}")
        print(f"[回测] 股票池: {len(stock_data_dict)} 只")
        print(f"{'='*60}")

        # 1. 计算所有股票的指标
        featured_data = {}
        for code, df in stock_data_dict.items():
            if len(df) < 60:
                continue
            featured_df = calc_all_features(df.copy())
            featured_df = featured_df[(featured_df["date"] >= start_date - timedelta(days=5)) &
                                      (featured_df["date"] <= end_date)]
            if len(featured_df) > 30:
                featured_data[code] = featured_df

        if not featured_data:
            print("[ERROR] 没有足够数据进行回测")
            return {}

        # 2. 获取所有交易日
        all_dates = set()
        for df in featured_data.values():
            all_dates.update(df["date"].tolist())
        trading_dates = sorted([d for d in all_dates if start_date <= d <= end_date])

        if not trading_dates:
            print("[ERROR] 回测区间内没有交易日")
            return {}

        # 3. 逐日模拟
        capital = self.config["initial_capital"]
        positions = {}  # {code: {buy_price, buy_date, shares, cost}}
        self.trades = []
        self.daily_equity = []

        for date in trading_dates:
            # 3a. 检查现有持仓 — 是否需要卖出
            codes_to_sell = []
            for code, pos in positions.items():
                if code not in featured_data:
                    continue

                df = featured_data[code]
                day_data = df[df["date"] == date]
                if day_data.empty:
                    continue

                current_price = day_data["close"].values[0]
                buy_price = pos["buy_price"]
                hold_days = (date - pos["buy_date"]).days
                pct_return = (current_price - buy_price) / buy_price

                sell = False
                sell_reason = ""

                # 止损
                if pct_return <= self.config["stop_loss"]:
                    sell = True
                    sell_reason = f"止损 ({pct_return:.1%})"

                # 止盈
                elif pct_return >= self.config["take_profit"]:
                    sell = True
                    sell_reason = f"止盈 ({pct_return:.1%})"

                # 持仓超时
                elif hold_days >= self.config["max_hold_days"]:
                    sell = True
                    sell_reason = f"超时 ({hold_days}天)"

                if sell:
                    # 计算卖出收益
                    sell_amount = current_price * pos["shares"]
                    commission = sell_amount * self.config["commission_rate"]
                    stamp_tax = sell_amount * self.config["stamp_tax"]
                    slippage_cost = sell_amount * self.config["slippage"]
                    net_amount = sell_amount - commission - stamp_tax - slippage_cost

                    profit = net_amount - pos["cost"]
                    capital += net_amount

                    self.trades.append({
                        "code": code,
                        "buy_date": pos["buy_date"],
                        "buy_price": buy_price,
                        "sell_date": date,
                        "sell_price": current_price,
                        "shares": pos["shares"],
                        "profit": round(profit, 2),
                        "return": round(pct_return * 100, 2),
                        "hold_days": hold_days,
                        "reason": sell_reason,
                    })

                    codes_to_sell.append(code)

            for code in codes_to_sell:
                del positions[code]

            # 3b. 寻找买入信号
            if len(positions) < self.config["max_positions"]:
                candidates = []
                for code, df in featured_data.items():
                    if code in positions:
                        continue

                    day_data = df[df["date"] == date]
                    if day_data.empty:
                        continue

                    features = day_data.iloc[0].to_dict()

                    # 检查硬性条件
                    passed, _ = self.scorer.check_hard_conditions(features)
                    if not passed:
                        continue

                    # 截取到当天的数据来打分
                    df_to_date = df[df["date"] <= date]
                    scores = self.scorer.calc_score(df_to_date, features)

                    if scores["total"] >= 50:  # 总分超过50才买
                        candidates.append({
                            "code": code,
                            "score": scores["total"],
                            "price": features["close"],
                            "date": date,
                        })

                # 按得分排序,选最高的
                candidates.sort(key=lambda x: x["score"], reverse=True)
                available_slots = self.config["max_positions"] - len(positions)

                for cand in candidates[:available_slots]:
                    # 计算买入
                    position_capital = capital * self.config["position_size"]
                    price = cand["price"]
                    slippage_price = price * (1 + self.config["slippage"])
                    shares = int(position_capital / (slippage_price * 100)) * 100  # 整手

                    if shares <= 0:
                        continue

                    cost = shares * slippage_price
                    commission = cost * self.config["commission_rate"]
                    total_cost = cost + commission

                    if total_cost > capital:
                        continue

                    capital -= total_cost
                    positions[cand["code"]] = {
                        "buy_price": price,
                        "buy_date": date,
                        "shares": shares,
                        "cost": total_cost,
                    }

            # 3c. 记录当日净值
            total_value = capital
            for code, pos in positions.items():
                if code in featured_data:
                    df = featured_data[code]
                    day_data = df[df["date"] == date]
                    if not day_data.empty:
                        current_price = day_data["close"].values[0]
                        total_value += current_price * pos["shares"]
                    else:
                        total_value += pos["buy_price"] * pos["shares"]

            self.daily_equity.append({
                "date": date,
                "equity": round(total_value, 2),
                "cash": round(capital, 2),
                "positions": len(positions),
            })

        # 4. 计算回测统计
        stats = self._calc_statistics()
        return stats

    def _calc_statistics(self):
        """计算回测统计指标"""
        if not self.trades:
            return {"total_trades": 0, "message": "无交易记录"}

        trades_df = pd.DataFrame(self.trades)
        equity_df = pd.DataFrame(self.daily_equity)

        initial_capital = self.config["initial_capital"]
        final_equity = equity_df["equity"].iloc[-1] if not equity_df.empty else initial_capital

        # 收益率
        total_return = (final_equity - initial_capital) / initial_capital

        # 胜率
        wins = len(trades_df[trades_df["profit"] > 0])
        losses = len(trades_df[trades_df["profit"] <= 0])
        win_rate = wins / len(trades_df) if len(trades_df) > 0 else 0

        # 盈亏比
        avg_win = trades_df[trades_df["profit"] > 0]["profit"].mean() if wins > 0 else 0
        avg_loss = abs(trades_df[trades_df["profit"] <= 0]["profit"].mean()) if losses > 0 else 1
        profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else float("inf")

        # 最大回撤
        max_drawdown = 0
        if not equity_df.empty:
            peak = equity_df["equity"].expanding().max()
            drawdown = (equity_df["equity"] - peak) / peak
            max_drawdown = drawdown.min()

        # 年化收益
        if not equity_df.empty and len(equity_df) > 1:
            days = (equity_df["date"].iloc[-1] - equity_df["date"].iloc[0]).days
            if days > 0:
                annual_return = (1 + total_return) ** (365 / days) - 1
            else:
                annual_return = 0
        else:
            annual_return = 0

        # Sharpe (简化版)
        if not equity_df.empty:
            equity_df["daily_return"] = equity_df["equity"].pct_change()
            daily_std = equity_df["daily_return"].std()
            daily_mean = equity_df["daily_return"].mean()
            sharpe = (daily_mean * 252 - 0.03) / (daily_std * np.sqrt(252)) if daily_std > 0 else 0
        else:
            sharpe = 0

        stats = {
            "initial_capital": initial_capital,
            "final_equity": round(final_equity, 2),
            "total_return": round(total_return * 100, 2),        # %
            "annual_return": round(annual_return * 100, 2),       # %
            "max_drawdown": round(max_drawdown * 100, 2),         # %
            "sharpe_ratio": round(sharpe, 2),
            "total_trades": len(trades_df),
            "win_trades": wins,
            "loss_trades": losses,
            "win_rate": round(win_rate * 100, 2),                 # %
            "avg_profit": round(trades_df["profit"].mean(), 2),
            "avg_return": round(trades_df["return"].mean(), 2),   # %
            "avg_hold_days": round(trades_df["hold_days"].mean(), 1),
            "profit_loss_ratio": round(profit_loss_ratio, 2),
            "max_profit": round(trades_df["profit"].max(), 2),
            "max_loss": round(trades_df["profit"].min(), 2),
            "total_profit": round(trades_df["profit"].sum(), 2),
        }

        return stats

    def get_trades_df(self):
        """获取交易明细"""
        return pd.DataFrame(self.trades)

    def get_equity_df(self):
        """获取净值曲线"""
        return pd.DataFrame(self.daily_equity)

    def print_report(self, stats):
        """打印回测报告"""
        if not stats or stats.get("total_trades", 0) == 0:
            print("[回测] 无交易发生")
            return

        print(f"\n{'='*50}")
        print(f"           回 测 报 告")
        print(f"{'='*50}")
        print(f"  初始资金:      {stats['initial_capital']:>12,.0f}")
        print(f"  最终净值:      {stats['final_equity']:>12,.0f}")
        print(f"  总收益:        {stats['total_profit']:>12,.0f}")
        print(f"  总收益率:      {stats['total_return']:>11.2f}%")
        print(f"  年化收益率:    {stats['annual_return']:>11.2f}%")
        print(f"  最大回撤:      {stats['max_drawdown']:>11.2f}%")
        print(f"  夏普比率:      {stats['sharpe_ratio']:>12.2f}")
        print(f"{'='*50}")
        print(f"  总交易次数:    {stats['total_trades']:>12d}")
        print(f"  盈利次数:      {stats['win_trades']:>12d}")
        print(f"  亏损次数:      {stats['loss_trades']:>12d}")
        print(f"  胜率:          {stats['win_rate']:>11.2f}%")
        print(f"  盈亏比:        {stats['profit_loss_ratio']:>12.2f}")
        print(f"  平均收益:      {stats['avg_profit']:>12,.0f}")
        print(f"  平均收益率:    {stats['avg_return']:>11.2f}%")
        print(f"  平均持仓天数:  {stats['avg_hold_days']:>12.1f}")
        print(f"  最大单笔盈利:  {stats['max_profit']:>12,.0f}")
        print(f"  最大单笔亏损:  {stats['max_loss']:>12,.0f}")
        print(f"{'='*50}")


def run_full_backtest(stock_data_dict):
    """
    便捷函数: 执行完整回测
    """
    engine = BacktestEngine()
    stats = engine.run_backtest(stock_data_dict)
    engine.print_report(stats)

    # 显示最近交易
    trades_df = engine.get_trades_df()
    if not trades_df.empty:
        print(f"\n最近10笔交易:")
        show_cols = ["code", "buy_date", "buy_price", "sell_date", "sell_price",
                     "profit", "return", "hold_days", "reason"]
        cols = [c for c in show_cols if c in trades_df.columns]
        print(trades_df[cols].tail(10).to_string(index=False))

    return engine


# ==================== 测试入口 ====================
if __name__ == "__main__":
    from _01_data_history import load_kline, get_stock_pool

    print("=== 加载本地数据 ===")
    stock_pool = pd.read_csv(
        os.path.join(config.DATA_DIR, "stock_pool.csv"), encoding="utf-8-sig"
    ) if os.path.exists(os.path.join(config.DATA_DIR, "stock_pool.csv")) else None

    if stock_pool is None:
        test_codes = ["000001", "600519", "000858", "002415", "601318",
                       "000333", "600036", "002594", "601012", "000568"]
    else:
        test_codes = stock_pool["code"].tolist()[:50]  # 回测前50只

    data_dict = {}
    for code in test_codes:
        df = load_kline(code, "daily")
        if not df.empty and len(df) >= 100:
            data_dict[code] = df

    print(f"加载了 {len(data_dict)} 只股票")

    if data_dict:
        engine = run_full_backtest(data_dict)