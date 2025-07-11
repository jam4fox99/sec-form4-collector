#!/usr/bin/env python3
"""
Insider Performance Analysis System
Calculates PnL for each insider and ranks them by trading performance
"""
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal
import yfinance as yf
from sqlalchemy import text

from ..database.db_manager import get_db_manager
from ..database.models import *

logger = logging.getLogger(__name__)

@dataclass
class InsiderTrade:
    """Represents a single insider trade"""
    trade_id: int
    insider_name: str
    insider_cik: str
    company_ticker: str
    company_name: str
    transaction_date: date
    transaction_code: str  # P=Purchase, S=Sale, D=Disposition, etc.
    shares: Decimal
    price_per_share: Decimal
    total_value: Decimal
    is_buy: bool
    is_sell: bool

@dataclass
class InsiderPosition:
    """Represents an insider's position in a stock"""
    insider_name: str
    insider_cik: str
    company_ticker: str
    total_shares: Decimal
    avg_cost_basis: Decimal
    current_price: Decimal
    market_value: Decimal
    unrealized_pnl: Decimal
    cost_basis: Decimal

@dataclass
class InsiderPerformanceMetrics:
    """Performance metrics for an insider"""
    insider_name: str
    insider_cik: str
    total_trades: int
    total_buy_value: Decimal
    total_sell_value: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    total_pnl: Decimal
    win_rate: float
    avg_holding_period: Optional[int]
    best_trade: Decimal
    worst_trade: Decimal
    sharpe_ratio: Optional[float]
    total_return_pct: float

class InsiderPerformanceAnalyzer:
    """Analyzes insider trading performance"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.db_manager = get_db_manager(config_path)
        self.current_prices = {}  # Cache for current stock prices
        
    def get_all_insider_trades(self) -> List[InsiderTrade]:
        """Get all insider trades from the database"""
        with self.db_manager.get_session() as session:
            trades = session.execute(text('''
                SELECT 
                    t.id,
                    COALESCE(i.name, f.reporting_owner_name) as insider_name,
                    COALESCE(i.cik, f.reporting_owner_cik) as insider_cik,
                    c.ticker,
                    c.name as company_name,
                    t.transaction_date,
                    t.transaction_code,
                    t.shares,
                    t.price_per_share,
                    t.total_value
                FROM transactions t
                JOIN form4_filings f ON t.filing_id = f.id
                LEFT JOIN insiders i ON f.insider_id = i.id
                LEFT JOIN companies c ON f.company_cik = c.cik
                WHERE t.price_per_share > 0  -- Only trades with actual prices
                ORDER BY t.transaction_date
            ''')).fetchall()
            
            insider_trades = []
            for trade in trades:
                # Classify buy/sell transactions
                is_buy = trade[6] in ['P', 'M', 'A']  # Purchase, Exercise, Award
                is_sell = trade[6] in ['S', 'D', 'F']  # Sale, Disposition, Tax withholding
                
                insider_trades.append(InsiderTrade(
                    trade_id=trade[0],
                    insider_name=trade[1] or "Unknown",
                    insider_cik=trade[2] or "Unknown",
                    company_ticker=trade[3] or "Unknown",
                    company_name=trade[4] or "Unknown",
                    transaction_date=trade[5],
                    transaction_code=trade[6],
                    shares=Decimal(str(trade[7])) if trade[7] else Decimal('0'),
                    price_per_share=Decimal(str(trade[8])) if trade[8] else Decimal('0'),
                    total_value=Decimal(str(trade[9])) if trade[9] else Decimal('0'),
                    is_buy=is_buy,
                    is_sell=is_sell
                ))
            
            return insider_trades
    
    def get_current_price(self, ticker: str) -> Optional[Decimal]:
        """Get current stock price using yfinance"""
        if ticker in self.current_prices:
            return self.current_prices[ticker]
        
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d")
            if not hist.empty:
                current_price = Decimal(str(hist['Close'].iloc[-1]))
                self.current_prices[ticker] = current_price
                return current_price
        except Exception as e:
            logger.warning(f"Could not get price for {ticker}: {e}")
        
        return None
    
    def calculate_insider_positions(self, trades: List[InsiderTrade]) -> Dict[str, Dict[str, InsiderPosition]]:
        """Calculate current positions for each insider"""
        positions = {}  # {insider_cik: {ticker: InsiderPosition}}
        
        for trade in trades:
            insider_key = trade.insider_cik
            ticker = trade.company_ticker
            
            if insider_key not in positions:
                positions[insider_key] = {}
            
            if ticker not in positions[insider_key]:
                positions[insider_key][ticker] = InsiderPosition(
                    insider_name=trade.insider_name,
                    insider_cik=trade.insider_cik,
                    company_ticker=ticker,
                    total_shares=Decimal('0'),
                    avg_cost_basis=Decimal('0'),
                    current_price=Decimal('0'),
                    market_value=Decimal('0'),
                    unrealized_pnl=Decimal('0'),
                    cost_basis=Decimal('0')
                )
            
            position = positions[insider_key][ticker]
            
            if trade.is_buy:
                # Add to position
                old_cost_basis = position.cost_basis
                new_shares = trade.shares
                new_cost = trade.total_value
                
                position.total_shares += new_shares
                position.cost_basis += new_cost
                
                if position.total_shares > 0:
                    position.avg_cost_basis = position.cost_basis / position.total_shares
            
            elif trade.is_sell:
                # Reduce position
                position.total_shares -= trade.shares
                if position.total_shares > 0:
                    # Reduce cost basis proportionally
                    proportion_sold = trade.shares / (position.total_shares + trade.shares)
                    position.cost_basis -= position.cost_basis * proportion_sold
                else:
                    # Position closed
                    position.cost_basis = Decimal('0')
                    position.avg_cost_basis = Decimal('0')
        
        # Calculate current market values and unrealized PnL
        for insider_positions in positions.values():
            for position in insider_positions.values():
                if position.total_shares > 0:
                    current_price = self.get_current_price(position.company_ticker)
                    if current_price:
                        position.current_price = current_price
                        position.market_value = position.total_shares * current_price
                        position.unrealized_pnl = position.market_value - position.cost_basis
        
        return positions
    
    def calculate_realized_pnl(self, trades: List[InsiderTrade]) -> Dict[str, Decimal]:
        """Calculate realized PnL for each insider using FIFO method"""
        realized_pnl = {}  # {insider_cik: total_realized_pnl}
        
        # Group trades by insider and ticker
        insider_trades = {}  # {insider_cik: {ticker: [trades]}}
        
        for trade in trades:
            insider_key = trade.insider_cik
            ticker = trade.company_ticker
            
            if insider_key not in insider_trades:
                insider_trades[insider_key] = {}
            if ticker not in insider_trades[insider_key]:
                insider_trades[insider_key][ticker] = []
            
            insider_trades[insider_key][ticker].append(trade)
        
        # Calculate realized PnL for each insider/ticker combination
        for insider_cik, ticker_trades in insider_trades.items():
            if insider_cik not in realized_pnl:
                realized_pnl[insider_cik] = Decimal('0')
            
            for ticker, trades in ticker_trades.items():
                # Sort trades by date
                trades.sort(key=lambda x: x.transaction_date)
                
                # FIFO calculation
                buy_queue = []  # [(shares, price)]
                
                for trade in trades:
                    if trade.is_buy:
                        buy_queue.append((trade.shares, trade.price_per_share))
                    
                    elif trade.is_sell:
                        shares_to_sell = trade.shares
                        sell_price = trade.price_per_share
                        
                        while shares_to_sell > 0 and buy_queue:
                            buy_shares, buy_price = buy_queue[0]
                            
                            if buy_shares <= shares_to_sell:
                                # Use entire buy lot
                                pnl = buy_shares * (sell_price - buy_price)
                                realized_pnl[insider_cik] += pnl
                                shares_to_sell -= buy_shares
                                buy_queue.pop(0)
                            else:
                                # Partial buy lot
                                pnl = shares_to_sell * (sell_price - buy_price)
                                realized_pnl[insider_cik] += pnl
                                buy_queue[0] = (buy_shares - shares_to_sell, buy_price)
                                shares_to_sell = Decimal('0')
        
        return realized_pnl
    
    def calculate_performance_metrics(self) -> List[InsiderPerformanceMetrics]:
        """Calculate comprehensive performance metrics for all insiders"""
        trades = self.get_all_insider_trades()
        positions = self.calculate_insider_positions(trades)
        realized_pnl = self.calculate_realized_pnl(trades)
        
        # Group trades by insider
        insider_trades = {}
        for trade in trades:
            if trade.insider_cik not in insider_trades:
                insider_trades[trade.insider_cik] = []
            insider_trades[trade.insider_cik].append(trade)
        
        performance_metrics = []
        
        for insider_cik, trades in insider_trades.items():
            insider_name = trades[0].insider_name
            
            # Calculate basic metrics
            total_trades = len(trades)
            total_buy_value = sum(trade.total_value for trade in trades if trade.is_buy)
            total_sell_value = sum(trade.total_value for trade in trades if trade.is_sell)
            
            # Get unrealized PnL from positions
            unrealized_pnl = Decimal('0')
            if insider_cik in positions:
                for position in positions[insider_cik].values():
                    unrealized_pnl += position.unrealized_pnl
            
            # Get realized PnL
            realized_pnl_value = realized_pnl.get(insider_cik, Decimal('0'))
            total_pnl = realized_pnl_value + unrealized_pnl
            
            # Calculate return percentage
            total_return_pct = float(total_pnl / total_buy_value * 100) if total_buy_value > 0 else 0
            
            # Calculate win rate (simplified)
            winning_trades = sum(1 for trade in trades if trade.is_sell and trade.total_value > 0)
            total_sell_trades = sum(1 for trade in trades if trade.is_sell)
            win_rate = winning_trades / total_sell_trades if total_sell_trades > 0 else 0
            
            # Best and worst trades (simplified)
            trade_values = [trade.total_value for trade in trades]
            best_trade = max(trade_values) if trade_values else Decimal('0')
            worst_trade = min(trade_values) if trade_values else Decimal('0')
            
            performance_metrics.append(InsiderPerformanceMetrics(
                insider_name=insider_name,
                insider_cik=insider_cik,
                total_trades=total_trades,
                total_buy_value=total_buy_value,
                total_sell_value=total_sell_value,
                realized_pnl=realized_pnl_value,
                unrealized_pnl=unrealized_pnl,
                total_pnl=total_pnl,
                win_rate=win_rate,
                avg_holding_period=None,  # Could be calculated with more complex logic
                best_trade=best_trade,
                worst_trade=worst_trade,
                sharpe_ratio=None,  # Would need historical returns
                total_return_pct=total_return_pct
            ))
        
        # Sort by total PnL descending
        performance_metrics.sort(key=lambda x: x.total_pnl, reverse=True)
        
        return performance_metrics
    
    def generate_leaderboard(self) -> str:
        """Generate a formatted leaderboard of insider performance"""
        metrics = self.calculate_performance_metrics()
        
        leaderboard = "\n🏆 INSIDER TRADING PERFORMANCE LEADERBOARD 🏆\n"
        leaderboard += "=" * 60 + "\n"
        
        for i, metric in enumerate(metrics, 1):
            leaderboard += f"\n#{i} {metric.insider_name}\n"
            leaderboard += f"   Total PnL: ${metric.total_pnl:,.2f}\n"
            leaderboard += f"   Realized: ${metric.realized_pnl:,.2f} | Unrealized: ${metric.unrealized_pnl:,.2f}\n"
            leaderboard += f"   Total Trades: {metric.total_trades} | Win Rate: {metric.win_rate:.1%}\n"
            leaderboard += f"   Return: {metric.total_return_pct:.1f}%\n"
            leaderboard += f"   Total Investment: ${metric.total_buy_value:,.2f}\n"
            leaderboard += "-" * 50 + "\n"
        
        return leaderboard
    
    def store_performance_metrics(self, metrics: List[InsiderPerformanceMetrics]):
        """Store performance metrics in the database"""
        with self.db_manager.get_session() as session:
            # Clear existing performance records
            session.execute(text("DELETE FROM insider_performance"))
            
            for metric in metrics:
                # Get insider ID
                insider = session.query(Insider).filter(
                    Insider.cik == metric.insider_cik
                ).first()
                
                if insider:
                    performance_record = InsiderPerformance(
                        insider_id=insider.id,
                        calculation_date=date.today(),
                        total_realized_pnl=metric.realized_pnl,
                        total_unrealized_pnl=metric.unrealized_pnl,
                        win_rate=Decimal(str(metric.win_rate)),
                        avg_holding_period_days=metric.avg_holding_period,
                        total_transactions=metric.total_trades,
                        best_trade_pnl=metric.best_trade,
                        worst_trade_pnl=metric.worst_trade,
                        sharpe_ratio=Decimal(str(metric.sharpe_ratio)) if metric.sharpe_ratio else None
                    )
                    session.add(performance_record)
            
            session.commit()
            logger.info(f"Stored {len(metrics)} performance records")

# Convenience function for command line usage
def run_performance_analysis():
    """Run complete performance analysis"""
    analyzer = InsiderPerformanceAnalyzer()
    
    print("🔍 Calculating insider trading performance...")
    metrics = analyzer.calculate_performance_metrics()
    
    print("💾 Storing performance metrics in database...")
    analyzer.store_performance_metrics(metrics)
    
    print(analyzer.generate_leaderboard())
    
    return metrics

if __name__ == "__main__":
    run_performance_analysis() 