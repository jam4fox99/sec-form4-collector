#!/usr/bin/env python3
"""
Cloud Data Analyzer for SEC Form 4 Data
This script allows you to analyze data stored in your Google Cloud database
from your local machine using Cursor.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Optional, Tuple
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.database.db_manager import DatabaseManager
from src.utils.rate_limiter import RateLimiter
import yfinance as yf

class CloudDataAnalyzer:
    """Analyzer for SEC Form 4 data stored in cloud database."""
    
    def __init__(self, config_path: str = "config/config_local_cloud_access.yaml"):
        """Initialize the analyzer with cloud database connection."""
        self.db = DatabaseManager(config_path)
        self.rate_limiter = RateLimiter(requests_per_second=10)
        
    def get_data_summary(self) -> Dict:
        """Get summary statistics about the collected data."""
        with self.db.get_session() as session:
            # Get counts for each table
            companies_count = session.execute("SELECT COUNT(*) FROM companies").scalar()
            insiders_count = session.execute("SELECT COUNT(*) FROM insiders").scalar()
            filings_count = session.execute("SELECT COUNT(*) FROM form4_filings").scalar()
            transactions_count = session.execute("SELECT COUNT(*) FROM transactions").scalar()
            
            # Get date range
            date_range = session.execute("""
                SELECT MIN(filing_date) as min_date, MAX(filing_date) as max_date 
                FROM form4_filings
            """).fetchone()
            
            # Get top companies by filing count
            top_companies = session.execute("""
                SELECT c.company_name, COUNT(*) as filing_count
                FROM companies c
                JOIN form4_filings f ON c.cik = f.company_cik
                GROUP BY c.company_name
                ORDER BY filing_count DESC
                LIMIT 10
            """).fetchall()
            
            return {
                'companies_count': companies_count,
                'insiders_count': insiders_count,
                'filings_count': filings_count,
                'transactions_count': transactions_count,
                'date_range': date_range,
                'top_companies': top_companies
            }
    
    def get_insider_performance(self, limit: int = 50) -> pd.DataFrame:
        """Get insider performance data."""
        query = """
        SELECT 
            i.name as insider_name,
            c.company_name,
            c.ticker_symbol,
            ip.total_pnl,
            ip.realized_pnl,
            ip.unrealized_pnl,
            ip.total_shares_owned,
            ip.total_transactions,
            ip.first_transaction_date,
            ip.last_transaction_date,
            ip.last_updated
        FROM insider_performance ip
        JOIN insiders i ON ip.insider_id = i.id
        JOIN companies c ON ip.company_cik = c.cik
        ORDER BY ip.total_pnl DESC
        LIMIT %s
        """
        
        with self.db.get_session() as session:
            result = session.execute(query, (limit,))
            df = pd.DataFrame(result.fetchall(), columns=[
                'insider_name', 'company_name', 'ticker_symbol', 'total_pnl',
                'realized_pnl', 'unrealized_pnl', 'total_shares_owned',
                'total_transactions', 'first_transaction_date',
                'last_transaction_date', 'last_updated'
            ])
            
            return df
    
    def get_company_insider_activity(self, ticker: str = None, limit: int = 100) -> pd.DataFrame:
        """Get insider activity for a specific company or all companies."""
        base_query = """
        SELECT 
            c.company_name,
            c.ticker_symbol,
            i.name as insider_name,
            ir.relationship_type,
            ir.title,
            t.transaction_date,
            t.transaction_code,
            t.transaction_type,
            t.shares,
            t.price_per_share,
            t.total_value,
            f.filing_date,
            f.accession_number
        FROM transactions t
        JOIN form4_filings f ON t.filing_id = f.id
        JOIN companies c ON f.company_cik = c.cik
        JOIN insiders i ON f.insider_id = i.id
        JOIN insider_relationships ir ON i.id = ir.insider_id AND c.cik = ir.company_cik
        """
        
        params = []
        if ticker:
            base_query += " WHERE c.ticker_symbol = %s"
            params.append(ticker.upper())
        
        base_query += " ORDER BY t.transaction_date DESC LIMIT %s"
        params.append(limit)
        
        with self.db.get_session() as session:
            result = session.execute(base_query, params)
            df = pd.DataFrame(result.fetchall(), columns=[
                'company_name', 'ticker_symbol', 'insider_name', 'relationship_type',
                'title', 'transaction_date', 'transaction_code', 'transaction_type',
                'shares', 'price_per_share', 'total_value', 'filing_date', 'accession_number'
            ])
            
            return df
    
    def get_transaction_patterns(self) -> pd.DataFrame:
        """Analyze transaction patterns."""
        query = """
        SELECT 
            transaction_code,
            transaction_type,
            COUNT(*) as count,
            SUM(shares) as total_shares,
            SUM(total_value) as total_value,
            AVG(price_per_share) as avg_price,
            MIN(transaction_date) as first_transaction,
            MAX(transaction_date) as last_transaction
        FROM transactions
        GROUP BY transaction_code, transaction_type
        ORDER BY count DESC
        """
        
        with self.db.get_session() as session:
            result = session.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=[
                'transaction_code', 'transaction_type', 'count', 'total_shares',
                'total_value', 'avg_price', 'first_transaction', 'last_transaction'
            ])
            
            return df
    
    def get_monthly_activity(self) -> pd.DataFrame:
        """Get monthly filing activity."""
        query = """
        SELECT 
            DATE_TRUNC('month', filing_date) as month,
            COUNT(*) as filing_count,
            COUNT(DISTINCT company_cik) as unique_companies,
            COUNT(DISTINCT insider_id) as unique_insiders,
            SUM(CASE WHEN t.transaction_code = 'P' THEN 1 ELSE 0 END) as purchases,
            SUM(CASE WHEN t.transaction_code = 'S' THEN 1 ELSE 0 END) as sales
        FROM form4_filings f
        LEFT JOIN transactions t ON f.id = t.filing_id
        GROUP BY DATE_TRUNC('month', filing_date)
        ORDER BY month
        """
        
        with self.db.get_session() as session:
            result = session.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=[
                'month', 'filing_count', 'unique_companies', 'unique_insiders',
                'purchases', 'sales'
            ])
            
            return df
    
    def create_performance_dashboard(self, save_path: str = "data/analysis/"):
        """Create performance dashboard with visualizations."""
        os.makedirs(save_path, exist_ok=True)
        
        # Get data
        print("📊 Generating performance dashboard...")
        summary = self.get_data_summary()
        performance_df = self.get_insider_performance()
        monthly_df = self.get_monthly_activity()
        patterns_df = self.get_transaction_patterns()
        
        # Create visualizations
        plt.style.use('seaborn-v0_8')
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # Top performers
        if not performance_df.empty:
            top_10 = performance_df.head(10)
            axes[0, 0].barh(range(len(top_10)), top_10['total_pnl'])
            axes[0, 0].set_yticks(range(len(top_10)))
            axes[0, 0].set_yticklabels(top_10['insider_name'], fontsize=8)
            axes[0, 0].set_xlabel('Total PnL ($)')
            axes[0, 0].set_title('Top 10 Insider Performers')
            axes[0, 0].grid(True, alpha=0.3)
        
        # Monthly activity
        if not monthly_df.empty:
            monthly_df['month'] = pd.to_datetime(monthly_df['month'])
            axes[0, 1].plot(monthly_df['month'], monthly_df['filing_count'], marker='o')
            axes[0, 1].set_xlabel('Month')
            axes[0, 1].set_ylabel('Filing Count')
            axes[0, 1].set_title('Monthly Filing Activity')
            axes[0, 1].tick_params(axis='x', rotation=45)
            axes[0, 1].grid(True, alpha=0.3)
        
        # Transaction patterns
        if not patterns_df.empty:
            axes[1, 0].pie(patterns_df['count'], labels=patterns_df['transaction_code'], autopct='%1.1f%%')
            axes[1, 0].set_title('Transaction Code Distribution')
        
        # PnL distribution
        if not performance_df.empty:
            axes[1, 1].hist(performance_df['total_pnl'], bins=30, alpha=0.7)
            axes[1, 1].set_xlabel('Total PnL ($)')
            axes[1, 1].set_ylabel('Frequency')
            axes[1, 1].set_title('PnL Distribution')
            axes[1, 1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f"{save_path}/performance_dashboard.png", dpi=300, bbox_inches='tight')
        plt.close()
        
        # Save summary report
        with open(f"{save_path}/data_summary.txt", 'w') as f:
            f.write("SEC Form 4 Data Collection Summary\n")
            f.write("=" * 40 + "\n\n")
            f.write(f"Companies: {summary['companies_count']:,}\n")
            f.write(f"Insiders: {summary['insiders_count']:,}\n")
            f.write(f"Filings: {summary['filings_count']:,}\n")
            f.write(f"Transactions: {summary['transactions_count']:,}\n")
            
            if summary['date_range']:
                f.write(f"Date Range: {summary['date_range'][0]} to {summary['date_range'][1]}\n")
            
            f.write("\nTop Companies by Filing Count:\n")
            for company, count in summary['top_companies']:
                f.write(f"  {company}: {count:,} filings\n")
        
        # Save CSV files
        performance_df.to_csv(f"{save_path}/insider_performance.csv", index=False)
        monthly_df.to_csv(f"{save_path}/monthly_activity.csv", index=False)
        patterns_df.to_csv(f"{save_path}/transaction_patterns.csv", index=False)
        
        print(f"✅ Dashboard saved to {save_path}")
        print(f"   - performance_dashboard.png")
        print(f"   - data_summary.txt")
        print(f"   - insider_performance.csv")
        print(f"   - monthly_activity.csv")
        print(f"   - transaction_patterns.csv")
    
    def search_insider(self, name: str) -> pd.DataFrame:
        """Search for specific insider by name."""
        query = """
        SELECT 
            i.name as insider_name,
            c.company_name,
            c.ticker_symbol,
            ir.relationship_type,
            ir.title,
            COUNT(f.id) as filing_count,
            MIN(f.filing_date) as first_filing,
            MAX(f.filing_date) as last_filing
        FROM insiders i
        JOIN form4_filings f ON i.id = f.insider_id
        JOIN companies c ON f.company_cik = c.cik
        JOIN insider_relationships ir ON i.id = ir.insider_id AND c.cik = ir.company_cik
        WHERE LOWER(i.name) LIKE LOWER(%s)
        GROUP BY i.name, c.company_name, c.ticker_symbol, ir.relationship_type, ir.title
        ORDER BY filing_count DESC
        """
        
        with self.db.get_session() as session:
            result = session.execute(query, (f"%{name}%",))
            df = pd.DataFrame(result.fetchall(), columns=[
                'insider_name', 'company_name', 'ticker_symbol', 'relationship_type',
                'title', 'filing_count', 'first_filing', 'last_filing'
            ])
            
            return df

def main():
    """Main function for command line usage."""
    print("🔍 SEC Form 4 Cloud Data Analyzer")
    print("=" * 40)
    
    try:
        analyzer = CloudDataAnalyzer()
        
        # Get and display summary
        print("\n📊 Data Summary:")
        summary = analyzer.get_data_summary()
        print(f"Companies: {summary['companies_count']:,}")
        print(f"Insiders: {summary['insiders_count']:,}")
        print(f"Filings: {summary['filings_count']:,}")
        print(f"Transactions: {summary['transactions_count']:,}")
        
        if summary['date_range']:
            print(f"Date Range: {summary['date_range'][0]} to {summary['date_range'][1]}")
        
        print("\n🏆 Top Companies by Filing Count:")
        for company, count in summary['top_companies'][:5]:
            print(f"  {company}: {count:,} filings")
        
        # Create dashboard
        analyzer.create_performance_dashboard()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure your cloud database is accessible and config is correct.")

if __name__ == "__main__":
    main() 