#!/usr/bin/env python3
"""
VM Scraper Monitor

This script allows you to monitor the progress of your VM scraper
from your local machine using the remote database connection.
"""

import os
import sys
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.database.db_manager import get_db_manager
from src.database.models import DownloadedYear, Form4Filing, Transaction

class VMScraperMonitor:
    """Monitor VM scraper progress from local machine."""
    
    def __init__(self, config_path: str = "config/config_remote.yaml"):
        """Initialize monitor with remote database connection."""
        self.db_manager = get_db_manager(config_path)
        self.last_check = None
        
        # Test connection
        if not self.db_manager.health_check():
            raise ConnectionError("Cannot connect to remote database")
        
        print("✅ Connected to remote database")
    
    def get_scraping_progress(self) -> Dict:
        """Get current scraping progress from database."""
        with self.db_manager.get_session() as session:
            # Get year status
            year_records = session.query(DownloadedYear).all()
            
            years_by_status = {
                'completed': [],
                'in_progress': [],
                'failed': [],
                'pending': []
            }
            
            for year_record in year_records:
                years_by_status[year_record.status].append(year_record)
            
            # Get total counts
            counts = self.db_manager.get_table_counts()
            
            # Calculate progress
            total_years = len(year_records)
            completed_years = len(years_by_status['completed'])
            progress_pct = (completed_years / total_years * 100) if total_years > 0 else 0
            
            return {
                'years_by_status': years_by_status,
                'total_counts': counts,
                'progress_pct': progress_pct,
                'last_updated': datetime.now()
            }
    
    def get_recent_activity(self, hours: int = 24) -> Dict:
        """Get recent scraping activity."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        with self.db_manager.get_session() as session:
            # Recent filings
            recent_filings = session.query(Form4Filing).filter(
                Form4Filing.created_at >= cutoff_time
            ).count()
            
            # Recent transactions
            recent_transactions = session.query(Transaction).filter(
                Transaction.created_at >= cutoff_time
            ).count()
            
            # Recent year updates
            recent_year_updates = session.query(DownloadedYear).filter(
                DownloadedYear.last_updated >= cutoff_time
            ).all()
            
            return {
                'recent_filings': recent_filings,
                'recent_transactions': recent_transactions,
                'recent_year_updates': recent_year_updates,
                'timeframe_hours': hours
            }
    
    def display_status(self):
        """Display current scraping status."""
        print("\n" + "="*60)
        print("📊 VM SCRAPER STATUS MONITOR")
        print("="*60)
        
        try:
            # Get progress data
            progress = self.get_scraping_progress()
            recent = self.get_recent_activity()
            
            # Display overall progress
            print(f"📈 Overall Progress: {progress['progress_pct']:.1f}%")
            print(f"🕐 Last Updated: {progress['last_updated'].strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Display year status
            print(f"\n📅 YEAR STATUS:")
            years_by_status = progress['years_by_status']
            
            if years_by_status['completed']:
                completed_years = [yr.year for yr in years_by_status['completed']]
                print(f"✅ Completed ({len(completed_years)}): {min(completed_years)}-{max(completed_years)}")
            
            if years_by_status['in_progress']:
                in_progress = years_by_status['in_progress'][0]
                hours_running = (datetime.now() - in_progress.download_started).total_seconds() / 3600
                print(f"🔄 In Progress: {in_progress.year} (running {hours_running:.1f}h)")
            
            if years_by_status['failed']:
                failed_years = [yr.year for yr in years_by_status['failed']]
                print(f"❌ Failed ({len(failed_years)}): {failed_years}")
            
            if years_by_status['pending']:
                pending_years = [yr.year for yr in years_by_status['pending']]
                print(f"⏳ Pending ({len(pending_years)}): {min(pending_years)}-{max(pending_years)}")
            
            # Display database counts
            print(f"\n📊 DATABASE TOTALS:")
            counts = progress['total_counts']
            print(f"   Companies: {counts.get('companies', 0):,}")
            print(f"   Insiders: {counts.get('insiders', 0):,}")
            print(f"   Filings: {counts.get('form4_filings', 0):,}")
            print(f"   Transactions: {counts.get('transactions', 0):,}")
            
            # Display recent activity
            print(f"\n⚡ RECENT ACTIVITY (last {recent['timeframe_hours']}h):")
            print(f"   New Filings: {recent['recent_filings']:,}")
            print(f"   New Transactions: {recent['recent_transactions']:,}")
            
            if recent['recent_year_updates']:
                print(f"   Updated Years: {len(recent['recent_year_updates'])}")
                for year_update in recent['recent_year_updates'][-3:]:  # Show last 3
                    print(f"     {year_update.year}: {year_update.status}")
            
        except Exception as e:
            print(f"❌ Error getting status: {e}")
        
        print("\n" + "="*60)
    
    def watch_mode(self, refresh_seconds: int = 60):
        """Continuously monitor scraper progress."""
        print(f"👁️  Starting watch mode (refresh every {refresh_seconds}s)")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                # Clear screen (works on most terminals)
                os.system('clear' if os.name == 'posix' else 'cls')
                
                self.display_status()
                
                print(f"\n⏳ Refreshing in {refresh_seconds}s... (Ctrl+C to stop)")
                time.sleep(refresh_seconds)
                
        except KeyboardInterrupt:
            print("\n👋 Monitoring stopped")
    
    def get_performance_estimate(self) -> Dict:
        """Estimate scraping performance and completion time."""
        with self.db_manager.get_session() as session:
            # Get completed years with timing
            completed_years = session.query(DownloadedYear).filter(
                DownloadedYear.status == 'completed',
                DownloadedYear.download_started.isnot(None),
                DownloadedYear.download_completed.isnot(None)
            ).all()
            
            if not completed_years:
                return {'error': 'No completed years with timing data'}
            
            # Calculate average time per year
            total_time = sum([
                (yr.download_completed - yr.download_started).total_seconds()
                for yr in completed_years
            ])
            
            avg_time_per_year = total_time / len(completed_years)
            
            # Get pending years
            pending_years = session.query(DownloadedYear).filter(
                DownloadedYear.status.in_(['pending', 'failed'])
            ).count()
            
            # Estimate completion time
            estimated_seconds = pending_years * avg_time_per_year
            estimated_hours = estimated_seconds / 3600
            
            return {
                'completed_years': len(completed_years),
                'avg_time_per_year_hours': avg_time_per_year / 3600,
                'pending_years': pending_years,
                'estimated_completion_hours': estimated_hours,
                'estimated_completion_days': estimated_hours / 24
            }
    
    def show_performance_estimate(self):
        """Show performance estimation."""
        print("\n📊 PERFORMANCE ESTIMATE:")
        
        try:
            perf = self.get_performance_estimate()
            
            if 'error' in perf:
                print(f"❌ {perf['error']}")
                return
            
            print(f"✅ Completed Years: {perf['completed_years']}")
            print(f"⏱️  Average Time per Year: {perf['avg_time_per_year_hours']:.1f} hours")
            print(f"⏳ Pending Years: {perf['pending_years']}")
            print(f"🕐 Estimated Completion: {perf['estimated_completion_hours']:.1f} hours ({perf['estimated_completion_days']:.1f} days)")
            
        except Exception as e:
            print(f"❌ Error calculating performance: {e}")


def main():
    """Main function for command line usage."""
    parser = argparse.ArgumentParser(
        description="Monitor VM scraper progress from local machine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python monitor_vm_scraper.py                    # Show current status
  python monitor_vm_scraper.py --watch            # Watch mode (refresh every 60s)
  python monitor_vm_scraper.py --watch --refresh 30  # Watch mode (refresh every 30s)
  python monitor_vm_scraper.py --performance      # Show performance estimate
  python monitor_vm_scraper.py --config config/config_remote.yaml  # Custom config
        """
    )
    
    parser.add_argument('--watch', action='store_true',
                       help='Run in watch mode (continuous monitoring)')
    parser.add_argument('--refresh', type=int, default=60,
                       help='Refresh interval in seconds for watch mode (default: 60)')
    parser.add_argument('--performance', action='store_true',
                       help='Show performance estimation')
    parser.add_argument('--config', default='config/config_remote.yaml',
                       help='Configuration file path')
    
    args = parser.parse_args()
    
    try:
        # Initialize monitor
        monitor = VMScraperMonitor(config_path=args.config)
        
        if args.watch:
            monitor.watch_mode(refresh_seconds=args.refresh)
        elif args.performance:
            monitor.show_performance_estimate()
        else:
            monitor.display_status()
            
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 