#!/usr/bin/env python3
"""
Comprehensive SEC Form 4 Scraper for VM Deployment

This script downloads all available SEC Form 4 data from 1995-present
with progress tracking, error handling, and VM-optimized settings.
"""

import os
import sys
import json
import logging
import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.database.db_manager import get_db_manager
from src.database.models import DownloadedYear
from src.data_collection.bulk_downloader import SECBulkDownloader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('comprehensive_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ComprehensiveScraper:
    """Comprehensive SEC Form 4 scraper with progress tracking."""
    
    def __init__(self, config_path: str = "config/config_vm.yaml", 
                 threads: int = 6, status_file: str = "scraper_status.json"):
        """
        Initialize the comprehensive scraper.
        
        Args:
            config_path: Path to configuration file
            threads: Number of download threads
            status_file: JSON file to track scraping status
        """
        self.config_path = config_path
        self.threads = threads
        self.status_file = status_file
        
        # Initialize database manager
        self.db_manager = get_db_manager(config_path)
        
        # Initialize downloader
        self.downloader = SECBulkDownloader(max_workers=threads)
        
        # Load/create status tracking
        self.status = self._load_status()
        
        logger.info(f"Comprehensive scraper initialized with {threads} threads")
    
    def _load_status(self) -> Dict:
        """Load scraping status from JSON file."""
        if os.path.exists(self.status_file):
            try:
                with open(self.status_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load status file: {e}")
        
        # Default status
        return {
            'started_at': None,
            'last_updated': None,
            'current_year': None,
            'completed_years': [],
            'failed_years': [],
            'total_filings': 0,
            'total_errors': 0,
            'continuous_mode': False
        }
    
    def _save_status(self):
        """Save current status to JSON file."""
        self.status['last_updated'] = datetime.now().isoformat()
        try:
            with open(self.status_file, 'w') as f:
                json.dump(self.status, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save status file: {e}")
    
    def _get_year_status(self, year: int) -> Optional[str]:
        """Get download status for a specific year."""
        with self.db_manager.get_session() as session:
            year_record = session.query(DownloadedYear).filter_by(year=year).first()
            return year_record.status if year_record else None
    
    def _set_year_status(self, year: int, status: str, **kwargs):
        """Set download status for a specific year."""
        with self.db_manager.get_session() as session:
            year_record = session.query(DownloadedYear).filter_by(year=year).first()
            if not year_record:
                year_record = DownloadedYear(year=year)
                session.add(year_record)
            
            year_record.status = status
            year_record.last_updated = datetime.now()
            
            # Update additional fields
            for key, value in kwargs.items():
                if hasattr(year_record, key):
                    setattr(year_record, key, value)
            
            session.commit()
    
    def get_years_to_process(self, start_year: int = 1995, 
                           end_year: int = None, force: bool = False) -> List[int]:
        """
        Get list of years that need processing.
        
        Args:
            start_year: Starting year for collection
            end_year: Ending year for collection (defaults to current year)
            force: Force re-download of all years
            
        Returns:
            List of years to process
        """
        if end_year is None:
            end_year = datetime.now().year
        
        years_to_process = []
        
        with self.db_manager.get_session() as session:
            for year in range(start_year, end_year + 1):
                year_record = session.query(DownloadedYear).filter_by(year=year).first()
                
                if force:
                    years_to_process.append(year)
                elif not year_record or year_record.status in ['pending', 'failed']:
                    years_to_process.append(year)
                elif year_record.status == 'in_progress':
                    # Check if it's been stuck for more than 24 hours
                    if year_record.download_started:
                        hours_since_start = (datetime.now() - year_record.download_started).total_seconds() / 3600
                        if hours_since_start > 24:
                            logger.warning(f"Year {year} has been in progress for {hours_since_start:.1f} hours, retrying")
                            years_to_process.append(year)
        
        return sorted(years_to_process)
    
    def download_year(self, year: int, force: bool = False) -> bool:
        """
        Download all Form 4 filings for a specific year.
        
        Args:
            year: Year to download
            force: Force re-download even if already completed
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🗓️  Starting download for year {year}")
        
        # Check if already completed
        if not force:
            year_status = self._get_year_status(year)
            if year_status == 'completed':
                logger.info(f"Year {year} already completed, skipping")
                return True
        
        # Set status to in_progress
        self._set_year_status(year, 'in_progress', 
                            download_started=datetime.now(),
                            download_completed=None)
        
        try:
            # Update status
            self.status['current_year'] = year
            self._save_status()
            
            # Download the year
            success = self.downloader.download_year(year, force=force)
            
            if success:
                # Mark as completed
                self._set_year_status(year, 'completed', 
                                    download_completed=datetime.now())
                
                if year not in self.status['completed_years']:
                    self.status['completed_years'].append(year)
                
                logger.info(f"✅ Successfully completed year {year}")
                return True
            else:
                # Mark as failed
                self._set_year_status(year, 'failed',
                                    error_message=f"Download failed for year {year}")
                
                if year not in self.status['failed_years']:
                    self.status['failed_years'].append(year)
                
                logger.error(f"❌ Failed to download year {year}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error downloading year {year}: {e}")
            
            # Mark as failed
            self._set_year_status(year, 'failed',
                                error_message=str(e))
            
            if year not in self.status['failed_years']:
                self.status['failed_years'].append(year)
            
            return False
        
        finally:
            # Update status
            self.status['current_year'] = None
            self._save_status()
    
    def download_all_years(self, start_year: int = 1995, end_year: int = None, 
                          force: bool = False) -> Dict[str, List[int]]:
        """
        Download all available years of Form 4 data.
        
        Args:
            start_year: Starting year for collection
            end_year: Ending year for collection (defaults to current year)
            force: Force re-download of all years
            
        Returns:
            Dictionary with 'completed' and 'failed' year lists
        """
        if end_year is None:
            end_year = datetime.now().year
        
        logger.info(f"🚀 Starting comprehensive download: {start_year}-{end_year}")
        
        # Initialize status
        if not self.status['started_at']:
            self.status['started_at'] = datetime.now().isoformat()
        
        # Get years to process
        years_to_process = self.get_years_to_process(start_year, end_year, force)
        
        if not years_to_process:
            logger.info("🎉 All years already completed!")
            return {
                'completed': self.status['completed_years'],
                'failed': self.status['failed_years']
            }
        
        logger.info(f"📋 Years to process: {len(years_to_process)} ({min(years_to_process)}-{max(years_to_process)})")
        
        # Process each year
        completed = []
        failed = []
        
        for i, year in enumerate(years_to_process, 1):
            logger.info(f"🔄 Processing year {year} ({i}/{len(years_to_process)})")
            
            success = self.download_year(year, force=force)
            
            if success:
                completed.append(year)
                logger.info(f"✅ Year {year} completed ({len(completed)}/{len(years_to_process)})")
            else:
                failed.append(year)
                logger.error(f"❌ Year {year} failed ({len(failed)} total failures)")
            
            # Update progress
            progress = (i / len(years_to_process)) * 100
            logger.info(f"📊 Overall progress: {progress:.1f}% ({i}/{len(years_to_process)})")
        
        # Final status
        logger.info(f"🎯 Comprehensive download completed!")
        logger.info(f"✅ Completed: {len(completed)} years")
        logger.info(f"❌ Failed: {len(failed)} years")
        
        if failed:
            logger.warning(f"Failed years: {failed}")
        
        return {
            'completed': completed,
            'failed': failed
        }
    
    def show_status(self):
        """Show current scraping status."""
        print("\n" + "="*50)
        print("📊 COMPREHENSIVE SCRAPER STATUS")
        print("="*50)
        
        if self.status['started_at']:
            print(f"🕐 Started: {self.status['started_at']}")
        
        if self.status['last_updated']:
            print(f"🔄 Last Updated: {self.status['last_updated']}")
        
        if self.status['current_year']:
            print(f"📅 Current Year: {self.status['current_year']}")
        
        print(f"✅ Completed Years: {len(self.status['completed_years'])}")
        if self.status['completed_years']:
            years_str = ', '.join(map(str, sorted(self.status['completed_years'])))
            print(f"   {years_str}")
        
        print(f"❌ Failed Years: {len(self.status['failed_years'])}")
        if self.status['failed_years']:
            years_str = ', '.join(map(str, sorted(self.status['failed_years'])))
            print(f"   {years_str}")
        
        print(f"📊 Total Filings: {self.status['total_filings']:,}")
        print(f"🚫 Total Errors: {self.status['total_errors']:,}")
        
        # Database status
        print(f"\n📈 DATABASE STATUS:")
        try:
            counts = self.db_manager.get_table_counts()
            print(f"   Companies: {counts.get('companies', 0):,}")
            print(f"   Insiders: {counts.get('insiders', 0):,}")
            print(f"   Filings: {counts.get('form4_filings', 0):,}")
            print(f"   Transactions: {counts.get('transactions', 0):,}")
        except Exception as e:
            print(f"   Error getting database counts: {e}")
        
        print("\n" + "="*50)
    
    def continuous_mode(self, start_year: int = 1995, end_year: int = None):
        """
        Run in continuous mode - keep checking for new data.
        
        Args:
            start_year: Starting year for collection
            end_year: Ending year for collection (defaults to current year)
        """
        logger.info("🔄 Starting continuous mode...")
        self.status['continuous_mode'] = True
        
        while True:
            try:
                # Update end year to current year
                current_year = datetime.now().year
                if end_year is None:
                    end_year = current_year
                
                # Download all years
                results = self.download_all_years(start_year, end_year)
                
                # Check if we need to wait
                if not results['failed'] and end_year >= current_year:
                    # All caught up, wait 1 hour
                    logger.info("😴 All caught up! Waiting 1 hour before next check...")
                    time.sleep(3600)
                else:
                    # Some failures or new year available, wait 30 minutes
                    logger.info("⏳ Waiting 30 minutes before retry...")
                    time.sleep(1800)
                    
            except KeyboardInterrupt:
                logger.info("🛑 Continuous mode stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Error in continuous mode: {e}")
                logger.info("⏳ Waiting 10 minutes before retry...")
                time.sleep(600)
        
        self.status['continuous_mode'] = False
        self._save_status()


def main():
    """Main function for command line usage."""
    parser = argparse.ArgumentParser(
        description="Comprehensive SEC Form 4 Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python comprehensive_scraper.py --start 1995 --end 2024     # Download all years
  python comprehensive_scraper.py --start 2023 --end 2024     # Download recent years
  python comprehensive_scraper.py --threads 6                 # Use 6 threads
  python comprehensive_scraper.py --force                     # Force re-download
  python comprehensive_scraper.py --status                    # Show status
  python comprehensive_scraper.py --continuous                # Run continuously
        """
    )
    
    parser.add_argument('--start', type=int, default=1995, 
                       help='Start year for collection (default: 1995)')
    parser.add_argument('--end', type=int, default=None,
                       help='End year for collection (default: current year)')
    parser.add_argument('--threads', type=int, default=6,
                       help='Number of download threads (default: 6)')
    parser.add_argument('--force', action='store_true',
                       help='Force re-download of all years')
    parser.add_argument('--status', action='store_true',
                       help='Show current status and exit')
    parser.add_argument('--continuous', action='store_true',
                       help='Run in continuous mode')
    parser.add_argument('--config', default='config/config_vm.yaml',
                       help='Configuration file path')
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = ComprehensiveScraper(
        config_path=args.config,
        threads=args.threads
    )
    
    # Handle different modes
    if args.status:
        scraper.show_status()
        return
    
    if args.continuous:
        scraper.continuous_mode(args.start, args.end)
        return
    
    # Regular download mode
    logger.info("🚀 Starting comprehensive SEC Form 4 download...")
    logger.info(f"📅 Years: {args.start}-{args.end or datetime.now().year}")
    logger.info(f"🔧 Threads: {args.threads}")
    logger.info(f"🔄 Force: {args.force}")
    
    try:
        results = scraper.download_all_years(args.start, args.end, args.force)
        
        print(f"\n🎯 Download completed!")
        print(f"✅ Successfully downloaded: {len(results['completed'])} years")
        print(f"❌ Failed: {len(results['failed'])} years")
        
        if results['failed']:
            print(f"❌ Failed years: {results['failed']}")
        
    except KeyboardInterrupt:
        logger.info("🛑 Download interrupted by user")
    except Exception as e:
        logger.error(f"❌ Download failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 