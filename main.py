#!/usr/bin/env python3
"""
Main script for the Insider Trading Analysis System.
Demonstrates how to use the system to download and analyze Form 4 filings.
"""
import logging
import sys
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from src.database.db_manager import init_database, get_db_manager
from src.data_collection.bulk_processor import BulkProcessor
from src.data_collection.edgar_downloader import EDGARDownloader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('insider_trading.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def setup_database():
    """Initialize database and create tables"""
    logger.info("Setting up database...")
    try:
        db_manager = init_database()
        logger.info("Database setup completed successfully")
        return db_manager
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        raise


def test_downloader():
    """Test the EDGAR downloader with a small sample"""
    logger.info("Testing EDGAR downloader...")
    
    downloader = EDGARDownloader()
    
    # Test downloading company tickers
    tickers = downloader.download_company_tickers()
    logger.info(f"Downloaded {len(tickers)} company tickers")
    
    # Test downloading a few recent filings
    recent_count = downloader.download_recent_filings(days_back=2)
    logger.info(f"Downloaded {recent_count} recent filings")
    
    # Show download stats
    stats = downloader.get_download_stats()
    logger.info(f"Download stats: {stats}")


def process_sample_data():
    """Process a small sample of Form 4 filings"""
    logger.info("Processing sample Form 4 filings...")
    
    # Process last 3 days of filings (limited sample)
    end_date = date.today()
    start_date = end_date - timedelta(days=3)
    
    processor = BulkProcessor(
        num_download_threads=2,
        num_parse_threads=2,
        batch_size=50
    )
    
    try:
        # Limit to 100 filings for demonstration
        stats = processor.process_date_range(start_date, end_date, max_filings=100)
        
        logger.info("Sample processing completed!")
        logger.info(f"Total filings: {stats.total_filings}")
        logger.info(f"Downloaded: {stats.downloaded}")
        logger.info(f"Parsed: {stats.parsed}")
        logger.info(f"Stored: {stats.stored}")
        logger.info(f"Errors: {stats.errors}")
        
        if stats.start_time and stats.end_time:
            duration = stats.end_time - stats.start_time
            logger.info(f"Processing time: {duration}")
        
    except Exception as e:
        logger.error(f"Error in sample processing: {e}")
        raise
    finally:
        processor.cleanup()


def process_historical_data(start_year: int, end_year: int):
    """Process historical Form 4 data (full collection)"""
    logger.info(f"Starting historical data collection from {start_year} to {end_year}")
    logger.warning("This will take several days to complete!")
    
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)
    
    processor = BulkProcessor(
        num_download_threads=2,  # Keep conservative for rate limiting
        num_parse_threads=4,
        batch_size=1000
    )
    
    try:
        stats = processor.process_date_range(start_date, end_date)
        
        logger.info("Historical data collection completed!")
        logger.info(f"Total filings: {stats.total_filings}")
        logger.info(f"Downloaded: {stats.downloaded}")
        logger.info(f"Parsed: {stats.parsed}")
        logger.info(f"Stored: {stats.stored}")
        logger.info(f"Errors: {stats.errors}")
        
        if stats.start_time and stats.end_time:
            duration = stats.end_time - stats.start_time
            logger.info(f"Total processing time: {duration}")
        
    except Exception as e:
        logger.error(f"Error in historical processing: {e}")
        raise
    finally:
        processor.cleanup()


def show_database_stats():
    """Show current database statistics"""
    logger.info("Database Statistics:")
    
    db_manager = get_db_manager()
    counts = db_manager.get_table_counts()
    
    for table_name, count in counts.items():
        logger.info(f"  {table_name}: {count:,} records")


def show_downloaded_years():
    """Show status of downloaded years"""
    print("📊 Downloaded Years Status:")
    
    db_manager = get_db_manager()
    downloaded_years = db_manager.get_downloaded_years()
    
    if not downloaded_years:
        print("   No years downloaded yet")
        return
    
    # Sort years for display
    sorted_years = sorted(downloaded_years.keys())
    
    for year in sorted_years:
        status = downloaded_years[year]
        status_icon = {
            'completed': '✅',
            'in_progress': '🔄',
            'failed': '❌',
            'pending': '⏳'
        }.get(status, '❓')
        
        print(f"   {year}: {status_icon} {status}")
    
    # Show summary
    status_counts = {}
    for status in downloaded_years.values():
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print(f"\n📈 Summary:")
    for status, count in status_counts.items():
        print(f"   {status}: {count} years")


def cmd_bulk(args):
    """Process data using true bulk downloads (NO rate limiting)"""
    print("🚀 Starting TRUE bulk download (NO rate limiting)...")
    
    from src.data_collection.bulk_downloader import SECBulkDownloader
    
    # Setup database
    print("Setting up database...")
    db_manager = setup_database()
    
    # Initialize bulk downloader with threading
    max_threads = getattr(args, 'threads', 6)  # Conservative default to avoid rate limiting
    force_download = getattr(args, 'force', False)
    downloader = SECBulkDownloader(max_workers=max_threads)
    
    print(f"⚙️ Using {max_threads} threads (use --threads to adjust, max 8 recommended)")
    if max_threads > 8:
        print("⚠️ WARNING: High thread counts may trigger SEC rate limiting!")
    
    if force_download:
        print("🔥 FORCE MODE: Re-downloading ALL filings (ignoring existing ones)")
    
    try:
        # Step 1: Find all filings (fast)
        print(f"📋 Step 1: Finding all {args.start_year}-{args.end_year} filings...")
        
        all_filings = []
        for year in range(args.start_year, args.end_year + 1):
            year_stats = downloader.bulk_download_year(year)
            all_filings.extend(year_stats['all_filings'])
        
        print(f"📊 Found {len(all_filings):,} total Form 4 filings!")
        
        # Step 2: Download and store filing content
        print(f"\n📥 Step 2: Downloading and storing filing content...")
        print(f"   Processing {len(all_filings):,} filings...")
        
        storage_stats = downloader.download_and_store_filings(all_filings, db_manager, force=force_download)
        
        print(f"\n✅ Bulk download and storage completed!")
        print(f"   Total filings found: {len(all_filings):,}")
        print(f"   Successfully stored: {storage_stats['stored']:,}")
        print(f"   Already existed: {storage_stats['skipped']:,}")
        print(f"   Errors: {storage_stats['errors']:,}")
        
        logger.info("Bulk download completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
    except Exception as e:
        logger.error(f"Error in bulk download: {e}")
        raise


def cmd_conservative_bulk(args):
    """Process data using conservative bulk downloads (rate-limit safe)"""
    print("🛡️ Starting CONSERVATIVE bulk download (rate-limit safe)...")
    
    from src.data_collection.bulk_downloader import SECBulkDownloader
    
    # Setup database
    print("Setting up database...")
    db_manager = setup_database()
    
    # Initialize bulk downloader with conservative settings
    max_threads = getattr(args, 'threads', 4)  # Default to 4 threads
    force_download = getattr(args, 'force', False)
    downloader = SECBulkDownloader(max_workers=max_threads)
    
    print(f"⚙️ Using {max_threads} threads (conservative rate-limit safe)")
    if max_threads <= 4:
        print("⚠️  This will take 2-4 hours and should avoid rate limiting!")
    else:
        print("⚠️  This will take 1-3 hours but may encounter some rate limiting!")
    
    if force_download:
        print("🔥 FORCE MODE: Re-downloading ALL filings (ignoring existing ones)")
    
    try:
        # Check downloaded years status
        print(f"📊 Checking status of years {args.start_year}-{args.end_year}...")
        downloaded_years = db_manager.get_downloaded_years()
        years_to_process = []
        
        for year in range(args.start_year, args.end_year + 1):
            status = downloaded_years.get(year, None)
            if status == 'completed' and not force_download:
                print(f"   {year}: ✅ Already downloaded")
            elif status == 'in_progress':
                print(f"   {year}: 🔄 In progress - resuming")
                years_to_process.append(year)
            elif status == 'failed':
                print(f"   {year}: ❌ Previously failed - retrying")
                years_to_process.append(year)
            else:
                print(f"   {year}: ⏳ Not downloaded")
                years_to_process.append(year)
        
        if not years_to_process:
            print("✅ All requested years already downloaded!")
            return
        
        print(f"\n📋 Processing {len(years_to_process)} years: {years_to_process}")
        
        # Process each year individually
        for year in years_to_process:
            print(f"\n📅 Processing year {year}...")
            
            # Mark year as in progress
            db_manager.set_year_status(year, 'in_progress')
            
            try:
                # Step 1: Find all filings for this year
                print(f"   🔍 Finding {year} filings...")
                year_stats = downloader.bulk_download_year(year)
                all_filings = year_stats['all_filings']
                
                if not all_filings:
                    print(f"   ⚠️ No filings found for {year}")
                    db_manager.set_year_status(year, 'completed', 
                                             total_filings=0, 
                                             processed_quarters=year_stats['processed_quarters'])
                    continue
                
                print(f"   📊 Found {len(all_filings):,} Form 4 filings!")
                
                # Update database with discovery stats
                db_manager.set_year_status(year, 'in_progress',
                                         total_filings=len(all_filings),
                                         processed_quarters=year_stats['processed_quarters'])
                
                # Step 2: Download and store filing content
                print(f"   📥 Downloading and storing {len(all_filings):,} filings...")
                # Estimate time based on thread count (roughly 2 seconds per filing / thread count)
                estimated_hours = (len(all_filings) * 2) / (max_threads * 3600)
                print(f"   ⏰ Estimated time: {estimated_hours:.1f} hours with {max_threads} threads")
                
                storage_stats = downloader.download_and_store_filings(all_filings, db_manager, force=force_download)
                
                # Update final stats
                db_manager.set_year_status(year, 'completed',
                                         downloaded_count=storage_stats['downloaded'],
                                         stored_count=storage_stats['stored'],
                                         error_count=storage_stats['errors'],
                                         skipped_count=storage_stats['skipped'])
                
                print(f"   ✅ {year} completed!")
                print(f"      Downloaded: {storage_stats['downloaded']:,}")
                print(f"      Stored: {storage_stats['stored']:,}")
                print(f"      Skipped: {storage_stats['skipped']:,}")
                print(f"      Errors: {storage_stats['errors']:,}")
                
            except Exception as e:
                error_msg = str(e)
                print(f"   ❌ {year} failed: {error_msg}")
                db_manager.set_year_status(year, 'failed', error_message=error_msg)
                logger.error(f"Year {year} failed: {error_msg}")
                
                # Continue with next year instead of failing completely
                continue
        
        print(f"\n✅ Conservative bulk download completed!")
        logger.info("Conservative bulk download completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        # Mark any in-progress years as failed
        for year in years_to_process:
            if db_manager.get_year_status(year) == 'in_progress':
                db_manager.set_year_status(year, 'failed', error_message='Cancelled by user')
    except Exception as e:
        logger.error(f"Error in conservative bulk download: {e}")
        raise


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Insider Trading Analysis System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py setup                    # Initialize database
  python main.py test                     # Test downloader
  python main.py sample                   # Process sample data
  python main.py historical 2020 2024    # Process historical data
  python main.py stats                    # Show database statistics
  python main.py years                    # Show downloaded years status
  python main.py bulk 2020 2024          # Download using true bulk method (fast)
  python main.py conservative_bulk 2023 2023  # Download 2023 data (rate-limit safe, 4 threads)
  python main.py conservative_bulk 2023 2023 --threads 6  # Download 2023 data (faster, 6 threads)
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Setup command
    subparsers.add_parser('setup', help='Initialize database and create tables')
    
    # Test command
    subparsers.add_parser('test', help='Test EDGAR downloader')
    
    # Sample command
    subparsers.add_parser('sample', help='Process sample Form 4 filings')
    
    # Historical command
    historical_parser = subparsers.add_parser('historical', help='Process historical data')
    historical_parser.add_argument('start_year', type=int, help='Start year')
    historical_parser.add_argument('end_year', type=int, help='End year')
    
    # Stats command
    subparsers.add_parser('stats', help='Show database statistics')
    
    # Years command
    subparsers.add_parser('years', help='Show downloaded years status')

    # Add the bulk command
    bulk_parser = subparsers.add_parser('bulk', help='Download using true bulk method (fast)')
    bulk_parser.add_argument('start_year', type=int, help='Start year for collection')
    bulk_parser.add_argument('end_year', type=int, help='End year for collection')
    bulk_parser.add_argument('--threads', type=int, default=6, help='Number of download threads (default: 6, max: 8 to avoid rate limiting)')
    bulk_parser.add_argument('--force', action='store_true', help='Force re-download all filings (skip duplicate check)')
    bulk_parser.set_defaults(func=cmd_bulk)

    # Add the conservative bulk command
    conservative_bulk_parser = subparsers.add_parser('conservative_bulk', help='Download using conservative bulk method (rate-limit safe)')
    conservative_bulk_parser.add_argument('start_year', type=int, help='Start year for collection')
    conservative_bulk_parser.add_argument('end_year', type=int, help='End year for collection')
    conservative_bulk_parser.add_argument('--threads', type=int, default=4, help='Number of download threads (default: 4, recommended 2-6 for rate-limit safety)')
    conservative_bulk_parser.add_argument('--force', action='store_true', help='Force re-download all filings (skip duplicate check)')
    conservative_bulk_parser.set_defaults(func=cmd_conservative_bulk)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'setup':
            setup_database()
        
        elif args.command == 'test':
            setup_database()
            test_downloader()
        
        elif args.command == 'sample':
            setup_database()
            process_sample_data()
        
        elif args.command == 'historical':
            setup_database()
            process_historical_data(args.start_year, args.end_year)
        
        elif args.command == 'stats':
            show_database_stats()
        
        elif args.command == 'years':
            show_downloaded_years()
        
        elif args.command == 'bulk':
            args.func(args)
        
        elif args.command == 'conservative_bulk':
            args.func(args)
        
        logger.info("Command completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Command failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 