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
        
        logger.info("Command completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Command failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 