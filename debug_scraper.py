#!/usr/bin/env python3
"""
Debug script to test bulk downloader functionality and identify storage issues.
"""

import logging
from datetime import datetime
from src.data_collection.bulk_downloader import SECBulkDownloader
from src.database.db_manager import get_db_manager
from src.database.models import Form4Filing, Company, Insider, Transaction

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_bulk_downloader():
    """Test the bulk downloader with a single year."""
    logger.info("🔍 Testing bulk downloader functionality...")
    
    # Initialize components
    downloader = SECBulkDownloader()
    db_manager = get_db_manager()
    
    # Test with a recent year that should have data
    test_year = 2020
    
    logger.info(f"📅 Testing with year {test_year}")
    
    # Step 1: Test bulk_download_year
    logger.info("Step 1: Testing bulk_download_year...")
    result = downloader.bulk_download_year(test_year)
    
    logger.info(f"Result: {result}")
    logger.info(f"Total filings found: {result.get('total_filings', 'N/A')}")
    logger.info(f"Processed quarters: {result.get('processed_quarters', 'N/A')}")
    logger.info(f"All filings count: {len(result.get('all_filings', []))}")
    
    if result.get('total_filings', 0) == 0:
        logger.error("❌ No filings found! This is the root issue.")
        return False
    
    # Step 2: Test download_year method
    logger.info("Step 2: Testing download_year method...")
    success = downloader.download_year(test_year, force=True)
    
    logger.info(f"Download year success: {success}")
    
    # Step 3: Check database
    logger.info("Step 3: Checking database...")
    
    with db_manager.get_session() as session:
        filing_count = session.query(Form4Filing).count()
        company_count = session.query(Company).count()
        insider_count = session.query(Insider).count()
        transaction_count = session.query(Transaction).count()
        
        logger.info(f"Database counts:")
        logger.info(f"  Filings: {filing_count}")
        logger.info(f"  Companies: {company_count}")
        logger.info(f"  Insiders: {insider_count}")
        logger.info(f"  Transactions: {transaction_count}")
    
    return success

def test_quarterly_data():
    """Test quarterly data retrieval."""
    logger.info("🔍 Testing quarterly data retrieval...")
    
    downloader = SECBulkDownloader()
    
    # Test with a specific year
    test_year = 2020
    
    # Get quarterly archives
    archives = downloader.get_quarterly_archives(test_year)
    
    logger.info(f"Found {len(archives)} quarterly archives for {test_year}")
    
    for archive in archives:
        logger.info(f"  Archive: {archive.year} Q{archive.quarter} - {archive.url}")
    
    if not archives:
        logger.error("❌ No quarterly archives found!")
        return False
    
    # Test downloading first quarter
    if archives:
        logger.info("Testing first quarter download...")
        quarter_data = downloader.download_quarterly_data(archives[0])
        
        logger.info(f"Quarter data: {quarter_data}")
        logger.info(f"Filings count: {quarter_data.get('count', 'N/A')}")
        logger.info(f"Filings list length: {len(quarter_data.get('filings', []))}")
        
        # Show first few filings
        filings = quarter_data.get('filings', [])
        if filings:
            logger.info("First few filings:")
            for i, filing in enumerate(filings[:3]):
                logger.info(f"  {i+1}: {filing}")
        
        return len(filings) > 0
    
    return False

def test_specific_filing():
    """Test downloading and storing a specific filing."""
    logger.info("🔍 Testing specific filing download...")
    
    downloader = SECBulkDownloader()
    
    # Create a sample filing info
    sample_filing = {
        'form_type': '4',
        'company_name': 'Test Company',
        'cik': '0000012345',
        'filing_date': datetime(2020, 1, 15).date(),
        'accession_number': '0000012345-20-000001',
        'file_url': 'https://www.sec.gov/Archives/edgar/data/12345/0000012345-20-000001.txt'
    }
    
    logger.info(f"Sample filing: {sample_filing}")
    
    # Test downloading single filing
    result = downloader._download_single_filing(sample_filing)
    
    logger.info(f"Download result: {result}")
    
    return result.get('status') == 'success'

if __name__ == "__main__":
    print("🔧 SEC Form 4 Bulk Downloader Diagnostic Script")
    print("=" * 60)
    
    try:
        # Test 1: Quarterly data retrieval
        print("\n1. Testing quarterly data retrieval...")
        if test_quarterly_data():
            print("✅ Quarterly data retrieval working")
        else:
            print("❌ Quarterly data retrieval failed")
        
        # Test 2: Bulk downloader
        print("\n2. Testing bulk downloader...")
        if test_bulk_downloader():
            print("✅ Bulk downloader working")
        else:
            print("❌ Bulk downloader failed")
        
        # Test 3: Specific filing
        print("\n3. Testing specific filing download...")
        if test_specific_filing():
            print("✅ Specific filing download working")
        else:
            print("❌ Specific filing download failed")
            
    except Exception as e:
        logger.error(f"❌ Error in diagnostic script: {e}")
        import traceback
        traceback.print_exc() 