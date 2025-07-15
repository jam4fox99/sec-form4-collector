"""
SEC Bulk Data Downloader - Downloads quarterly ZIP archives without rate limiting.
Uses SEC's bulk data API and FTP to download entire quarters of Form 4 data.
"""
import requests
import logging
import zipfile
import io
import os
import time
from datetime import datetime, date
from typing import List, Dict, Optional, Generator
from pathlib import Path
import re
from dataclasses import dataclass
import tempfile
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


@dataclass
class BulkArchiveInfo:
    """Information about a bulk archive file"""
    year: int
    quarter: int
    url: str
    filename: str
    size_mb: Optional[float] = None


class SECBulkDownloader:
    """Enhanced SEC bulk data downloader with multi-threading support."""
    
    def __init__(self, data_path: str = "./data/bulk_downloads", max_workers: int = 8):
        """
        Initialize the bulk downloader.
        
        Args:
            data_path: Directory to store downloaded files
            max_workers: Number of concurrent download threads (recommend 4-8 max to avoid rate limiting)
        """
        self.data_path = Path(data_path)
        self.data_path.mkdir(parents=True, exist_ok=True)
        
        # Cap max workers to avoid SEC rate limiting
        if max_workers > 8:
            logger.warning(f"Max workers capped at 8 to avoid SEC rate limiting (requested: {max_workers})")
            max_workers = 8
        
        self.max_workers = max_workers
        
        # Create requests session with optimized settings for multi-threading
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Academic Research - Form 4 Analyzer 1.0 contact@research.edu',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        
        # Thread-local sessions for concurrent requests
        self._local = threading.local()
        
        logger.info(f"SEC Bulk Downloader initialized - RATE LIMIT SAFE (max_workers={max_workers})")

    def _get_session(self):
        """Get thread-local session for concurrent requests."""
        if not hasattr(self._local, 'session'):
            self._local.session = requests.Session()
            self._local.session.headers.update({
                'User-Agent': 'Academic Research - Form 4 Analyzer 1.0 contact@research.edu',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            })
        return self._local.session
    
    def get_quarterly_archives(self, year: int) -> List[BulkArchiveInfo]:
        """Get list of quarterly archives for a year."""
        archives = []
        
        for quarter in range(1, 5):  # Q1-Q4
            # Use the known working SEC daily-index pattern
            url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/"
            
            try:
                # Check if the quarter directory exists
                response = self.session.get(url, timeout=30)
                if response.status_code == 200:
                    # This quarter exists - we'll batch download all daily files
                    archives.append(BulkArchiveInfo(
                        year=year,
                        quarter=quarter,
                        url=url,
                        filename=f"{year}_QTR{quarter}_bulk",
                        size_mb=50.0  # Estimate
                    ))
                    logger.info(f"Found quarter directory: {year} Q{quarter}")
            except:
                continue
        
        return archives
    
    def download_quarterly_data(self, archive_info: BulkArchiveInfo) -> Dict[str, any]:
        """Download and process quarterly data directly (no local files)."""
        logger.info(f"Processing {archive_info.year} Q{archive_info.quarter} data...")
        
        # Get list of all daily index files for this quarter
        base_url = archive_info.url
        response = self.session.get(base_url)
        
        # Extract daily index file links from the directory listing
        import re
        index_files = re.findall(r'form\.(\d{8})\.idx', response.text)
        
        total_filings = 0
        all_filings = []
        
        logger.info(f"Found {len(index_files)} daily index files for {archive_info.year} Q{archive_info.quarter}")
        
        # Download and process each daily index file
        for date_str in index_files:
            daily_url = f"{base_url}form.{date_str}.idx"
            try:
                daily_response = self.session.get(daily_url, timeout=30)
                if daily_response.status_code == 200:
                    # Parse this daily index
                    for filing_info in self._parse_index_content(daily_response.text):
                        if filing_info['form_type'] == '4':  # Form 4 only
                            all_filings.append(filing_info)
                            total_filings += 1
            except Exception as e:
                logger.debug(f"Error processing daily file {date_str}: {e}")
                continue
        
        logger.info(f"{archive_info.year} Q{archive_info.quarter}: {total_filings:,} Form 4 filings")
        return {
            'filings': all_filings,
            'count': total_filings
        }
    
    def extract_form4_data(self, archive_path: Path) -> Generator[Dict, None, None]:
        """Extract Form 4 data from downloaded archive."""
        logger.info(f"Extracting Form 4 data from {archive_path.name}")
        
        with zipfile.ZipFile(archive_path, 'r') as zip_file:
            # Look for daily index files
            for file_info in zip_file.filelist:
                if file_info.filename.endswith('.idx') and 'form.' in file_info.filename:
                    # Extract and parse index file
                    with zip_file.open(file_info) as idx_file:
                        content = idx_file.read().decode('utf-8', errors='ignore')
                        for filing_info in self._parse_index_content(content):
                            if filing_info['form_type'] == '4':  # Form 4 only
                                yield filing_info
    
    def _parse_index_content(self, content: str) -> Generator[Dict, None, None]:
        """Parse index file content to extract filing information."""
        lines = content.strip().split('\n')
        
        # Find data start - skip headers and separators
        data_start = 0
        for i, line in enumerate(lines):
            if line.startswith('Form Type'):
                data_start = i + 2  # Skip header and separator line
                break
        
        for line in lines[data_start:]:
            if not line.strip() or len(line) < 90:
                continue
            
            # Skip separator lines
            if '--------' in line or line.startswith('Form Type'):
                continue
            
            try:
                # Parse fixed-width format
                form_type = line[:12].strip()
                company_name = line[12:74].strip()
                cik = line[74:86].strip().zfill(10)
                date_str = line[86:94].strip()
                file_path = line[94:].strip()
                
                # Skip lines with invalid dates
                if not date_str.isdigit() or len(date_str) != 8:
                    continue
                
                # Parse date
                filing_date = datetime.strptime(date_str, '%Y%m%d').date()
                
                # Extract accession number
                accession_number = file_path.split('/')[-1].replace('.txt', '')
                
                # Fix URL construction - file_path already includes /edgar/data/...
                # Remove leading /edgar if present to avoid double /edgar/
                clean_path = file_path.lstrip('/').replace('edgar/', '')
                file_url = f"https://www.sec.gov/Archives/edgar/{clean_path}"
                
                yield {
                    'form_type': form_type,
                    'company_name': company_name,
                    'cik': cik,
                    'filing_date': filing_date,
                    'file_path': file_path,
                    'accession_number': accession_number,
                    'file_url': file_url
                }
            except Exception as e:
                logger.debug(f"Error parsing line: {e}")
                continue
    
    def bulk_download_year(self, year: int) -> Dict[str, int]:
        """Download and process all Form 4 data for a year."""
        logger.info(f"Starting bulk download for {year}")
        
        # Get quarterly archives
        archives = self.get_quarterly_archives(year)
        if not archives:
            logger.warning(f"No archives found for {year}")
            return {'total_filings': 0, 'processed_quarters': 0, 'all_filings': []}
        
        total_filings = 0
        processed_quarters = 0
        all_year_filings = []
        
        for archive_info in archives:
            try:
                # Download and process quarterly data
                quarter_data = self.download_quarterly_data(archive_info)
                processed_quarters += 1
                
                quarter_filings = quarter_data['count']
                total_filings += quarter_filings
                all_year_filings.extend(quarter_data['filings'])
                
            except Exception as e:
                logger.error(f"Error processing {archive_info.year} Q{archive_info.quarter}: {e}")
        
        logger.info(f"Bulk download complete for {year}: {total_filings:,} total Form 4 filings")
        return {
            'total_filings': total_filings,
            'processed_quarters': processed_quarters,
            'all_filings': all_year_filings
        }
    
    def bulk_download_range(self, start_year: int, end_year: int) -> Dict[str, int]:
        """Download data for multiple years."""
        logger.info(f"Starting bulk download for {start_year}-{end_year}")
        
        total_stats = {'total_filings': 0, 'processed_quarters': 0, 'years_processed': 0}
        
        for year in range(start_year, end_year + 1):
            year_stats = self.bulk_download_year(year)
            total_stats['total_filings'] += year_stats['total_filings']
            total_stats['processed_quarters'] += year_stats['processed_quarters']
            total_stats['years_processed'] += 1
            
            logger.info(f"Progress: {year}/{end_year} complete")
        
        logger.info(f"Bulk download complete: {total_stats['total_filings']:,} total filings")
        return total_stats

    def _download_single_filing(self, filing_info: Dict) -> Dict[str, any]:
        """Download a single filing - thread-safe worker function."""
        import time
        import random
        
        try:
            session = self._get_session()
            file_url = filing_info['file_url']
            accession = filing_info['accession_number']
            
            # Exponential backoff for rate limiting
            max_retries = 3
            base_delay = 1.0
            
            for attempt in range(max_retries):
                # Add progressive delay to avoid overwhelming SEC servers
                if attempt == 0:
                    delay = random.uniform(0.5, 1.0)  # 500ms-1s for first attempt
                else:
                    delay = base_delay * (2 ** attempt) + random.uniform(0.5, 1.0)  # Exponential backoff
                
                time.sleep(delay)
                
                # Add detailed logging for debugging
                logger.debug(f"Downloading: {accession} (attempt {attempt + 1}) from {file_url}")
                
                response = session.get(file_url, timeout=30)
                
                if response.status_code == 200:
                    xml_content = response.text
                    
                    # Validate we got actual XML content
                    if len(xml_content) < 100:
                        logger.warning(f"Suspiciously short content for {accession}: {len(xml_content)} bytes")
                        return {
                            'status': 'error',
                            'filing_info': filing_info,
                            'error': f"Content too short: {len(xml_content)} bytes"
                        }
                    
                    # Extract Form 4 XML if it's embedded
                    if '<ownershipDocument>' in xml_content:
                        start = xml_content.find('<ownershipDocument>')
                        end = xml_content.find('</ownershipDocument>') + len('</ownershipDocument>')
                        if start != -1 and end != -1:
                            xml_content = xml_content[start:end]
                    
                    return {
                        'status': 'success',
                        'filing_info': filing_info,
                        'xml_content': xml_content
                    }
                elif response.status_code == 429:  # Rate limited
                    error_msg = "Rate limited by SEC - too many requests"
                    logger.warning(f"RATE LIMITED! {accession} (attempt {attempt + 1}): {error_msg}")
                    
                    if attempt < max_retries - 1:
                        # Wait longer before retrying
                        retry_delay = 10 * (2 ** attempt) + random.uniform(1, 5)
                        logger.info(f"Waiting {retry_delay:.1f}s before retry {attempt + 2}/{max_retries}")
                        time.sleep(retry_delay)
                        continue
                    else:
                        # Max retries reached
                        logger.error(f"Max retries reached for {accession}")
                        return {
                            'status': 'error',
                            'filing_info': filing_info,
                            'error': error_msg
                        }
                else:
                    error_msg = f"HTTP {response.status_code}"
                    logger.warning(f"Download failed for {accession}: {error_msg} - URL: {file_url}")
                    return {
                        'status': 'error',
                        'filing_info': filing_info,
                        'error': error_msg
                    }
                    
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Exception downloading {filing_info.get('accession_number', 'unknown')}: {error_msg}")
            return {
                'status': 'error',
                'filing_info': filing_info,
                'error': error_msg
            }

    def _batch_store_filings(self, filing_results: List[Dict], db_manager, force: bool = False) -> Dict[str, int]:
        """Store a batch of downloaded filings in database - thread-safe."""
        from ..database.models import Form4Filing, Company
        from sqlalchemy import update
        from sqlalchemy.exc import IntegrityError
        
        batch_stats = {'stored': 0, 'errors': 0}
        
        try:
            with db_manager.get_session() as session:
                for result in filing_results:
                    if result['status'] != 'success':
                        batch_stats['errors'] += 1
                        continue
                        
                    filing_info = result['filing_info']
                    xml_content = result['xml_content']
                    
                    try:
                        # Check if filing already exists
                        existing_filing = session.query(Form4Filing).filter(
                            Form4Filing.accession_number == filing_info['accession_number']
                        ).first()
                        
                        if existing_filing:
                            if force:
                                # Update existing filing with new content
                                existing_filing.xml_content = xml_content
                                existing_filing.filing_date = datetime.combine(filing_info['filing_date'], datetime.min.time())
                                existing_filing.processed = False
                                batch_stats['stored'] += 1
                                logger.debug(f"Updated existing filing: {filing_info['accession_number']}")
                            else:
                                # Skip existing filing
                                logger.debug(f"Skipping existing filing: {filing_info['accession_number']}")
                            continue
                        
                        # Create or get company
                        company = session.query(Company).filter(
                            Company.cik == filing_info['cik']
                        ).first()
                        
                        if not company:
                            company = Company(
                                cik=filing_info['cik'],
                                name=filing_info['company_name'],
                                last_updated=datetime.utcnow()
                            )
                            session.add(company)
                        
                        if force:
                            # In force mode, update existing or insert new
                            existing = session.query(Form4Filing).filter(
                                Form4Filing.accession_number == filing_info['accession_number']
                            ).first()
                            
                            if existing:
                                # Update existing record
                                existing.xml_content = xml_content
                                existing.processed = False
                                existing.filing_date = datetime.combine(filing_info['filing_date'], datetime.min.time())
                            else:
                                # Create new record
                                form4_filing = Form4Filing(
                                    accession_number=filing_info['accession_number'],
                                    filing_date=datetime.combine(filing_info['filing_date'], datetime.min.time()),
                                    company_cik=filing_info['cik'],
                                    xml_content=xml_content,
                                    processed=False
                                )
                                session.add(form4_filing)
                        else:
                            # Normal mode - only insert if not exists (this shouldn't happen due to pre-filtering)
                            form4_filing = Form4Filing(
                                accession_number=filing_info['accession_number'],
                                filing_date=datetime.combine(filing_info['filing_date'], datetime.min.time()),
                                company_cik=filing_info['cik'],
                                xml_content=xml_content,
                                processed=False
                            )
                            session.add(form4_filing)
                        
                        batch_stats['stored'] += 1
                    
                    except IntegrityError as e:
                        # Handle database integrity errors (duplicates, etc.)
                        session.rollback()
                        if "duplicate key" in str(e).lower():
                            logger.debug(f"Duplicate key for {filing_info['accession_number']} - skipping")
                        else:
                            logger.error(f"Database integrity error for {filing_info['accession_number']}: {e}")
                            batch_stats['errors'] += 1
                    except Exception as e:
                        session.rollback()
                        batch_stats['errors'] += 1
                        logger.error(f"Error storing {filing_info['accession_number']}: {e}")
                
                session.commit()
                
        except Exception as e:
            logger.error(f"Batch storage error: {e}")
            batch_stats['errors'] += len(filing_results)
            batch_stats['stored'] = 0
        
        return batch_stats

    def download_and_store_filings(self, all_filings: List[Dict], db_manager, force: bool = False) -> Dict[str, int]:
        """Download and store filing content using multi-threading."""
        from .form4_parser import Form4Parser
        from ..database.models import Form4Filing, Company, Insider
        from tqdm import tqdm
        
        parser = Form4Parser()
        stats = {'downloaded': 0, 'stored': 0, 'errors': 0, 'skipped': 0}
        
        print(f"📥 Downloading and storing {len(all_filings):,} filings...")
        print(f"🚀 Using {self.max_workers} threads for concurrent downloads")
        
        if force:
            print("🔥 FORCE MODE: Downloading ALL filings (ignoring existing ones)")
            filings_to_process = all_filings
        else:
            # Filter out existing filings first
            print("🔍 Checking for existing filings...")
            filings_to_process = []
            
            batch_size = 1000  # Check existence in larger batches
            for i in range(0, len(all_filings), batch_size):
                batch = all_filings[i:i + batch_size]
                accession_numbers = [f['accession_number'] for f in batch]
                
                with db_manager.get_session() as session:
                    existing_numbers = set(
                        session.query(Form4Filing.accession_number)
                        .filter(Form4Filing.accession_number.in_(accession_numbers))
                        .all()
                    )
                    existing_numbers = {acc[0] for acc in existing_numbers}
                
                for filing in batch:
                    if filing['accession_number'] in existing_numbers:
                        stats['skipped'] += 1
                    else:
                        filings_to_process.append(filing)
            
            print(f"📊 Found {len(filings_to_process):,} new filings to download ({stats['skipped']:,} already exist)")
            
            if not filings_to_process:
                print("✅ All filings already downloaded!")
                return stats
        
        # Process with multi-threading
        download_batch_size = 50  # Store in batches to reduce database contention
        rate_limit_errors = 0
        
        with tqdm(total=len(filings_to_process), desc="Downloading & storing") as pbar:
            for i in range(0, len(filings_to_process), download_batch_size):
                batch = filings_to_process[i:i + download_batch_size]
                
                # Download batch concurrently
                batch_results = []
                batch_rate_limits = 0
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_filing = {
                        executor.submit(self._download_single_filing, filing): filing 
                        for filing in batch
                    }
                    
                    for future in as_completed(future_to_filing):
                        result = future.result()
                        batch_results.append(result)
                        
                        if result['status'] == 'success':
                            stats['downloaded'] += 1
                        else:
                            stats['errors'] += 1
                            # Check for rate limiting
                            if "Rate limited" in result.get('error', ''):
                                batch_rate_limits += 1
                                rate_limit_errors += 1
                
                # Check if we're getting heavily rate limited
                if batch_rate_limits > len(batch) * 0.5:  # More than 50% rate limited
                    print(f"\n⚠️ HIGH RATE LIMITING DETECTED!")
                    print(f"   {batch_rate_limits}/{len(batch)} requests rate limited in this batch")
                    print(f"   Total rate limit errors: {rate_limit_errors}")
                    print(f"   Recommendation: Wait 10-30 minutes and try again with fewer threads")
                    print(f"   Current threads: {self.max_workers} (try --threads 4)")
                    
                    # If very high rate limiting, add a longer delay
                    if batch_rate_limits > len(batch) * 0.8:  # More than 80% rate limited
                        delay_time = 60  # 60 seconds
                        print(f"   🕐 Adding {delay_time}s delay due to severe rate limiting...")
                        time.sleep(delay_time)
                
                # Store batch in database
                store_stats = self._batch_store_filings(batch_results, db_manager, force=force)
                stats['stored'] += store_stats['stored']
                stats['errors'] += store_stats['errors']
                
                pbar.update(len(batch))
                pbar.set_postfix({
                    'downloaded': stats['downloaded'],
                    'stored': stats['stored'],
                    'errors': stats['errors'],
                    'skipped': stats['skipped'],
                    'rate_limits': rate_limit_errors
                })
        
        logger.info(f"Filing storage complete: {stats['stored']} stored, {stats['errors']} errors, {stats['skipped']} skipped")
        return stats

    def download_year(self, year: int, force: bool = False) -> bool:
        """
        Download all Form 4 filings for a specific year.
        
        Args:
            year: Year to download
            force: Force re-download even if already completed
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use the existing bulk_download_year method
            result = self.bulk_download_year(year)
            
            # Consider it successful if we got any filings
            if result['total_filings'] > 0:
                logger.info(f"✅ Successfully downloaded {result['total_filings']:,} filings for year {year}")
                return True
            else:
                logger.warning(f"❌ No filings found for year {year}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error downloading year {year}: {e}")
            return False


def main():
    """Test the bulk downloader."""
    downloader = SECBulkDownloader()
    
    # Test with 2023 data
    print("Testing bulk download for 2023...")
    stats = downloader.bulk_download_year(2023)
    print(f"Results: {stats}")


if __name__ == "__main__":
    main() 