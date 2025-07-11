"""
EDGAR Bulk Data Downloader for Form 4 filings.
Handles downloading from SEC EDGAR database with rate limiting and error handling.
"""
import requests
import logging
import time
import zipfile
import io
import os
import json
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple, Generator
from pathlib import Path
from urllib.parse import urljoin
import csv
from dataclasses import dataclass

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm

from ..utils.rate_limiter import get_rate_limiter
from ..database.db_manager import get_db_manager
from ..database.models import Company, Form4Filing

logger = logging.getLogger(__name__)


@dataclass
class FilingInfo:
    """Information about a Form 4 filing"""
    accession_number: str
    filing_date: date
    company_cik: str
    company_name: str
    form_type: str
    file_url: str


class EDGARDownloader:
    """
    Downloads historical Form 4 data from SEC EDGAR database.
    Respects SEC rate limits and handles errors gracefully.
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize EDGAR downloader with configuration"""
        self.db_manager = get_db_manager(config_path)
        self.config = self.db_manager.config
        
        # SEC EDGAR configuration
        self.base_url = self.config['edgar']['base_url']
        self.user_agent = self.config['edgar']['user_agent']
        self.rate_limiter = get_rate_limiter(
            max_requests=self.config['edgar']['rate_limit']['max_requests'],
            time_window=self.config['edgar']['rate_limit']['time_window']
        )
        
        # Data paths
        self.raw_data_path = Path(self.config['paths']['raw_data'])
        self.cache_path = Path(self.config['paths']['cache'])
        self.processed_data_path = Path(self.config['paths']['processed_data'])
        
        # Create directories if they don't exist
        self.raw_data_path.mkdir(parents=True, exist_ok=True)
        self.cache_path.mkdir(parents=True, exist_ok=True)
        self.processed_data_path.mkdir(parents=True, exist_ok=True)
        
        # HTTP session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        logger.info(f"EDGAR Downloader initialized with base URL: {self.base_url}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout))
    )
    def _make_request(self, url: str, **kwargs) -> requests.Response:
        """
        Make a rate-limited HTTP request with retry logic.
        
        Args:
            url: URL to request
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
        """
        self.rate_limiter.wait_if_needed()
        
        try:
            response = self.session.get(url, timeout=30, **kwargs)
            response.raise_for_status()
            
            # Handle rate limiting from adaptive rate limiter
            if hasattr(self.rate_limiter, 'handle_successful_request'):
                self.rate_limiter.handle_successful_request()
            
            return response
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Too Many Requests
                logger.warning(f"Rate limit exceeded for URL: {url}")
                if hasattr(self.rate_limiter, 'handle_rate_limit_exceeded'):
                    self.rate_limiter.handle_rate_limit_exceeded()
                time.sleep(5)  # Additional wait for 429 errors
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for URL {url}: {e}")
            raise
    
    def download_company_tickers(self) -> Dict[str, Dict]:
        """
        Download company tickers JSON file from SEC.
        
        Returns:
            Dictionary mapping CIK to company information
        """
        url = "https://www.sec.gov/files/company_tickers.json"
        cache_file = self.cache_path / "company_tickers.json"
        
        # Check if we have a cached version less than 24 hours old
        if cache_file.exists():
            file_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            if file_age.total_seconds() < 24 * 3600:  # 24 hours
                logger.info("Using cached company tickers")
                with open(cache_file, 'r') as f:
                    return json.load(f)
        
        logger.info("Downloading company tickers from SEC")
        response = self._make_request(url)
        
        # Parse and restructure the data
        tickers_data = response.json()
        companies = {}
        
        for entry in tickers_data.values():
            cik = str(entry['cik_str']).zfill(10)
            companies[cik] = {
                'cik': cik,
                'ticker': entry['ticker'],
                'title': entry['title']
            }
        
        # Cache the results
        with open(cache_file, 'w') as f:
            json.dump(companies, f, indent=2)
        
        logger.info(f"Downloaded {len(companies)} company tickers")
        return companies
    
    def get_daily_index_url(self, date_obj: date) -> str:
        """
        Get the URL for a daily index file.
        
        Args:
            date_obj: Date for the index file
            
        Returns:
            URL for the daily index file
        """
        year = date_obj.year
        quarter = f"QTR{(date_obj.month - 1) // 3 + 1}"
        date_str = date_obj.strftime("%Y%m%d")
        
        return f"{self.base_url}/daily-index/{year}/{quarter}/form.{date_str}.idx"
    
    def download_daily_index(self, date_obj: date) -> List[FilingInfo]:
        """
        Download and parse daily index file for a specific date.
        
        Args:
            date_obj: Date for the index file
            
        Returns:
            List of Form 4 filings for that date
        """
        url = self.get_daily_index_url(date_obj)
        
        try:
            response = self._make_request(url)
            content = response.text
            
            # Parse the index file
            lines = content.strip().split('\n')
            
            # Find the start of the data (after the header)
            data_start = 0
            for i, line in enumerate(lines):
                if line.startswith('Form Type'):
                    data_start = i + 2  # Skip header and separator line
                    break
            
            form4_filings = []
            for line in lines[data_start:]:
                if not line.strip():
                    continue
                
                # Parse the fixed-width line format
                # Format: Form Type(12) Company Name(62) CIK(12) Date(8) File Name(rest)
                if len(line) < 90:  # Minimum line length
                    continue
                
                form_type = line[:12].strip()
                if form_type == '4':  # Form 4
                    company_name = line[12:74].strip()
                    cik = line[74:86].strip().zfill(10)
                    date_str = line[86:94].strip()
                    file_path = line[94:].strip()
                    
                    # Parse the date (YYYYMMDD format)
                    try:
                        filing_date = datetime.strptime(date_str, '%Y%m%d').date()
                    except ValueError:
                        continue  # Skip invalid date formats
                    
                    # Construct full URL
                    file_url = urljoin(self.base_url, file_path)
                    accession_number = file_path.split('/')[-1].replace('.txt', '')
                    
                    form4_filings.append(FilingInfo(
                        accession_number=accession_number,
                        filing_date=filing_date,
                        company_cik=cik,
                        company_name=company_name,
                        form_type=form_type,
                        file_url=file_url
                    ))
            
            logger.info(f"Found {len(form4_filings)} Form 4 filings for {date_obj}")
            return form4_filings
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.info(f"No index file found for {date_obj} (likely weekend/holiday)")
                return []
            else:
                logger.error(f"Error downloading index for {date_obj}: {e}")
                raise
        except Exception as e:
            logger.error(f"Error parsing index for {date_obj}: {e}")
            return []
    
    def download_form4_filing(self, filing_info: FilingInfo) -> Optional[str]:
        """
        Download individual Form 4 filing XML.
        
        Args:
            filing_info: Information about the filing to download
            
        Returns:
            XML content as string, or None if failed
        """
        try:
            response = self._make_request(filing_info.file_url)
            
            # The filing might be a complete submission with multiple documents
            # We need to extract the Form 4 XML from it
            content = response.text
            
            # Look for the Form 4 XML document
            if '<ownershipDocument>' in content:
                # This is the XML we want
                start = content.find('<ownershipDocument>')
                end = content.find('</ownershipDocument>') + len('</ownershipDocument>')
                if start != -1 and end != -1:
                    xml_content = content[start:end]
                    return xml_content
            
            # If not found, return the whole content (might be pre-XML format)
            return content
            
        except Exception as e:
            logger.error(f"Error downloading Form 4 filing {filing_info.accession_number}: {e}")
            return None
    
    def get_historical_form4_list(self, start_date: date, end_date: date) -> Generator[FilingInfo, None, None]:
        """
        Get list of all Form 4 filings between start and end dates.
        
        Args:
            start_date: Start date for collection
            end_date: End date for collection
            
        Yields:
            FilingInfo objects for each Form 4 filing
        """
        current_date = start_date
        total_filings = 0
        
        logger.info(f"Collecting Form 4 filings from {start_date} to {end_date}")
        
        while current_date <= end_date:
            # Skip weekends (SEC doesn't publish on weekends)
            if current_date.weekday() < 5:  # Monday = 0, Sunday = 6
                try:
                    daily_filings = self.download_daily_index(current_date)
                    for filing in daily_filings:
                        total_filings += 1
                        yield filing
                except Exception as e:
                    logger.error(f"Error processing {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        logger.info(f"Total Form 4 filings found: {total_filings}")
    
    def bulk_download_form4s(self, start_date: date, end_date: date, 
                           max_filings: Optional[int] = None) -> int:
        """
        Bulk download Form 4 filings and store in database.
        
        Args:
            start_date: Start date for collection
            end_date: End date for collection
            max_filings: Maximum number of filings to download (for testing)
            
        Returns:
            Number of filings successfully downloaded
        """
        downloaded_count = 0
        error_count = 0
        
        # Get all Form 4 filings in the date range
        filing_generator = self.get_historical_form4_list(start_date, end_date)
        
        # If max_filings is specified, limit the generator
        if max_filings:
            filing_list = list(filing_generator)[:max_filings]
            filing_generator = iter(filing_list)
        
        # Progress bar
        pbar = tqdm(desc="Downloading Form 4 filings", unit="filings")
        
        for filing_info in filing_generator:
            try:
                # Check if we already have this filing
                with self.db_manager.get_session() as session:
                    existing = session.query(Form4Filing).filter(
                        Form4Filing.accession_number == filing_info.accession_number
                    ).first()
                    
                    if existing:
                        pbar.update(1)
                        continue
                
                # Download the filing
                xml_content = self.download_form4_filing(filing_info)
                
                if xml_content:
                    # Store in database
                    with self.db_manager.get_session() as session:
                        # Create or get company
                        company = session.query(Company).filter(
                            Company.cik == filing_info.company_cik
                        ).first()
                        
                        if not company:
                            company = Company(
                                cik=filing_info.company_cik,
                                name=filing_info.company_name,
                                last_updated=datetime.utcnow()
                            )
                            session.add(company)
                        
                        # Create Form 4 filing record
                        form4_filing = Form4Filing(
                            accession_number=filing_info.accession_number,
                            filing_date=datetime.combine(filing_info.filing_date, datetime.min.time()),
                            company_cik=filing_info.company_cik,
                            xml_content=xml_content,
                            processed=False
                        )
                        session.add(form4_filing)
                        session.commit()
                    
                    downloaded_count += 1
                    pbar.set_postfix({
                        'downloaded': downloaded_count,
                        'errors': error_count
                    })
                else:
                    error_count += 1
                
                pbar.update(1)
                
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing filing {filing_info.accession_number}: {e}")
                pbar.update(1)
                continue
        
        pbar.close()
        
        logger.info(f"Bulk download completed: {downloaded_count} downloaded, {error_count} errors")
        return downloaded_count
    
    def download_recent_filings(self, days_back: int = 7) -> int:
        """
        Download recent Form 4 filings from the last N days.
        
        Args:
            days_back: Number of days back to check for filings
            
        Returns:
            Number of filings downloaded
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"Downloading recent Form 4 filings from {start_date} to {end_date}")
        return self.bulk_download_form4s(start_date, end_date)
    
    def get_download_stats(self) -> Dict[str, int]:
        """
        Get statistics about downloaded filings.
        
        Returns:
            Dictionary with download statistics
        """
        with self.db_manager.get_session() as session:
            total_filings = session.query(Form4Filing).count()
            processed_filings = session.query(Form4Filing).filter(
                Form4Filing.processed == True
            ).count()
            unprocessed_filings = total_filings - processed_filings
            
            return {
                'total_filings': total_filings,
                'processed_filings': processed_filings,
                'unprocessed_filings': unprocessed_filings
            }
    
    def cleanup_failed_downloads(self) -> int:
        """
        Clean up filings that failed to download properly.
        
        Returns:
            Number of records cleaned up
        """
        with self.db_manager.get_session() as session:
            # Find filings with no XML content
            failed_filings = session.query(Form4Filing).filter(
                Form4Filing.xml_content.is_(None) | 
                (Form4Filing.xml_content == '')
            ).all()
            
            for filing in failed_filings:
                session.delete(filing)
            
            session.commit()
            
            logger.info(f"Cleaned up {len(failed_filings)} failed downloads")
            return len(failed_filings)
    
    def __del__(self):
        """Clean up resources"""
        if hasattr(self, 'session'):
            self.session.close() 