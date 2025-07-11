"""
Bulk processor for coordinating multi-threaded downloading and parsing of Form 4 filings.
Manages queues and worker threads for efficient processing.
"""
import logging
import threading
import time
from queue import Queue, Empty
from typing import List, Dict, Optional, Callable, Any
from dataclasses import dataclass
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event
import traceback
from sqlalchemy.exc import IntegrityError

from tqdm import tqdm

from .edgar_downloader import EDGARDownloader, FilingInfo
from .form4_parser import Form4Parser, ParsedForm4
from ..database.db_manager import get_db_manager
from ..database.models import Form4Filing, Transaction, Insider, Company, InsiderRelationship

logger = logging.getLogger(__name__)


@dataclass
class ProcessingStats:
    """Statistics for bulk processing"""
    total_filings: int = 0
    downloaded: int = 0
    parsed: int = 0
    stored: int = 0
    errors: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class BulkProcessor:
    """
    Multi-threaded processing pipeline for Form 4 filings:
    1. Download queue (rate-limited)
    2. Parse queue (CPU-bound, parallelizable)
    3. Database insertion queue (batch inserts)
    """
    
    def __init__(self, config_path: str = "config/config.yaml", 
                 num_download_threads: int = 2, 
                 num_parse_threads: int = 4,
                 batch_size: int = 100):
        """
        Initialize bulk processor.
        
        Args:
            config_path: Path to configuration file
            num_download_threads: Number of download threads
            num_parse_threads: Number of parsing threads
            batch_size: Batch size for database operations
        """
        self.config_path = config_path
        self.db_manager = get_db_manager(config_path)
        self.config = self.db_manager.config
        
        # Thread configuration
        self.num_download_threads = num_download_threads
        self.num_parse_threads = num_parse_threads
        self.batch_size = batch_size
        
        # Components
        self.downloader = EDGARDownloader(config_path)
        self.parser = Form4Parser()
        
        # Queues
        self.download_queue = Queue()
        self.parse_queue = Queue()
        self.storage_queue = Queue()
        
        # Processing control
        self.stop_event = Event()
        self.stats = ProcessingStats()
        
        # Progress tracking
        self.progress_bar = None
        
        logger.info(f"Bulk processor initialized with {num_download_threads} download threads, "
                   f"{num_parse_threads} parse threads, batch size {batch_size}")
    
    def process_date_range(self, start_date: date, end_date: date, 
                          max_filings: Optional[int] = None) -> ProcessingStats:
        """
        Process all Form 4 filings in a date range.
        
        Args:
            start_date: Start date for processing
            end_date: End date for processing
            max_filings: Maximum number of filings to process (for testing)
            
        Returns:
            ProcessingStats object with processing results
        """
        logger.info(f"Starting bulk processing from {start_date} to {end_date}")
        
        # Reset stats
        self.stats = ProcessingStats(start_time=datetime.now())
        
        try:
            # Get list of all filings in the date range
            filings = list(self.downloader.get_historical_form4_list(start_date, end_date))
            
            # Limit filings if specified
            if max_filings and len(filings) > max_filings:
                filings = filings[:max_filings]
                logger.info(f"Limited to {max_filings} filings for testing")
            
            self.stats.total_filings = len(filings)
            logger.info(f"Found {len(filings)} Form 4 filings to process")
            
            if not filings:
                logger.info("No filings found in date range")
                return self.stats
            
            # Initialize progress bar
            self.progress_bar = tqdm(total=len(filings), desc="Processing Form 4 filings")
            
            # Start worker threads
            workers = self._start_workers()
            
            # Queue all filings for processing
            for filing in filings:
                self.download_queue.put(filing)
            
            # Wait for all processing to complete
            self._wait_for_completion(workers)
            
            # Final statistics
            self.stats.end_time = datetime.now()
            duration = self.stats.end_time - self.stats.start_time
            
            logger.info(f"Bulk processing completed in {duration}")
            logger.info(f"Results: {self.stats.downloaded} downloaded, {self.stats.parsed} parsed, "
                       f"{self.stats.stored} stored, {self.stats.errors} errors")
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Error in bulk processing: {e}")
            self.stats.end_time = datetime.now()
            raise
        finally:
            if self.progress_bar:
                self.progress_bar.close()
    
    def _start_workers(self) -> List[threading.Thread]:
        """Start all worker threads"""
        workers = []
        
        # Download workers
        for i in range(self.num_download_threads):
            worker = threading.Thread(
                target=self._download_worker,
                name=f"download-worker-{i}",
                daemon=True
            )
            worker.start()
            workers.append(worker)
        
        # Parse workers
        for i in range(self.num_parse_threads):
            worker = threading.Thread(
                target=self._parse_worker,
                name=f"parse-worker-{i}",
                daemon=True
            )
            worker.start()
            workers.append(worker)
        
        # Storage worker (single thread to avoid database conflicts)
        storage_worker = threading.Thread(
            target=self._storage_worker,
            name="storage-worker",
            daemon=True
        )
        storage_worker.start()
        workers.append(storage_worker)
        
        logger.info(f"Started {len(workers)} worker threads")
        return workers
    
    def _download_worker(self):
        """Worker thread for downloading filings"""
        while not self.stop_event.is_set():
            try:
                # Get next filing to download
                filing_info = self.download_queue.get(timeout=1)
                
                # Skip duplicate check here - handle it in storage to avoid race conditions
                
                # Download the filing
                xml_content = self.downloader.download_form4_filing(filing_info)
                
                if xml_content:
                    # Queue for parsing
                    self.parse_queue.put((filing_info, xml_content))
                    self.stats.downloaded += 1
                else:
                    logger.warning(f"Failed to download filing {filing_info.accession_number}")
                    self.stats.errors += 1
                
                self.download_queue.task_done()
                
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error in download worker: {e}")
                self.stats.errors += 1
                try:
                    self.download_queue.task_done()
                except:
                    pass
    
    def _parse_worker(self):
        """Worker thread for parsing filings"""
        while not self.stop_event.is_set():
            try:
                # Get next filing to parse
                filing_info, xml_content = self.parse_queue.get(timeout=1)
                
                # Parse the filing
                parsed_form4 = self.parser.parse_form4_xml(xml_content)
                
                if parsed_form4:
                    # Queue for storage
                    self.storage_queue.put((filing_info, xml_content, parsed_form4))
                    self.stats.parsed += 1
                else:
                    logger.warning(f"Failed to parse filing {filing_info.accession_number}")
                    self.stats.errors += 1
                
                self.parse_queue.task_done()
                
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error in parse worker: {e}")
                logger.error(traceback.format_exc())
                self.stats.errors += 1
                try:
                    self.parse_queue.task_done()
                except:
                    pass
    
    def _storage_worker(self):
        """Worker thread for storing parsed data in database"""
        batch = []
        
        while not self.stop_event.is_set():
            try:
                # Get next item to store
                filing_info, xml_content, parsed_form4 = self.storage_queue.get(timeout=1)
                batch.append((filing_info, xml_content, parsed_form4))
                
                # Process batch when full or timeout
                if len(batch) >= self.batch_size:
                    self._process_storage_batch(batch)
                    batch = []
                
                self.storage_queue.task_done()
                
            except Empty:
                # Process any remaining items in batch
                if batch:
                    self._process_storage_batch(batch)
                    batch = []
                continue
            except Exception as e:
                logger.error(f"Error in storage worker: {e}")
                logger.error(traceback.format_exc())
                self.stats.errors += 1
                try:
                    self.storage_queue.task_done()
                except:
                    pass
        
        # Process any remaining items
        if batch:
            self._process_storage_batch(batch)
    
    def _process_storage_batch(self, batch: List[tuple]):
        """Process a batch of parsed filings for storage"""
        for filing_info, xml_content, parsed_form4 in batch:
            try:
                # Process each filing in its own transaction to avoid rollback issues
                with self.db_manager.get_session() as session:
                    # Check if filing already exists (with database lock to prevent race conditions)
                    existing_filing = session.query(Form4Filing).filter(
                        Form4Filing.accession_number == filing_info.accession_number
                    ).first()
                    
                    if existing_filing:
                        # Filing already exists, skip it
                        continue
                    
                    # Store the filing and transactions
                    self._store_parsed_form4(session, filing_info, xml_content, parsed_form4)
                    self.stats.stored += 1
                    
                    # Update progress
                    if self.progress_bar:
                        self.progress_bar.update(1)
                        self.progress_bar.set_postfix({
                            'downloaded': self.stats.downloaded,
                            'parsed': self.stats.parsed,
                            'stored': self.stats.stored,
                            'errors': self.stats.errors
                        })
                
            except IntegrityError as e:
                # Handle database constraint violations
                error_msg = str(e)
                if "duplicate key value violates unique constraint" in error_msg:
                    logger.debug(f"Duplicate filing skipped: {filing_info.accession_number}")
                    # Don't count duplicates as errors since they're expected
                elif "StringDataRightTruncation" in error_msg or "value too long" in error_msg:
                    logger.error(f"Data truncation error for filing {filing_info.accession_number}: {e}")
                    self.stats.errors += 1
                else:
                    logger.error(f"Database integrity error for filing {filing_info.accession_number}: {e}")
                    self.stats.errors += 1
            except Exception as e:
                logger.error(f"Unexpected error storing filing {filing_info.accession_number}: {e}")
                self.stats.errors += 1
                
                # Update progress for failed items too
                if self.progress_bar:
                    self.progress_bar.update(1)
                    self.progress_bar.set_postfix({
                        'downloaded': self.stats.downloaded,
                        'parsed': self.stats.parsed,
                        'stored': self.stats.stored,
                        'errors': self.stats.errors
                    })
    
    def _store_parsed_form4(self, session, filing_info: FilingInfo, 
                           xml_content: str, parsed_form4: ParsedForm4):
        """Store parsed Form 4 data in database"""
        
        # Create or get company
        company = session.query(Company).filter(
            Company.cik == filing_info.company_cik
        ).first()
        
        if not company:
            company = Company(
                cik=filing_info.company_cik,
                name=parsed_form4.issuer.name or filing_info.company_name,
                ticker=parsed_form4.issuer.trading_symbol,
                last_updated=datetime.utcnow()
            )
            session.add(company)
            session.flush()  # Get the ID
        
        # Create or get insider
        insider = None
        if parsed_form4.reporting_owner.cik:
            insider = session.query(Insider).filter(
                Insider.cik == parsed_form4.reporting_owner.cik
            ).first()
            
            if not insider:
                insider = Insider(
                    cik=parsed_form4.reporting_owner.cik,
                    name=parsed_form4.reporting_owner.name,
                    last_updated=datetime.utcnow()
                )
                session.add(insider)
                session.flush()
        
        # Create Form 4 filing record
        form4_filing = Form4Filing(
            accession_number=filing_info.accession_number,
            filing_date=datetime.combine(filing_info.filing_date, datetime.min.time()),
            accepted_date=datetime.combine(parsed_form4.period_of_report, datetime.min.time()) 
                          if parsed_form4.period_of_report else None,
            insider_id=insider.id if insider else None,
            company_cik=filing_info.company_cik,
            reporting_owner_cik=parsed_form4.reporting_owner.cik,
            reporting_owner_name=parsed_form4.reporting_owner.name,
            reporting_owner_relationship=parsed_form4.reporting_owner.relationship,
            xml_content=xml_content,
            processed=True
        )
        session.add(form4_filing)
        session.flush()
        
        # Create insider relationship if needed
        if insider and parsed_form4.reporting_owner.relationship:
            existing_relationship = session.query(InsiderRelationship).filter(
                InsiderRelationship.insider_id == insider.id,
                InsiderRelationship.company_cik == company.cik
            ).first()
            
            if not existing_relationship:
                relationship = InsiderRelationship(
                    insider_id=insider.id,
                    company_cik=company.cik,
                    relationship_type=parsed_form4.reporting_owner.relationship,
                    title=parsed_form4.reporting_owner.officer_title,
                    is_active=True,
                    start_date=filing_info.filing_date
                )
                session.add(relationship)
        
        # Create transaction records
        transactions = self.parser.extract_transactions(parsed_form4)
        for trans_data in transactions:
            transaction = Transaction(
                filing_id=form4_filing.id,
                transaction_date=trans_data['transaction_date'],
                transaction_code=trans_data['transaction_code'],
                shares=trans_data['shares'],
                price_per_share=trans_data['price_per_share'],
                total_value=trans_data['total_value'],
                shares_owned_after=trans_data['shares_owned_after'],
                is_direct=trans_data['is_direct'],
                transaction_type=trans_data['transaction_type'],
                security_title=trans_data['security_title'],
                notes=trans_data['notes']
            )
            session.add(transaction)
    
    def _filing_exists(self, accession_number: str) -> bool:
        """Check if a filing already exists in the database"""
        with self.db_manager.get_session() as session:
            existing = session.query(Form4Filing).filter(
                Form4Filing.accession_number == accession_number
            ).first()
            return existing is not None
    
    def _wait_for_completion(self, workers: List[threading.Thread]):
        """Wait for all queues to be processed and workers to complete"""
        try:
            # Wait for download queue to be empty
            logger.info("Waiting for downloads to complete...")
            self.download_queue.join()
            
            # Wait for parse queue to be empty
            logger.info("Waiting for parsing to complete...")
            self.parse_queue.join()
            
            # Wait for storage queue to be empty
            logger.info("Waiting for storage to complete...")
            self.storage_queue.join()
            
            # Signal workers to stop
            self.stop_event.set()
            
            # Wait for all workers to finish
            for worker in workers:
                worker.join(timeout=5)
                if worker.is_alive():
                    logger.warning(f"Worker {worker.name} did not stop gracefully")
            
            logger.info("All workers completed")
            
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping workers...")
            self.stop_event.set()
            
            # Wait for workers to stop
            for worker in workers:
                worker.join(timeout=2)
            
            raise
    
    def process_recent_filings(self, days_back: int = 7) -> ProcessingStats:
        """
        Process recent Form 4 filings.
        
        Args:
            days_back: Number of days back to process
            
        Returns:
            ProcessingStats object
        """
        from datetime import timedelta
        
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        
        return self.process_date_range(start_date, end_date)
    
    def get_processing_status(self) -> Dict[str, Any]:
        """Get current processing status"""
        return {
            'total_filings': self.stats.total_filings,
            'downloaded': self.stats.downloaded,
            'parsed': self.stats.parsed,
            'stored': self.stats.stored,
            'errors': self.stats.errors,
            'download_queue_size': self.download_queue.qsize(),
            'parse_queue_size': self.parse_queue.qsize(),
            'storage_queue_size': self.storage_queue.qsize(),
            'is_running': not self.stop_event.is_set()
        }
    
    def cleanup(self):
        """Clean up resources"""
        self.stop_event.set()
        
        # Clear queues
        while not self.download_queue.empty():
            try:
                self.download_queue.get_nowait()
            except:
                break
        
        while not self.parse_queue.empty():
            try:
                self.parse_queue.get_nowait()
            except:
                break
        
        while not self.storage_queue.empty():
            try:
                self.storage_queue.get_nowait()
            except:
                break
        
        if self.progress_bar:
            self.progress_bar.close()
        
        logger.info("Bulk processor cleanup completed") 