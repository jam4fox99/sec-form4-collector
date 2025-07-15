"""
Database manager for the insider trading analysis system.
"""
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Dict, Any
import yaml
import os

from .models import Base

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize database manager with configuration"""
        self.config = self._load_config(config_path)
        self.engine = None
        self.Session = None
        self._initialize_database()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            return config
        except FileNotFoundError:
            logger.error(f"Config file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing config file: {e}")
            raise
    
    def _initialize_database(self):
        """Initialize database connection and session factory"""
        db_config = self.config['database']
        
        # Create connection string
        connection_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        )
        
        try:
            # Create engine with connection pooling
            self.engine = create_engine(
                connection_string,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                echo=False  # Set to True for SQL debugging
            )
            
            # Create session factory
            self.Session = sessionmaker(bind=self.engine)
            
            logger.info("Database connection initialized successfully")
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to initialize database connection: {e}")
            raise
    
    def create_tables(self):
        """Create all database tables"""
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to create database tables: {e}")
            raise
    
    def drop_tables(self):
        """Drop all database tables (use with caution!)"""
        try:
            Base.metadata.drop_all(self.engine)
            logger.info("Database tables dropped successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to drop database tables: {e}")
            raise
    
    @contextmanager
    def get_session(self) -> Session:
        """Context manager for database sessions"""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def get_session_factory(self) -> sessionmaker:
        """Get the session factory for use in other modules"""
        return self.Session
    
    def execute_sql(self, sql: str, params: Optional[Dict] = None) -> Any:
        """Execute raw SQL query"""
        with self.get_session() as session:
            try:
                result = session.execute(text(sql), params or {})
                return result.fetchall()
            except SQLAlchemyError as e:
                logger.error(f"SQL execution error: {e}")
                raise
    
    def health_check(self) -> bool:
        """Check database connectivity"""
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    def get_table_counts(self) -> Dict[str, int]:
        """Get row counts for all tables"""
        tables = {
            'companies': 'companies',
            'insiders': 'insiders',
            'insider_relationships': 'insider_relationships',
            'form4_filings': 'form4_filings',
            'transactions': 'transactions',
            'stock_prices': 'stock_prices',
            'insider_positions': 'insider_positions',
            'insider_performance': 'insider_performance',
            'downloaded_years': 'downloaded_years'
        }
        
        counts = {}
        with self.get_session() as session:
            for table_name, table_ref in tables.items():
                try:
                    result = session.execute(text(f"SELECT COUNT(*) FROM {table_ref}"))
                    counts[table_name] = result.scalar()
                except SQLAlchemyError as e:
                    logger.warning(f"Could not count rows in {table_name}: {e}")
                    counts[table_name] = -1
        
        return counts
    
    def vacuum_analyze(self):
        """Run VACUUM ANALYZE on all tables for performance optimization"""
        tables = [
            'companies', 'insiders', 'insider_relationships', 'form4_filings',
            'transactions', 'stock_prices', 'insider_positions', 'insider_performance',
            'downloaded_years'
        ]
        
        # Note: VACUUM cannot be run inside a transaction
        connection = self.engine.connect()
        connection.execute(text("COMMIT"))  # Close any open transaction
        
        for table in tables:
            try:
                connection.execute(text(f"VACUUM ANALYZE {table}"))
                logger.info(f"Vacuumed and analyzed table: {table}")
            except SQLAlchemyError as e:
                logger.warning(f"Could not vacuum table {table}: {e}")
        
        connection.close()
    
    def get_downloaded_years(self) -> Dict[int, str]:
        """Get all downloaded years and their status"""
        from .models import DownloadedYear
        
        with self.get_session() as session:
            years = session.query(DownloadedYear).all()
            return {year.year: year.status for year in years}
    
    def get_year_status(self, year: int) -> Optional[str]:
        """Get status of a specific year"""
        from .models import DownloadedYear
        
        with self.get_session() as session:
            year_record = session.query(DownloadedYear).filter(DownloadedYear.year == year).first()
            return year_record.status if year_record else None
    
    def set_year_status(self, year: int, status: str, **kwargs):
        """Set or update the status of a year"""
        from .models import DownloadedYear
        from datetime import datetime
        
        with self.get_session() as session:
            year_record = session.query(DownloadedYear).filter(DownloadedYear.year == year).first()
            
            if not year_record:
                year_record = DownloadedYear(year=year, status=status)
                session.add(year_record)
            else:
                year_record.status = status
            
            # Update additional fields if provided
            if 'total_filings' in kwargs:
                year_record.total_filings = kwargs['total_filings']
            if 'processed_quarters' in kwargs:
                year_record.processed_quarters = kwargs['processed_quarters']
            if 'downloaded_count' in kwargs:
                year_record.downloaded_count = kwargs['downloaded_count']
            if 'stored_count' in kwargs:
                year_record.stored_count = kwargs['stored_count']
            if 'error_count' in kwargs:
                year_record.error_count = kwargs['error_count']
            if 'skipped_count' in kwargs:
                year_record.skipped_count = kwargs['skipped_count']
            if 'error_message' in kwargs:
                year_record.error_message = kwargs['error_message']
            
            # Set timestamps based on status
            if status == 'in_progress' and not year_record.download_started:
                year_record.download_started = datetime.utcnow()
            elif status == 'completed':
                year_record.download_completed = datetime.utcnow()
            
            year_record.last_updated = datetime.utcnow()
    
    def is_year_downloaded(self, year: int) -> bool:
        """Check if a year has been completely downloaded"""
        status = self.get_year_status(year)
        return status == 'completed'


# Global database manager instance
db_manager = None


def get_db_manager(config_path: str = "config/config.yaml") -> DatabaseManager:
    """Get global database manager instance"""
    global db_manager
    if db_manager is None:
        db_manager = DatabaseManager(config_path)
    return db_manager


def init_database(config_path: str = "config/config.yaml"):
    """Initialize database with tables"""
    manager = get_db_manager(config_path)
    manager.create_tables()
    return manager 