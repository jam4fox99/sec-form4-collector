"""
Database models for the insider trading analysis system.
"""
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, Date, ForeignKey, BigInteger
from sqlalchemy.types import Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class Company(Base):
    """Company information from SEC filings"""
    __tablename__ = 'companies'
    
    cik = Column(String(10), primary_key=True)
    ticker = Column(String(10), index=True)
    name = Column(String(255))
    sic = Column(String(4))
    last_updated = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    form4_filings = relationship("Form4Filing", back_populates="company")
    insider_relationships = relationship("InsiderRelationship", back_populates="company")
    insider_positions = relationship("InsiderPosition", back_populates="company")


class Insider(Base):
    """Individual insider information"""
    __tablename__ = 'insiders'
    
    id = Column(Integer, primary_key=True)
    cik = Column(String(10), unique=True, index=True)
    name = Column(String(255))
    last_updated = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    form4_filings = relationship("Form4Filing", back_populates="insider")
    insider_relationships = relationship("InsiderRelationship", back_populates="insider")
    insider_positions = relationship("InsiderPosition", back_populates="insider")
    performance_records = relationship("InsiderPerformance", back_populates="insider")


class InsiderRelationship(Base):
    """Relationship between insiders and companies"""
    __tablename__ = 'insider_relationships'
    
    id = Column(Integer, primary_key=True)
    insider_id = Column(Integer, ForeignKey('insiders.id'))
    company_cik = Column(String(10), ForeignKey('companies.cik'))
    relationship_type = Column(String(100))  # officer, director, 10% owner - increased from 50 to 100
    title = Column(String(255))
    is_active = Column(Boolean, default=True)
    start_date = Column(Date)
    end_date = Column(Date)
    
    # Relationships
    insider = relationship("Insider", back_populates="insider_relationships")
    company = relationship("Company", back_populates="insider_relationships")


class Form4Filing(Base):
    """Form 4 filing information"""
    __tablename__ = 'form4_filings'
    
    id = Column(Integer, primary_key=True)
    accession_number = Column(String(20), unique=True, index=True)
    filing_date = Column(DateTime, index=True)
    accepted_date = Column(DateTime)
    insider_id = Column(Integer, ForeignKey('insiders.id'))
    company_cik = Column(String(10), ForeignKey('companies.cik'))
    reporting_owner_cik = Column(String(10))
    reporting_owner_name = Column(String(255))
    reporting_owner_relationship = Column(String(255))
    xml_content = Column(Text)  # Store raw XML for reprocessing
    processed = Column(Boolean, default=False)
    processing_errors = Column(Text)
    
    # Relationships
    insider = relationship("Insider", back_populates="form4_filings")
    company = relationship("Company", back_populates="form4_filings")
    transactions = relationship("Transaction", back_populates="filing")


class Transaction(Base):
    """Individual transaction from Form 4 filings"""
    __tablename__ = 'transactions'
    
    id = Column(Integer, primary_key=True)
    filing_id = Column(Integer, ForeignKey('form4_filings.id'))
    transaction_date = Column(Date, index=True)
    transaction_code = Column(String(1))  # P=Purchase, S=Sale, etc.
    shares = Column(Numeric(15, 4))
    price_per_share = Column(Numeric(12, 4))
    total_value = Column(Numeric(18, 2))
    shares_owned_after = Column(Numeric(15, 4))
    is_direct = Column(Boolean)
    transaction_type = Column(String(20))  # common, derivative
    security_title = Column(String(255))
    notes = Column(Text)
    
    # Relationships
    filing = relationship("Form4Filing", back_populates="transactions")


class StockPrice(Base):
    """Historical stock price data"""
    __tablename__ = 'stock_prices'
    
    id = Column(Integer, primary_key=True)
    ticker = Column(String(10), index=True)
    date = Column(Date, index=True)
    open = Column(Numeric(12, 4))
    high = Column(Numeric(12, 4))
    low = Column(Numeric(12, 4))
    close = Column(Numeric(12, 4))
    adjusted_close = Column(Numeric(12, 4))
    volume = Column(BigInteger)
    
    __table_args__ = (
        {'schema': None},
    )


class InsiderPosition(Base):
    """Insider position tracking over time"""
    __tablename__ = 'insider_positions'
    
    id = Column(Integer, primary_key=True)
    insider_id = Column(Integer, ForeignKey('insiders.id'))
    company_cik = Column(String(10), ForeignKey('companies.cik'))
    position_date = Column(Date, index=True)
    shares = Column(Numeric(15, 4))
    avg_cost_basis = Column(Numeric(12, 4))
    market_value = Column(Numeric(18, 2))
    unrealized_pnl = Column(Numeric(18, 2))
    
    # Relationships
    insider = relationship("Insider", back_populates="insider_positions")
    company = relationship("Company", back_populates="insider_positions")


class InsiderPerformance(Base):
    """Performance metrics for each insider"""
    __tablename__ = 'insider_performance'
    
    id = Column(Integer, primary_key=True)
    insider_id = Column(Integer, ForeignKey('insiders.id'))
    calculation_date = Column(Date, index=True)
    total_realized_pnl = Column(Numeric(18, 2))
    total_unrealized_pnl = Column(Numeric(18, 2))
    win_rate = Column(Numeric(5, 2))
    avg_holding_period_days = Column(Integer)
    total_transactions = Column(Integer)
    best_trade_pnl = Column(Numeric(18, 2))
    worst_trade_pnl = Column(Numeric(18, 2))
    sharpe_ratio = Column(Numeric(8, 4))
    
    # Relationships
    insider = relationship("Insider", back_populates="performance_records")


# Additional indexes for performance optimization
from sqlalchemy import Index

Index('idx_transactions_insider_date', Transaction.filing_id, Transaction.transaction_date)
Index('idx_form4_company_date', Form4Filing.company_cik, Form4Filing.filing_date)
Index('idx_stock_prices_ticker_date', StockPrice.ticker, StockPrice.date)
Index('idx_insider_positions_unique', InsiderPosition.insider_id, InsiderPosition.company_cik, InsiderPosition.position_date, unique=True)
Index('idx_insider_performance_unique', InsiderPerformance.insider_id, InsiderPerformance.calculation_date, unique=True) 