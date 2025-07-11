# Historical Insider Trading Data Collection & Analysis System

A comprehensive Python-based system to download, parse, and analyze historical Form 4 insider trading filings from the SEC EDGAR database. This system identifies the most successful insider traders based on their profit and loss (P&L) performance.

## 🚀 Features

- **SEC EDGAR Integration**: Downloads Form 4 filings from SEC EDGAR with proper rate limiting (respects 10 requests/second limit)
- **Multi-threaded Processing**: Efficient parallel processing pipeline with separate threads for downloading, parsing, and database storage
- **Form 4 XML Parsing**: Comprehensive parser handling both modern XML and legacy text formats
- **Database Storage**: PostgreSQL database with optimized schema for fast queries
- **Rate Limiting**: Adaptive rate limiting to prevent SEC API violations
- **Error Handling**: Robust error handling with retry logic and graceful degradation
- **Progress Tracking**: Real-time progress bars and detailed statistics
- **Incremental Updates**: Support for both full historical collection and incremental daily updates

## 📊 Data Coverage

- **Historical Range**: 2003-present (when SEC began electronic Form 4 filings)
- **Expected Volume**: ~4-5 million Form 4 filings
- **Data Size**: ~50-100 GB of XML data, ~20-30 GB database storage
- **Processing Time**: 3-5 days for full historical collection (respecting rate limits)

## 🏗️ System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SEC EDGAR     │    │   Rate Limiter  │    │   Multi-threaded│
│   Daily Index   │───▶│   (8 req/sec)   │───▶│   Download      │
│   Files         │    │                 │    │   Workers       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │   Batch Storage │    │   XML Parser    │
│   Database      │◀───│   Worker        │◀───│   Workers       │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📋 Requirements

### System Requirements
- Python 3.8+
- PostgreSQL 12+
- 16+ GB RAM (recommended)
- 100+ GB free disk space

### Python Dependencies
```bash
pip install -r requirements.txt
```

## 🔧 Setup Instructions

### 1. Database Setup

First, set up PostgreSQL:

```bash
# Install PostgreSQL (Ubuntu/Debian)
sudo apt-get install postgresql postgresql-contrib

# Create database and user
sudo -u postgres psql
CREATE DATABASE insider_trading;
CREATE USER insider_user WITH ENCRYPTED PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE insider_trading TO insider_user;
\q
```

### 2. Configuration

Update the database configuration in `config/config.yaml`:

```yaml
database:
  host: "localhost"
  port: 5432
  name: "insider_trading"
  user: "insider_user"
  password: "your_password"
```

**Important**: Update the `user_agent` in the config file with your contact information as required by SEC:

```yaml
edgar:
  user_agent: "Your Company Name admin@yourcompany.com"
```

### 3. Initialize Database

```bash
python main.py setup
```

## 🚀 Usage

### Quick Start (Sample Data)

Test the system with a small sample of recent filings:

```bash
# Test the downloader
python main.py test

# Process sample data (last 3 days, max 100 filings)
python main.py sample
```

### Full Historical Collection

**Warning**: This will take 3-5 days to complete and download ~100GB of data.

```bash
# Process all historical data from 2003 to 2024
python main.py historical 2003 2024

# Process specific year range
python main.py historical 2020 2024
```

### Database Statistics

```bash
# View current database statistics
python main.py stats
```

## 📊 Database Schema

The system uses a normalized PostgreSQL schema optimized for insider trading analysis:

### Core Tables

- **`companies`**: Company information (CIK, ticker, name)
- **`insiders`**: Individual insider information
- **`insider_relationships`**: Relationship between insiders and companies
- **`form4_filings`**: Form 4 filing metadata
- **`transactions`**: Individual transaction records
- **`stock_prices`**: Historical stock price data
- **`insider_positions`**: Position tracking over time
- **`insider_performance`**: Performance metrics and rankings

### Key Indexes

- Transaction date and company lookups
- Insider and filing relationships
- Stock price date ranges
- Performance ranking queries

## 🔍 Data Processing Pipeline

### 1. Download Phase
- Fetches daily index files from SEC EDGAR
- Filters for Form 4 filings
- Downloads individual XML files with rate limiting
- Handles network errors with exponential backoff

### 2. Parsing Phase
- Parses Form 4 XML using custom parser
- Extracts issuer, reporting owner, and transaction data
- Handles footnotes and data validation
- Supports both modern XML and legacy formats

### 3. Storage Phase
- Batch inserts for performance
- Creates normalized database records
- Maintains referential integrity
- Handles duplicate detection

## 🎯 Key Features

### Rate Limiting
- Adaptive rate limiter respects SEC's 10 requests/second limit
- Automatically reduces rate if 429 errors occur
- Gradual recovery to normal rates

### Error Handling
- Comprehensive retry logic with exponential backoff
- Graceful handling of malformed XML
- Network timeout and connection error recovery
- Detailed error logging and statistics

### Multi-threading
- Separate thread pools for download, parsing, and storage
- Configurable thread counts for optimal performance
- Queue-based work distribution
- Progress tracking across all threads

### Data Quality
- XML validation and sanitization
- Decimal precision handling for financial data
- Date format normalization
- Footnote processing and linking

## 📈 Performance Optimization

### Database Optimization
```bash
# Run VACUUM ANALYZE for better performance
python -c "
from src.database.db_manager import get_db_manager
db_manager = get_db_manager()
db_manager.vacuum_analyze()
"
```

### Monitoring
- Real-time progress bars
- Thread-specific error tracking
- Queue size monitoring
- Processing rate statistics

## 🔒 SEC Compliance

This system is designed to comply with SEC EDGAR access guidelines:

- **Rate Limiting**: Maximum 8 requests/second (below the 10/second limit)
- **User Agent**: Properly formatted user agent string required
- **Respectful Access**: Automatic backoff on rate limit errors
- **Terms of Service**: Users must comply with SEC terms of service

## 📝 Logging

The system provides comprehensive logging:

- **File Logging**: `insider_trading.log`
- **Console Output**: Real-time progress and status
- **Error Tracking**: Detailed error logs with stack traces
- **Statistics**: Processing metrics and performance data

## 🐛 Troubleshooting

### Common Issues

1. **Database Connection Error**
   - Check PostgreSQL is running
   - Verify credentials in config.yaml
   - Ensure database exists

2. **Rate Limit Errors**
   - System automatically handles these
   - Check user agent is properly set
   - Verify internet connection

3. **XML Parsing Errors**
   - Some legacy filings may have formatting issues
   - Errors are logged but don't stop processing
   - Check logs for specific filing issues

4. **Memory Issues**
   - Reduce batch_size in configuration
   - Monitor system resources
   - Consider processing smaller date ranges

### Performance Tuning

```bash
# Adjust thread counts based on system capabilities
processor = BulkProcessor(
    num_download_threads=2,    # Limited by rate limiting
    num_parse_threads=4,       # Scale with CPU cores
    batch_size=1000           # Adjust based on memory
)
```

## 📚 Next Steps

After collecting the historical data, you can:

1. **Implement P&L Calculation**: Add the position tracking and P&L calculation engine
2. **Stock Price Integration**: Integrate with yfinance for historical stock prices
3. **Performance Analytics**: Build ranking and performance analysis tools
4. **Web Interface**: Create a web dashboard for data visualization
5. **API Development**: Build REST API for data access

## 🔮 Future Enhancements

Phase 2 components to be implemented:

- **Stock Price Collection**: Historical price data integration
- **Position Tracking**: FIFO-based position management
- **P&L Calculation**: Realized and unrealized profit/loss calculations
- **Performance Metrics**: Sharpe ratio, win rate, holding periods
- **Ranking System**: Insider performance leaderboards
- **Visualization**: Charts and dashboards for analysis

## 📄 License

This project is for educational and research purposes. Users must comply with SEC terms of service and applicable regulations regarding the use of SEC data.

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## 📞 Support

For issues and questions:

1. Check the troubleshooting section
2. Review the logs for error details
3. Open an issue with detailed information
4. Include system specs and error messages

---

**Disclaimer**: This system is for educational and research purposes only. Users are responsible for complying with SEC regulations and terms of service. The authors are not responsible for any misuse of the system or data. 