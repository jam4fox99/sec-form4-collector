# VM Deployment Guide - SEC Form 4 Scraping System

This guide shows you how to set up your VM to continuously scrape SEC Form 4 data while accessing the database remotely from your local machine.

## 🏗️ Architecture Overview

```
┌─────────────────────┐       ┌─────────────────────┐
│     Local Machine   │       │      Cloud VM       │
│                     │       │                     │
│  ┌─────────────────┐│       │ ┌─────────────────┐ │
│  │   Analysis &    ││  SSH  │ │ Comprehensive   │ │
│  │   Monitoring    ││◄─────►│ │    Scraper      │ │
│  │   Scripts       ││       │ │   (6 threads)   │ │
│  └─────────────────┘│       │ └─────────────────┘ │
│           │          │       │          │          │
│     Remote DB        │       │     Local DB        │
│      Access          │       │      Access         │
│           │          │       │          │          │
└─────────────────────┘       └─────────────────────┘
                               │
                               ▼
                     ┌─────────────────────┐
                     │   PostgreSQL DB     │
                     │                     │
                     │  • 1995-2024 data   │
                     │  • ~60-80 GB        │
                     │  • Remote access    │
                     └─────────────────────┘
```

## 📊 Expected Data Size (1995-2024)

- **Raw XML Files**: ~60-80 GB
- **Database Storage**: ~25-35 GB  
- **Total Form 4 Filings**: ~1.2-1.5 million
- **Processing Time**: ~3-5 days with 6 threads

## 🚀 Step-by-Step Setup

### 1. SSH into Your VM

```bash
# SSH into your VM
ssh username@your-vm-ip

# Or if using Google Cloud
gcloud compute ssh your-vm-name --zone=your-zone
```

### 2. Set Up Remote Database Access

```bash
# Copy and run the database setup script
./setup_remote_database.sh

# This script will:
# - Configure PostgreSQL for remote access
# - Set up IP whitelisting
# - Open firewall port 5432
# - Create database user with proper permissions
```

**Important**: When prompted, provide your local machine's public IP address. You can find it by running:
```bash
# On your local machine
curl -s https://api.ipify.org
```

### 3. Upload Project Files to VM

```bash
# From your local machine, copy the project
scp -r "SEC Form 4" username@your-vm-ip:/home/username/

# Or using rsync (recommended)
rsync -av --exclude='.git' --exclude='data/' "SEC Form 4/" username@your-vm-ip:/home/username/sec-form4/
```

### 4. Install Dependencies on VM

```bash
# On VM - install Python dependencies
cd /home/username/sec-form4
pip install -r requirements.txt

# Install PostgreSQL if not already installed
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib
```

### 5. Configure Database on VM

```bash
# On VM - initialize database
python main.py setup

# Verify database is working
python main.py stats
```

### 6. Start Comprehensive Scraping

```bash
# On VM - start the comprehensive scraper
python src/data_collection/comprehensive_scraper.py --start 1995 --end 2024 --threads 6

# Or run in background with nohup
nohup python src/data_collection/comprehensive_scraper.py --start 1995 --end 2024 --threads 6 > scraper.log 2>&1 &

# Check status
python src/data_collection/comprehensive_scraper.py --status
```

## 📱 Remote Monitoring from Local Machine

### 1. Configure Local Database Access

```bash
# On your local machine - update config file
cp config/config_remote.yaml config/config_remote.yaml.backup

# Edit config/config_remote.yaml and replace:
# host: "YOUR_VM_PUBLIC_IP"
# With your actual VM's public IP
```

### 2. Test Remote Connection

```bash
# Test remote database connection
python -c "
from src.database.db_manager import get_db_manager
db = get_db_manager('config/config_remote.yaml')
print('✅ Connection successful!' if db.health_check() else '❌ Connection failed')
"
```

### 3. Monitor Progress

```bash
# Show current status
python monitor_vm_scraper.py

# Watch mode (updates every 60 seconds)
python monitor_vm_scraper.py --watch

# Show performance estimates
python monitor_vm_scraper.py --performance
```

### 4. Run Analysis Remotely

```bash
# Run analysis on remote data
python main.py stats --config config/config_remote.yaml

# Show downloaded years
python main.py years --config config/config_remote.yaml
```

## 🔧 Advanced Configuration

### Continuous Mode (Recommended)

```bash
# On VM - run in continuous mode (keeps checking for new data)
python src/data_collection/comprehensive_scraper.py --continuous --start 1995

# This will:
# - Download all historical data
# - Keep checking for new filings
# - Automatically handle new years
# - Restart on failures
```

### Performance Tuning

```bash
# Adjust thread count based on your VM specs
python src/data_collection/comprehensive_scraper.py --threads 4  # Conservative
python src/data_collection/comprehensive_scraper.py --threads 6  # Recommended
python src/data_collection/comprehensive_scraper.py --threads 8  # Maximum (may hit rate limits)
```

### Error Handling

```bash
# Check scraper logs
tail -f comprehensive_scraper.log

# Restart failed years
python src/data_collection/comprehensive_scraper.py --start 2020 --end 2024 --force
```

## 🔒 Security Considerations

### Database Security

1. **IP Whitelisting**: Only your IP can access the database
2. **Strong Password**: Use a strong password for the database user
3. **SSL Connection**: Consider enabling SSL for database connections
4. **Firewall Rules**: Only port 5432 is opened for your IP

### SSH Security

1. **Key-based Authentication**: Use SSH keys instead of passwords
2. **Disable Root Login**: Disable direct root SSH access
3. **Fail2Ban**: Install fail2ban to prevent brute force attacks

### Alternative: SSH Tunnel (More Secure)

```bash
# Create SSH tunnel for database access
ssh -L 5432:localhost:5432 username@your-vm-ip

# Then use localhost in your config
# host: "localhost"
# port: 5432
```

## 📊 Monitoring & Maintenance

### Daily Monitoring

```bash
# Check scraper status
python monitor_vm_scraper.py

# Check VM resources
ssh username@your-vm-ip "htop"

# Check database size
ssh username@your-vm-ip "du -sh /var/lib/postgresql/*/main/"
```

### Weekly Maintenance

```bash
# On VM - vacuum database for performance
python -c "
from src.database.db_manager import get_db_manager
db = get_db_manager()
db.vacuum_analyze()
"

# Check logs for errors
ssh username@your-vm-ip "grep ERROR /home/username/sec-form4/comprehensive_scraper.log"
```

## 🚨 Troubleshooting

### Common Issues

1. **Connection Refused**
   ```bash
   # Check if PostgreSQL is running
   sudo systemctl status postgresql
   
   # Check firewall
   sudo ufw status
   ```

2. **Rate Limiting**
   ```bash
   # Reduce thread count
   python src/data_collection/comprehensive_scraper.py --threads 4
   ```

3. **Disk Space**
   ```bash
   # Check disk usage
   df -h
   
   # Clean up old logs
   find . -name "*.log" -mtime +7 -delete
   ```

4. **Memory Issues**
   ```bash
   # Check memory usage
   free -h
   
   # Restart scraper if needed
   pkill -f comprehensive_scraper.py
   ```

### Recovery Commands

```bash
# Restart scraper after failure
python src/data_collection/comprehensive_scraper.py --start 1995 --end 2024 --threads 6

# Resume from specific year
python src/data_collection/comprehensive_scraper.py --start 2020 --end 2024 --threads 6

# Force re-download of failed years
python src/data_collection/comprehensive_scraper.py --start 2020 --end 2024 --force
```

## 📈 Performance Expectations

### Expected Timeline (6 threads)

- **1995-2003**: ~8-12 hours (smaller volume)
- **2004-2015**: ~2-3 days (medium volume) 
- **2016-2024**: ~1-2 days (high volume)
- **Total**: ~3-5 days for complete historical data

### Resource Usage

- **CPU**: ~60-80% (6 threads)
- **Memory**: ~2-4 GB
- **Network**: ~1-2 Mbps sustained
- **Disk I/O**: Moderate (batched writes)

## 🎯 Success Metrics

You'll know it's working when:

1. ✅ Remote database connection works
2. ✅ VM scraper shows progress in logs
3. ✅ Monitor shows increasing filing counts
4. ✅ Years are marked as 'completed' in database
5. ✅ You can run analysis from local machine

## 🔄 Automation Setup

### Daily Cron Job (Optional)

```bash
# On VM - add to crontab
crontab -e

# Add this line to run daily at 6 AM
0 6 * * * cd /home/username/sec-form4 && python src/data_collection/comprehensive_scraper.py --start 1995 --continuous >> daily_scraper.log 2>&1
```

### Systemd Service (Recommended)

```bash
# Create systemd service file
sudo tee /etc/systemd/system/sec-scraper.service << EOF
[Unit]
Description=SEC Form 4 Scraper
After=network.target

[Service]
Type=simple
User=username
WorkingDirectory=/home/username/sec-form4
ExecStart=/usr/bin/python3 src/data_collection/comprehensive_scraper.py --continuous --start 1995
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
sudo systemctl enable sec-scraper
sudo systemctl start sec-scraper

# Check status
sudo systemctl status sec-scraper
```

---

## 📞 Getting Help

If you encounter issues:

1. Check the logs: `tail -f comprehensive_scraper.log`
2. Test database connection: `python monitor_vm_scraper.py`
3. Verify VM resources: `htop` and `df -h`
4. Check network connectivity: `ping sec.gov`
5. Review SEC rate limiting: Look for 429 errors in logs

Good luck with your SEC Form 4 data collection! 🚀 