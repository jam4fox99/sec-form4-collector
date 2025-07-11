#!/usr/bin/env python3
"""
Deployment script for SEC Form 4 data collection system on Google Cloud.
This script prepares the project for cloud deployment.
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path

def create_cloud_config():
    """Create cloud-specific configuration."""
    config_content = """
# Cloud Database Configuration
database:
  host: 'localhost'  # Will be localhost on the VM
  port: 5432
  database: 'insider_trading'
  username: 'sec_user'
  password: 'your_secure_password_here'  # Change this!

# Rate limiting (conservative for cloud)
rate_limits:
  sec_requests_per_second: 8  # Slightly under 10/sec limit
  max_concurrent_downloads: 3
  max_concurrent_parsers: 4
  max_concurrent_storage: 2

# Logging configuration
logging:
  level: 'INFO'
  file: 'insider_trading.log'
  max_size_mb: 100
  backup_count: 5

# Processing configuration
processing:
  batch_size: 10
  max_retries: 3
  retry_delay: 5
  download_timeout: 30
"""
    
    with open('config/config_cloud.yaml', 'w') as f:
        f.write(config_content)
    print("✓ Created cloud configuration file")

def create_daily_collection_script():
    """Create script for daily data collection."""
    script_content = """#!/bin/bash
# Daily SEC Form 4 Collection Script
# This script collects new filings from the previous day

set -e  # Exit on error

# Configuration
PROJECT_DIR="/home/$USER/sec-form4"
LOG_DIR="$PROJECT_DIR/logs"
DATE=$(date -d "yesterday" +%Y-%m-%d)

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Log file for this run
LOG_FILE="$LOG_DIR/daily_collection_$(date +%Y%m%d).log"

echo "Starting daily collection for $DATE" | tee -a "$LOG_FILE"

# Change to project directory
cd "$PROJECT_DIR"

# Activate virtual environment
source venv/bin/activate

# Run collection for yesterday's filings
python main.py --date "$DATE" --config config/config_cloud.yaml 2>&1 | tee -a "$LOG_FILE"

# Check if collection was successful
if [ $? -eq 0 ]; then
    echo "✓ Daily collection completed successfully" | tee -a "$LOG_FILE"
else
    echo "✗ Daily collection failed" | tee -a "$LOG_FILE"
    exit 1
fi

# Optional: Send email notification or post to monitoring system
# echo "Daily collection completed for $DATE" | mail -s "SEC Form 4 Collection" your@email.com

echo "Collection completed at $(date)" | tee -a "$LOG_FILE"
"""
    
    with open('daily_collection.sh', 'w') as f:
        f.write(script_content)
    
    # Make executable
    os.chmod('daily_collection.sh', 0o755)
    print("✓ Created daily collection script")

def create_setup_script():
    """Create VM setup script."""
    script_content = """#!/bin/bash
# VM Setup Script for SEC Form 4 Data Collection
# Run this script after SSH'ing into your Google Cloud VM

set -e  # Exit on error

echo "Setting up SEC Form 4 data collection environment..."

# Update system
echo "📦 Updating system packages..."
sudo apt-get update && sudo apt-get upgrade -y

# Install required packages
echo "📦 Installing required packages..."
sudo apt-get install -y python3-pip python3-venv git postgresql postgresql-contrib screen htop

# Start PostgreSQL
echo "🐘 Starting PostgreSQL..."
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create project directory
PROJECT_DIR="/home/$USER/sec-form4"
mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR"

# Create virtual environment
echo "🐍 Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies (you'll need to transfer requirements.txt)
echo "📦 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Setup database
echo "🐘 Setting up PostgreSQL database..."
sudo -u postgres psql << EOF
CREATE DATABASE insider_trading;
CREATE USER sec_user WITH PASSWORD 'change_this_password';
GRANT ALL PRIVILEGES ON DATABASE insider_trading TO sec_user;
\q
EOF

# Create database tables
echo "📊 Creating database tables..."
python -c "
from src.database.db_manager import DatabaseManager
db = DatabaseManager()
db.create_tables()
print('Database tables created successfully')
"

# Test the setup
echo "🧪 Testing the setup..."
python main.py --year 2023 --limit 10 --config config/config_cloud.yaml

echo ""
echo "✅ Setup completed successfully!"
echo ""
echo "Next steps:"
echo "1. Test with a small dataset: python main.py --year 2023 --limit 100"
echo "2. Run full collection in screen: screen -S sec-collection"
echo "3. Set up daily automation: crontab -e"
echo "   Add: 0 6 * * * /home/$USER/sec-form4/daily_collection.sh"
echo ""
echo "To monitor progress: screen -r sec-collection"
echo "To detach from screen: Ctrl+A, D"
"""
    
    with open('setup_vm.sh', 'w') as f:
        f.write(script_content)
    
    os.chmod('setup_vm.sh', 0o755)
    print("✓ Created VM setup script")

def create_local_config_for_cloud_access():
    """Create local configuration to access cloud database."""
    config_content = """
# Local Configuration for Cloud Database Access
# Update the host IP with your Google Cloud VM's external IP

database:
  host: 'YOUR_VM_EXTERNAL_IP'  # Replace with your VM's external IP
  port: 5432
  database: 'insider_trading'
  username: 'sec_user'
  password: 'your_secure_password_here'  # Same as cloud config

# Local analysis configuration
analysis:
  cache_dir: 'data/cache'
  output_dir: 'data/analysis'
  
# Rate limiting (not needed for analysis)
rate_limits:
  sec_requests_per_second: 10
  max_concurrent_downloads: 4
  max_concurrent_parsers: 8
  max_concurrent_storage: 4

logging:
  level: 'INFO'
  file: 'local_analysis.log'
"""
    
    with open('config/config_local_cloud_access.yaml', 'w') as f:
        f.write(config_content)
    print("✓ Created local configuration for cloud database access")

def create_git_setup():
    """Create git repository setup instructions."""
    gitignore_content = """
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
env/
ENV/

# IDE
.vscode/
.idea/
*.swp
*.swo

# Logs
*.log
logs/

# Data
data/raw/
data/processed/
data/cache/

# Config (contains passwords)
config/config_cloud.yaml
config/config_local_cloud_access.yaml

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db
"""
    
    with open('.gitignore', 'w') as f:
        f.write(gitignore_content)
    print("✓ Created .gitignore file")

def main():
    """Main deployment preparation function."""
    print("🚀 Preparing SEC Form 4 project for cloud deployment...")
    print()
    
    # Create necessary directories
    os.makedirs('config', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    
    # Create configuration files
    create_cloud_config()
    create_local_config_for_cloud_access()
    
    # Create deployment scripts
    create_daily_collection_script()
    create_setup_script()
    
    # Create git setup
    create_git_setup()
    
    print()
    print("✅ Deployment preparation completed!")
    print()
    print("Next steps:")
    print("1. Initialize git repository: git init && git add . && git commit -m 'Initial commit'")
    print("2. Create GitHub repository and push code")
    print("3. Create Google Cloud VM using the guide")
    print("4. SSH into VM and run: git clone <your-repo-url> .")
    print("5. Run setup script: ./setup_vm.sh")
    print("6. Update config/config_cloud.yaml with your database password")
    print("7. Test with small dataset, then run full collection")
    print()
    print("For daily automation, add to crontab:")
    print("0 6 * * * /home/$USER/sec-form4/daily_collection.sh")

if __name__ == "__main__":
    main() 