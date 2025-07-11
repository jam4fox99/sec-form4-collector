#!/bin/bash
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
