# Google Cloud Setup Guide for SEC Form 4 Data Collection

## Step 1: Create Google Cloud VM

1. **Go to Google Cloud Console**: https://console.cloud.google.com/
2. **Create a new project** (or use existing)
3. **Enable Compute Engine API**
4. **Create VM Instance**:
   - Name: `sec-form4-collector`
   - Region: `us-central1` (or closest to you)
   - Machine type: `e2-standard-2` (2 vCPUs, 8 GB memory)
   - Boot disk: `Ubuntu 22.04 LTS`, 50 GB
   - **Allow HTTP/HTTPS traffic** (check both boxes)
   - **Advanced options > Management > Startup script**:
     ```bash
     #!/bin/bash
     apt-get update
     apt-get install -y python3 python3-pip postgresql postgresql-contrib git
     systemctl start postgresql
     systemctl enable postgresql
     ```

## Step 2: Initial VM Setup

SSH into your VM (click "SSH" button in console):

```bash
# Update system
sudo apt-get update && sudo apt-get upgrade -y

# Install Python dependencies
sudo apt-get install -y python3-pip python3-venv git postgresql postgresql-contrib

# Create project directory
mkdir -p /home/$USER/sec-form4
cd /home/$USER/sec-form4

# Clone your project (we'll set this up)
git clone <your-repo-url> .
```

## Step 3: PostgreSQL Setup

```bash
# Switch to postgres user
sudo -u postgres psql

# Create database and user
CREATE DATABASE insider_trading;
CREATE USER sec_user WITH PASSWORD 'secure_password_here';
GRANT ALL PRIVILEGES ON DATABASE insider_trading TO sec_user;
\q

# Configure PostgreSQL for remote connections (if needed)
sudo nano /etc/postgresql/14/main/postgresql.conf
# Uncomment and set: listen_addresses = '*'

sudo nano /etc/postgresql/14/main/pg_hba.conf
# Add line: host    all             all             0.0.0.0/0               md5

sudo systemctl restart postgresql
```

## Step 4: Python Environment Setup

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Update config for cloud database
cp config/config.yaml config/config.yaml.bak
nano config/config.yaml
```

## Step 5: Database Migration

```bash
# Run initial database setup
python -c "
from src.database.db_manager import DatabaseManager
db = DatabaseManager()
db.create_tables()
print('Database tables created successfully')
"
```

## Step 6: Test and Run

```bash
# Test with small dataset first
python main.py --year 2023 --limit 100

# If successful, run full collection
screen -S sec-collection
python main.py --year 2023
# Ctrl+A, D to detach from screen
```

## Data Access Options

### Option 1: Direct Database Connection (Recommended)
- Connect to PostgreSQL from your local machine
- Update your local config.yaml with cloud database credentials
- Use your existing Cursor setup for analysis

### Option 2: SSH + Local Development
- SSH into VM for data collection monitoring
- Export data periodically for local analysis
- Use rsync or scp to transfer processed data

### Option 3: Hybrid Approach
- Cloud VM for data collection only
- Regular database dumps to local machine
- Keep analysis environment local

## Live Data Collection Setup

### Daily Automation
```bash
# Create daily collection script
cat > daily_collection.sh << 'EOF'
#!/bin/bash
cd /home/$USER/sec-form4
source venv/bin/activate
python main.py --date $(date -d "yesterday" +%Y-%m-%d)
EOF

chmod +x daily_collection.sh

# Add to crontab (runs daily at 6 AM)
crontab -e
# Add: 0 6 * * * /home/$USER/sec-form4/daily_collection.sh
```

## Security Considerations

1. **Firewall**: Only open necessary ports
2. **Database**: Use strong passwords, consider IP restrictions
3. **SSH Keys**: Set up key-based authentication
4. **Monitoring**: Set up alerts for system health

## Cost Estimation

- e2-standard-2 VM: ~$25-30/month
- Storage (50GB): ~$2/month
- Network: Minimal for SEC API calls
- **Total: ~$30-35/month** (covered by $300 free credits)

## Next Steps

1. Create the VM
2. Set up the environment
3. Test with small dataset
4. Run full historical collection
5. Set up daily automation 