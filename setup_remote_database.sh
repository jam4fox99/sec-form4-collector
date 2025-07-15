#!/bin/bash

# PostgreSQL Remote Access Setup Script
# Run this on your VM to enable remote database access

echo "🐘 Setting up PostgreSQL for remote access..."
echo "================================================"

# Get VM's public IP
PUBLIC_IP=$(curl -s https://api.ipify.org)
echo "📍 VM Public IP: $PUBLIC_IP"

# Get your local IP (you'll need to provide this)
echo "❓ Please provide your local machine's public IP address:"
read LOCAL_IP

# Database configuration
DB_NAME="insider_trading"
DB_USER="insider_user"
DB_PASSWORD="insider_password"

echo "🔧 Configuring PostgreSQL..."

# 1. Update PostgreSQL configuration to allow remote connections
echo "1. Updating postgresql.conf..."
sudo sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" /etc/postgresql/*/main/postgresql.conf

# 2. Configure authentication for remote connections
echo "2. Updating pg_hba.conf..."
sudo tee -a /etc/postgresql/*/main/pg_hba.conf << EOF

# Remote access configuration
host    $DB_NAME    $DB_USER    $LOCAL_IP/32    md5
host    $DB_NAME    $DB_USER    $PUBLIC_IP/32   md5
EOF

# 3. Create/update database user with remote access
echo "3. Setting up database user..."
sudo -u postgres psql << EOF
-- Create database if it doesn't exist
CREATE DATABASE $DB_NAME;

-- Create user if it doesn't exist
CREATE USER $DB_USER WITH ENCRYPTED PASSWORD '$DB_PASSWORD';

-- Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $DB_USER;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $DB_USER;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO $DB_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO $DB_USER;

\q
EOF

# 4. Open firewall port 5432
echo "4. Opening firewall port 5432..."
sudo ufw allow 5432/tcp

# 5. Restart PostgreSQL
echo "5. Restarting PostgreSQL..."
sudo systemctl restart postgresql

# 6. Test connection
echo "6. Testing PostgreSQL service..."
sudo systemctl status postgresql --no-pager

echo "✅ PostgreSQL remote access setup completed!"
echo ""
echo "🔗 Connection details:"
echo "   Host: $PUBLIC_IP"
echo "   Port: 5432"
echo "   Database: $DB_NAME"
echo "   User: $DB_USER"
echo "   Password: $DB_PASSWORD"
echo ""
echo "🔒 Security Notes:"
echo "   - Only your IP ($LOCAL_IP) can connect remotely"
echo "   - SSL connections are recommended"
echo "   - Consider using SSH tunneling for extra security"
echo ""
echo "📋 Next steps:"
echo "   1. Test connection from your local machine"
echo "   2. Update your local config file with VM connection details"
echo "   3. Deploy the scraper to this VM" 