#!/bin/bash
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
