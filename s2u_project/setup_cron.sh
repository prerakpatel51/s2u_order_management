#!/bin/bash

# Setup script for automatic syncing via cron jobs
# This script configures daily syncing of products, stores, stocks, and monthly sales data

# Get the absolute path to this script's directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
VENV_PYTHON="$SCRIPT_DIR/../s2u/bin/python"
MANAGE_PY="$SCRIPT_DIR/manage.py"
LOG_DIR="$SCRIPT_DIR/logs"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

echo "========================================="
echo "S2U Order Management - Cron Job Setup"
echo "========================================="
echo ""
echo "This script will set up the following daily cron jobs:"
echo "  • 04:00  Sync stores"
echo "  • 04:15  Sync products"
echo "  • 04:30  Sync stocks (all products)"
echo "  • 05:00  Precompute monthly sales (all products)"
echo ""
echo "Project directory: $SCRIPT_DIR"
echo "Python executable: $VENV_PYTHON"
echo "Logs directory: $LOG_DIR"
echo ""

# Verify paths exist
if [ ! -f "$VENV_PYTHON" ]; then
    echo "ERROR: Virtual environment Python not found at $VENV_PYTHON"
    echo "Please ensure the virtual environment is set up correctly."
    exit 1
fi

if [ ! -f "$MANAGE_PY" ]; then
    echo "ERROR: manage.py not found at $MANAGE_PY"
    exit 1
fi

# Test that Django can be imported
$VENV_PYTHON -c "import django" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "ERROR: Django is not installed in the virtual environment"
    exit 1
fi

echo "All paths verified successfully!"
echo ""

# Create a wrapper script for running Django management commands
WRAPPER_SCRIPT="$SCRIPT_DIR/run_sync.sh"

cat > "$WRAPPER_SCRIPT" << 'EOF'
#!/bin/bash
# Wrapper script for Django management commands with logging

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PYTHON="$SCRIPT_DIR/../s2u/bin/python"
MANAGE_PY="$SCRIPT_DIR/manage.py"
LOG_DIR="$SCRIPT_DIR/logs"

# Get command from arguments
COMMAND="$1"
TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_FILE="$LOG_DIR/${COMMAND}_${TIMESTAMP}.log"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Run the command and log output
echo "=========================================" >> "$LOG_FILE"
echo "Running: $COMMAND" >> "$LOG_FILE"
echo "Started: $(date)" >> "$LOG_FILE"
echo "=========================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

cd "$SCRIPT_DIR"
$VENV_PYTHON $MANAGE_PY $COMMAND >> "$LOG_FILE" 2>&1

EXIT_CODE=$?

echo "" >> "$LOG_FILE"
echo "=========================================" >> "$LOG_FILE"
echo "Finished: $(date)" >> "$LOG_FILE"
echo "Exit code: $EXIT_CODE" >> "$LOG_FILE"
echo "=========================================" >> "$LOG_FILE"

# Keep only last 30 days of logs
find "$LOG_DIR" -name "*.log" -type f -mtime +30 -delete

exit $EXIT_CODE
EOF

chmod +x "$WRAPPER_SCRIPT"
echo "Created wrapper script: $WRAPPER_SCRIPT"
echo ""

# Generate crontab entries
CRON_FILE="/tmp/s2u_crontab_entries.txt"

cat > "$CRON_FILE" << EOF
# S2U Order Management - Automatic Sync Jobs
# Generated on $(date)

# Daily 04:00 AM: Sync stores
0 4 * * * $WRAPPER_SCRIPT sync_stores
# Daily 04:15 AM: Sync products
15 4 * * * $WRAPPER_SCRIPT load_products --skip-csv
# Daily 04:30 AM: Sync stocks for all products (may take time)
30 4 * * * $WRAPPER_SCRIPT sync_stocks
# Daily 05:00 AM: Precompute monthly sales cache for all products
0 5 * * * $WRAPPER_SCRIPT sync_all_monthly_sales --days 30

EOF

echo "Generated crontab entries:"
echo ""
cat "$CRON_FILE"
echo ""

# Ask user if they want to install the cron jobs
read -p "Do you want to install these cron jobs? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    # Backup existing crontab
    crontab -l > "/tmp/crontab_backup_$(date +%Y%m%d_%H%M%S).txt" 2>/dev/null

    # Add new entries to crontab
    (crontab -l 2>/dev/null | grep -v "S2U Order Management"; cat "$CRON_FILE") | crontab -

    echo ""
    echo "✓ Cron jobs installed successfully!"
    echo ""
    echo "Current crontab:"
    crontab -l | grep -A 10 "S2U Order Management"
    echo ""
    echo "Logs will be written to: $LOG_DIR"
    echo ""
    echo "To view logs:"
    echo "  ls -lh $LOG_DIR"
    echo ""
    echo "To manually run a sync:"
    echo "  $WRAPPER_SCRIPT sync_stores"
    echo "  $WRAPPER_SCRIPT load_products"
    echo ""
    echo "To remove these cron jobs:"
    echo "  crontab -e"
    echo "  (then delete the lines under 'S2U Order Management')"
    echo ""
else
    echo ""
    echo "Cron jobs NOT installed."
    echo ""
    echo "To install manually, run:"
    echo "  crontab -e"
    echo ""
    echo "Then add this line:"
    cat "$CRON_FILE"
    echo ""
fi

rm -f "$CRON_FILE"
EOF
