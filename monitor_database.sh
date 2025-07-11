#!/bin/bash
# Database Monitoring Script
# Run this in a separate terminal to monitor collection progress

echo "📊 SEC Form 4 Database Monitor"
echo "=============================="
echo "Press Ctrl+C to stop monitoring"
echo ""

while true; do
    clear
    echo "📊 SEC Form 4 Database Monitor - $(date)"
    echo "=============================="
    echo ""
    
    # Get database statistics
    PGPASSWORD=SecurePass2024 psql -h localhost -U sec_user -d insider_trading -c "
    SELECT 
        'Total Filings' as metric,
        COUNT(*) as count
    FROM form4_filings
    UNION ALL
    SELECT 
        'Unique Companies' as metric,
        COUNT(DISTINCT cik) as count
    FROM form4_filings
    UNION ALL
    SELECT 
        'Total Transactions' as metric,
        COUNT(*) as count
    FROM transactions
    UNION ALL
    SELECT 
        'Latest Filing Date' as metric,
        MAX(document_date)::text as count
    FROM form4_filings
    UNION ALL
    SELECT 
        'Most Recent Insert' as metric,
        MAX(created_at)::text as count
    FROM form4_filings;
    " 2>/dev/null || echo "Database connection failed"
    
    echo ""
    echo "Recent activity (last 10 filings):"
    PGPASSWORD=SecurePass2024 psql -h localhost -U sec_user -d insider_trading -c "
    SELECT 
        document_date,
        company_name,
        reporting_owner_name,
        created_at
    FROM form4_filings 
    ORDER BY created_at DESC 
    LIMIT 10;
    " 2>/dev/null || echo "Database connection failed"
    
    echo ""
    echo "Refreshing in 10 seconds... (Ctrl+C to stop)"
    sleep 10
done 