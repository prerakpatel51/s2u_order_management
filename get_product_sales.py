#!/usr/bin/env python3
"""
Script to get last month's sales for a specific product at a specific store
from Korona Cloud API.

Usage:
    python get_product_sales.py --product-number 1 --store-number 6300
    python get_product_sales.py --product-number 1 --store-number 6300 --days 60
"""

import requests
from requests.auth import HTTPBasicAuth
import os
import sys
import argparse
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Korona API credentials
BASE_URL = 'https://185.koronacloud.com/web/api/v3'
ACCOUNT_ID = os.getenv('KORONA_ACCOUNT_ID')
USERNAME = os.getenv('KORONA_USER')
PASSWORD = os.getenv('KORONA_PASSWORD')


def get_product_by_number(product_number):
    """Get product details by product number."""
    url = f'{BASE_URL}/accounts/{ACCOUNT_ID}/products'
    params = {
        'number': product_number,
        'size': 1
    }

    response = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD), params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    results = data.get('results', [])

    if not results:
        return None

    return results[0]


def get_store_by_number(store_number):
    """Get store details by store number."""
    url = f'{BASE_URL}/accounts/{ACCOUNT_ID}/organizationalUnits'
    params = {
        'number': store_number,
        'size': 1
    }

    response = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD), params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    results = data.get('results', [])

    if not results:
        return None

    return results[0]


def calculate_product_sales(product_id, store_id=None, days=30):
    """
    Calculate total sales for a product in the last N days.

    Args:
        product_id: Korona product UUID
        store_id: Korona store UUID (optional, if None will aggregate all stores)
        days: Number of days to look back (default 30)

    Returns:
        Dictionary with sales data
    """
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    from_time = start_date.strftime('%Y-%m-%dT00:00:00-07:00')
    to_time = end_date.strftime('%Y-%m-%dT23:59:59-07:00')

    url = f'{BASE_URL}/accounts/{ACCOUNT_ID}/receipts'
    page = 1
    total_qty = 0
    receipt_count = 0
    receipts_detail = []

    print(f'\nFetching receipts from {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}...')

    start_time = time.time()

    while True:
        params = {
            'minBookingTime': from_time,
            'maxBookingTime': to_time,
            'page': page,
            'size': 100
        }

        response = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD), params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        results = data.get('results', [])

        if not results:
            break

        # Process receipts
        for receipt in results:
            # Skip voided or cancelled receipts
            if receipt.get('voided') or receipt.get('cancelled'):
                continue

            # Check store if specified
            if store_id:
                org_unit = receipt.get('organizationalUnit', {})
                if org_unit.get('id') != store_id:
                    continue

            # Check items for our product
            for item in receipt.get('items', []):
                product = item.get('product', {})
                if product.get('id') == product_id:
                    qty = item.get('quantity', 0)
                    total_qty += qty
                    receipt_count += 1

                    receipts_detail.append({
                        'receipt_number': receipt.get('number'),
                        'date': receipt.get('bookingTime'),
                        'quantity': qty,
                        'store': receipt.get('organizationalUnit', {}).get('name'),
                        'store_number': receipt.get('organizationalUnit', {}).get('number')
                    })

        # Check if more pages
        if page >= data.get('pagesTotal', 1):
            break
        page += 1

    elapsed_time = time.time() - start_time

    return {
        'total_quantity': total_qty,
        'receipt_count': receipt_count,
        'receipts': receipts_detail,
        'days': days,
        'elapsed_time': elapsed_time
    }


def main():
    parser = argparse.ArgumentParser(description='Get monthly sales for a product at a store')
    parser.add_argument('--product-number', required=True, help='Product number (e.g., 1, 2016, etc.)')
    parser.add_argument('--store-number', required=False, help='Store number (e.g., 6300, 1317, etc.)')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back (default: 30)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed receipt list')

    args = parser.parse_args()

    print('='*80)
    print('KORONA PRODUCT SALES CALCULATOR')
    print('='*80)

    # Get product details
    print(f'\nLooking up product #{args.product_number}...')
    product = get_product_by_number(args.product_number)

    if not product:
        print(f'❌ Product #{args.product_number} not found!')
        sys.exit(1)

    print(f'✅ Found: {product["name"]} (ID: {product["id"]})')

    # Get store details if specified
    store = None
    store_id = None
    if args.store_number:
        print(f'\nLooking up store #{args.store_number}...')
        store = get_store_by_number(args.store_number)

        if not store:
            print(f'❌ Store #{args.store_number} not found!')
            sys.exit(1)

        store_id = store['id']
        print(f'✅ Found: {store["name"]} (ID: {store["id"]})')

    # Calculate sales
    print(f'\nCalculating sales for last {args.days} days...')
    sales_data = calculate_product_sales(product['id'], store_id, args.days)

    # Display results
    print('\n' + '='*80)
    print('RESULTS')
    print('='*80)
    print(f'Product: {product["name"]} (#{args.product_number})')
    if store:
        print(f'Store: {store["name"]} (#{args.store_number})')
    else:
        print(f'Store: All stores')
    print(f'Period: Last {args.days} days')
    print(f'\n✅ Total Quantity Sold: {sales_data["total_quantity"]}')
    print(f'✅ Number of Receipts: {sales_data["receipt_count"]}')
    print(f'⏱️  Time taken: {sales_data["elapsed_time"]:.2f} seconds')

    if sales_data['total_quantity'] > 0:
        avg_per_day = sales_data['total_quantity'] / args.days
        monthly_projection = avg_per_day * 30
        print(f'\n📊 Average per day: {avg_per_day:.2f}')
        print(f'📊 Monthly projection (30 days): {monthly_projection:.0f}')

    if args.verbose and sales_data['receipts']:
        print('\n' + '-'*80)
        print('DETAILED RECEIPT LIST')
        print('-'*80)
        for receipt in sales_data['receipts']:
            print(f"Receipt #{receipt['receipt_number']} - {receipt['date']} - Qty: {receipt['quantity']} - Store: {receipt['store']} (#{receipt['store_number']})")

    print('='*80)


if __name__ == '__main__':
    main()
