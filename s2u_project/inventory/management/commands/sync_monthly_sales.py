"""
Django management command to fetch receipts from Korona Cloud API
and calculate monthly needed quantities based on actual sales data.

This uses the receipts endpoint for efficient batch processing of ALL products.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict
from decimal import Decimal
from collections import defaultdict

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from inventory.korona import get_session, build_url
from inventory.models import Product, Store, WeeklyOrderItem, WeeklyOrderList

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Fetch receipts from Korona API and calculate monthly needed quantities for ALL products"

    def add_arguments(self, parser):
        parser.add_argument(
            "--days",
            type=int,
            default=30,
            help="Number of days to look back for sales data (default: 30)",
        )
        parser.add_argument(
            "--store-number",
            type=str,
            help="Only process specific store by number (optional)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print calculations without updating database",
        )

    def handle(self, *args, **options):
        """Entry point for the command; orchestrates fetching and updating.

        Example (CLI):
            # Calculate for last 30 days and update DB
            python manage.py sync_monthly_sales

            # Dry-run, custom window, single store by number
            python manage.py sync_monthly_sales --days 14 --store-number 101 --dry-run
        """
        days_back = options["days"]
        store_number = options.get("store_number")
        dry_run = options["dry_run"]

        self.stdout.write(
            self.style.SUCCESS(f"\n{'='*80}\nFetching sales data for last {days_back} days...\n{'='*80}\n")
        )

        # Calculate dynamic date range (from current time backwards)
        end_date = timezone.now()
        start_date = end_date - timedelta(days=days_back)

        self.stdout.write(f"Date range: {start_date.strftime('%Y-%m-%d %H:%M')} to {end_date.strftime('%Y-%m-%d %H:%M')}\n")

        # Fetch and aggregate sales from receipts (ALL products in one pass)
        sales_data = self.fetch_and_aggregate_sales(start_date, end_date)

        if not sales_data:
            self.stdout.write(self.style.WARNING("No sales data found."))
            return

        # Calculate monthly needed quantities
        monthly_needed = self.calculate_monthly_needed(sales_data, days_back)

        # Display results
        self.display_results(monthly_needed)

        if not dry_run:
            # Update WeeklyOrderItem records
            updated_count = self.update_monthly_orders(monthly_needed, store_number)
            self.stdout.write(
                self.style.SUCCESS(
                    f"\n{'='*80}\nSuccessfully updated {updated_count} order items.\n{'='*80}\n"
                )
            )
        else:
            self.stdout.write(
                self.style.WARNING(f"\n{'='*80}\nDry run - no database updates performed.\n{'='*80}\n")
            )

    def fetch_and_aggregate_sales(
        self, start_date: datetime, end_date: datetime
    ) -> Dict[str, Dict[str, Decimal]]:
        """
        Fetch receipts from Korona API and aggregate sales by store and product.

        This processes ALL products in one API call cycle for efficiency.

        Returns:
            Dict with structure: {store_id: {product_id: total_quantity}}
        """
        session = get_session()
        url = build_url("receipts")

        # Format dates for API (ISO 8601 format with timezone)
        from_time = start_date.strftime("%Y-%m-%dT%H:%M:%S%z")
        to_time = end_date.strftime("%Y-%m-%dT%H:%M:%S%z")

        # If timezone info is missing, add it
        if not from_time.endswith(('+00:00', '-00:00')) and 'T' in from_time:
            from_time = start_date.strftime("%Y-%m-%dT%H:%M:%S-07:00")
            to_time = end_date.strftime("%Y-%m-%dT%H:%M:%S-07:00")

        sales = defaultdict(lambda: defaultdict(Decimal))
        page = 1
        total_receipts = 0
        total_items = 0

        self.stdout.write(f"Fetching receipts from {from_time} to {to_time}...\n")

        while True:
            params = {
                "minBookingTime": from_time,
                "maxBookingTime": to_time,
                "page": page,
                "size": 100,
            }

            try:
                response = session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                results = data.get("results", [])
                if not results:
                    break

                self.stdout.write(f"  Processing page {page}: {len(results)} receipts...")

                # Process ALL products from receipts
                for receipt in results:
                    # Skip voided or cancelled receipts
                    if receipt.get("voided") or receipt.get("cancelled"):
                        continue

                    # Get store information
                    org_unit = receipt.get("organizationalUnit", {})
                    store_id = org_unit.get("id")

                    if not store_id:
                        continue

                    total_receipts += 1

                    # Process all items in receipt
                    items = receipt.get("items", [])
                    for item in items:
                        product = item.get("product", {})
                        product_id = product.get("id")
                        quantity = Decimal(str(item.get("quantity", 0)))

                        # Only count positive quantities (exclude returns with negative qty)
                        if product_id and quantity > 0:
                            sales[store_id][product_id] += quantity
                            total_items += 1

                # Check if there are more pages
                pages_total = data.get("pagesTotal", 1)
                if page >= pages_total:
                    break

                page += 1

            except Exception as e:
                logger.error(f"Error fetching receipts: {e}")
                self.stdout.write(self.style.ERROR(f"Error: {e}"))
                break

        self.stdout.write(
            self.style.SUCCESS(
                f"\n✅ Processed {total_receipts} receipts with {total_items} product items\n"
            )
        )

        # Count unique products across all stores
        unique_products = set()
        for products in sales.values():
            unique_products.update(products.keys())

        self.stdout.write(f"📦 Found sales for {len(unique_products)} unique products across {len(sales)} stores\n")

        return sales

    def calculate_monthly_needed(
        self, sales_data: Dict[str, Dict[str, Decimal]], days_period: int
    ) -> Dict[str, Dict[str, int]]:
        """
        Calculate monthly needed quantities based on sales data.

        Formula: (Total sold in period / days in period) * 30 days

        Returns:
            Dict with structure: {store_id: {product_id: monthly_needed}}
        """
        monthly_needed = defaultdict(dict)

        for store_id, products in sales_data.items():
            for product_id, total_quantity in products.items():
                # Calculate average daily sales
                daily_avg = total_quantity / days_period

                # Calculate monthly need (30 days)
                monthly_qty = int((daily_avg * 30).quantize(Decimal("1")))

                # Store the result (minimum 1 if there were any sales)
                monthly_needed[store_id][product_id] = max(1, monthly_qty)

        return monthly_needed

    def display_results(self, monthly_needed: Dict[str, Dict[str, int]]):
        """Display calculated monthly needed quantities."""
        self.stdout.write("\n" + "=" * 80)
        self.stdout.write(self.style.SUCCESS("CALCULATED MONTHLY NEEDED QUANTITIES"))
        self.stdout.write("=" * 80 + "\n")

        total_products = 0

        for store_id, products in monthly_needed.items():
            # Get store info
            try:
                store = Store.objects.get(korona_id=store_id)
                store_name = f"{store.name} ({store.number})"
            except Store.DoesNotExist:
                store_name = f"Unknown Store ({store_id})"
                logger.warning(f"Store not found in database: {store_id}")
                continue

            self.stdout.write(f"\n{store_name}:")
            self.stdout.write("-" * 80)

            sorted_products = sorted(
                products.items(), key=lambda x: x[1], reverse=True
            )

            for product_id, quantity in sorted_products[:50]:  # Show top 50
                # Get product info
                try:
                    product = Product.objects.get(korona_id=product_id)
                    product_name = f"{product.number} - {product.name}"
                except Product.DoesNotExist:
                    product_name = f"Unknown Product"
                    logger.warning(f"Product not found in database: {product_id}")
                    continue

                self.stdout.write(f"  {product_name}: {quantity} units/month")
                total_products += 1

            if len(products) > 50:
                self.stdout.write(f"  ... and {len(products) - 50} more products")

        self.stdout.write(f"\n{'-'*80}")
        self.stdout.write(f"Total products calculated: {total_products}\n")

    def update_monthly_orders(
        self, monthly_needed: Dict[str, Dict[str, int]], store_number: str = None
    ) -> int:
        """Update WeeklyOrderItem records with calculated monthly_needed values."""
        updated_count = 0
        created_count = 0

        with transaction.atomic():
            for store_id, products in monthly_needed.items():
                # Get store
                try:
                    store = Store.objects.get(korona_id=store_id)

                    # Filter by store number if specified
                    if store_number and store.number != store_number:
                        continue

                except Store.DoesNotExist:
                    logger.warning(f"Store not found: {store_id}")
                    continue

                # Update ALL non-finalized lists for this store
                order_lists = WeeklyOrderList.objects.filter(
                    store=store,
                    finalized_at__isnull=True  # Only update non-finalized lists
                )

                # If no lists exist, create one for today
                if not order_lists.exists():
                    order_list = WeeklyOrderList.objects.create(
                        store=store,
                        target_date=timezone.now().date()
                    )
                    order_lists = [order_list]
                    self.stdout.write(
                        self.style.SUCCESS(f"\nCreated new order list for {store.name}")
                    )

                # Update monthly_needed for each product in ALL non-finalized lists
                for order_list in order_lists:
                    for product_id, quantity in products.items():
                        try:
                            product = Product.objects.get(korona_id=product_id)

                            # Get or create weekly order item
                            order_item, item_created = WeeklyOrderItem.objects.get_or_create(
                                order_list=order_list,
                                product=product,
                            )

                            # Update monthly_needed
                            order_item.monthly_needed = quantity
                            order_item.save(update_fields=["monthly_needed"])

                            if item_created:
                                created_count += 1
                            updated_count += 1

                        except Product.DoesNotExist:
                            logger.warning(f"Product not found: {product_id}")
                            continue

        self.stdout.write(
            self.style.SUCCESS(
                f"\nCreated {created_count} new items, updated {updated_count} total items"
            )
        )
        return updated_count
