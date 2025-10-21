from __future__ import annotations

from decimal import Decimal
from typing import Iterable, List, Optional, Sequence
from uuid import UUID

import requests
from django.core.management.base import BaseCommand
from django.db import transaction

from inventory.korona import fetch_product_stocks
from inventory.models import Product, ProductStock, Store


class Command(BaseCommand):
    help = "Synchronize product stock levels for the specified products (or all products)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--product",
            type=int,
            action="append",
            dest="product_numbers",
            help="Limit synchronization to the given product number (can be provided multiple times).",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Limit the number of products processed.",
        )

    def handle(self, *args, **options):
        """Run stock synchronization for one, many, or all products.

        Example (CLI):
            - Sync all products (warning: can be slow):
                python manage.py sync_stocks

            - Sync specific products by number:
                python manage.py sync_stocks --product 123 --product 456

            - Limit number processed (useful for testing):
                python manage.py sync_stocks --limit 50
        """
        product_numbers: Optional[List[int]] = options.get("product_numbers")
        limit: Optional[int] = options.get("limit")

        queryset = Product.objects.exclude(korona_id__isnull=True)
        if product_numbers:
            queryset = queryset.filter(number__in=product_numbers)

        if limit:
            queryset = queryset.order_by("number")[: limit]

        total = queryset.count()
        if total == 0:
            self.stdout.write("No products found to synchronize.")
            return

        store_map = {store.korona_id: store for store in Store.objects.all()}
        if not store_map:
            self.stderr.write(
                "No stores available. Run `python manage.py sync_stores` first."
            )
            return
        processed = 0
        created = 0
        updated = 0
        cleared = 0
        skipped = 0

        for product in queryset:
            processed += 1
            try:
                payload = fetch_product_stocks(product.korona_id)
            except requests.RequestException as exc:
                skipped += 1
                self.stderr.write(f"Failed to fetch stock for product {product.number}: {exc}")
                continue

            if payload is None:
                cleared += self._clear_product_stocks(product)
                continue

            results = payload.get("results", [])
            seen_store_ids: List[UUID] = []

            with transaction.atomic():
                for entry in results:
                    warehouse = entry.get("warehouse") or {}
                    warehouse_id = warehouse.get("id")
                    if not warehouse_id:
                        continue

                    try:
                        korona_store_id = UUID(str(warehouse_id))
                    except ValueError:
                        self.stderr.write(
                            f"Invalid warehouse UUID for product {product.number}: {warehouse_id}"
                        )
                        continue

                    store = store_map.get(korona_store_id)
                    if not store:
                        self.stderr.write(
                            f"Store with Korona ID {korona_store_id} not found locally. "
                            "Run sync_stores first."
                        )
                        continue

                    seen_store_ids.append(store.korona_id)
                    amount = entry.get("amount") or {}

                    stock, was_created = ProductStock.objects.update_or_create(
                        product=product,
                        store=store,
                        defaults={
                            "actual": Decimal(str(amount.get("actual", "0") or "0")),
                            "lent": Decimal(str(amount.get("lent", "0") or "0")),
                            "max_level": Decimal(str(amount.get("maxLevel", "0") or "0")),
                            "ordered": Decimal(str(amount.get("ordered", "0") or "0")),
                            "reorder_level": Decimal(str(amount.get("reorderLevel", "0") or "0")),
                            "average_purchase_price": Decimal(
                                str(entry.get("averagePurchasePrice", "0") or "0")
                            ),
                            "listed": bool(entry.get("listed", False)),
                        },
                    )
                    if was_created:
                        created += 1
                    else:
                        updated += 1

                # Remove stale stock rows for this product if not returned.
                if seen_store_ids:
                    ProductStock.objects.filter(product=product).exclude(
                        store__korona_id__in=seen_store_ids
                    ).delete()

            if processed % 50 == 0 or processed == total:
                self.stdout.write(f"Processed {processed}/{total} products...")

        self.stdout.write(
            self.style.SUCCESS(
                f"Stock sync complete. Processed: {processed}, Created: {created}, "
                f"Updated: {updated}, Cleared: {cleared}, Skipped: {skipped}"
            )
        )

    def _clear_product_stocks(self, product: Product) -> int:
        """Delete all existing stock rows for a product.

        Args:
            product: The product whose stocks should be cleared.

        Returns:
            Number of rows deleted.

        Example:
            >>> from inventory.models import Product
            >>> p = Product.objects.first()
            >>> Command()._clear_product_stocks(p)  # doctest: +SKIP
            42
        """
        deleted, _ = ProductStock.objects.filter(product=product).delete()
        return deleted
