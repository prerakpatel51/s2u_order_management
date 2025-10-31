"""
Populate MonthlySales for all products across all (active) stores by scanning
Korona receipts once.

Usage examples:
  python manage.py sync_all_monthly_sales
  python manage.py sync_all_monthly_sales --days 14
  python manage.py sync_all_monthly_sales --store-number 6300
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict

from django.core.management.base import BaseCommand
from django.utils import timezone

from inventory.korona import get_session, build_url
from inventory.models import MonthlySales, Product, Store
from inventory.redis_client import r as redis_client

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Scan receipts and cache monthly sales for all products in MonthlySales."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=30, help="Lookback window in days (default: 30)")
        parser.add_argument("--store-number", type=str, default=None, help="Limit to a single store number (optional)")

    def handle(self, *args, **opts):
        days = int(opts.get("days") or 30)
        store_number = opts.get("store_number")

        now = timezone.now()
        start = now - timedelta(days=days)

        self.stdout.write(self.style.SUCCESS(
            f"Scanning receipts for last {days} days ({start:%Y-%m-%d} to {now:%Y-%m-%d})"))

        sales = self._aggregate_sales(start, now)
        if not sales:
            self.stdout.write(self.style.WARNING("No receipts found in window."))
            return

        # Map Korona IDs -> DB IDs once
        store_ids = list(sales.keys())
        stores = {str(s.korona_id): s for s in Store.objects.filter(korona_id__in=store_ids, active=True)}
        if store_number:
            stores = {k: v for k, v in stores.items() if v.number == str(store_number)}

        product_ids: set[str] = set()
        for per_store in sales.values():
            product_ids.update(per_store.keys())
        products = {str(p.korona_id): p for p in Product.objects.filter(korona_id__in=list(product_ids))}

        total_pairs = 0
        upserts = []
        now_ts = timezone.now()
        for store_kid, per_store in sales.items():
            store_obj = stores.get(str(store_kid))
            if not store_obj:
                continue
            for product_kid, qty in per_store.items():
                product_obj = products.get(str(product_kid))
                if not product_obj:
                    continue
                total_pairs += 1
                upserts.append((product_obj, store_obj, int(qty)))

        if not upserts:
            self.stdout.write(self.style.WARNING("No matching store/product pairs to update."))
            return

        # Try bulk upsert; fall back to per-row update_or_create if unavailable
        updated = 0
        created = 0
        try:
            objs = [
                MonthlySales(product=p, store=s, quantity_sold=q, days_calculated=days, calculated_at=now_ts)
                for (p, s, q) in upserts
            ]
            MonthlySales.objects.bulk_create(
                objs,
                update_conflicts=True,
                update_fields=["quantity_sold", "days_calculated", "calculated_at"],
                unique_fields=["product", "store"],
            )
            updated = len(objs)  # approximate; we don't get exact created/updated splits here
        except Exception:  # pragma: no cover - fallback path
            updated = 0
            for (p, s, q) in upserts:
                _, was_created = MonthlySales.objects.update_or_create(
                    product=p,
                    store=s,
                    defaults={"quantity_sold": q, "days_calculated": days},
                )
                if was_created:
                    created += 1
                else:
                    updated += 1

        # Warm Redis for fast API/UI responses
        warmed = 0
        for (p, s, q) in upserts:
            try:
                redis_client.set(f"monthly_sales:{p.number}:{s.id}", int(q), ex=3600)
                warmed += 1
            except Exception:
                pass

        self.stdout.write(self.style.SUCCESS(
            f"MonthlySales upsert complete. Pairs: {total_pairs}, updated~: {updated}, created: {created}, redis warmed: {warmed}"
        ))

    def _aggregate_sales(self, start, end) -> Dict[str, Dict[str, Decimal]]:
        """Scan receipts once and aggregate quantities per store/product."""
        session = get_session()
        url = build_url("receipts")
        from_time = start.strftime('%Y-%m-%dT00:00:00-07:00')
        to_time = end.strftime('%Y-%m-%dT23:59:59-07:00')

        sales: Dict[str, Dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
        page = 1
        total = 0
        while True:
            params = {
                'minBookingTime': from_time,
                'maxBookingTime': to_time,
                'page': page,
                'size': 100,
            }
            try:
                resp = session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json() or {}
            except Exception as exc:  # noqa: BLE001
                logger.error("Korona receipts fetch failed: %s", exc)
                break

            results = data.get('results') or []
            if not results:
                break
            for receipt in results:
                if receipt.get('voided') or receipt.get('cancelled'):
                    continue
                org = receipt.get('organizationalUnit') or {}
                store_id = org.get('id')
                if not store_id:
                    continue
                for item in receipt.get('items') or []:
                    prod = (item.get('product') or {}).get('id')
                    qty = Decimal(str(item.get('quantity') or 0))
                    if prod and qty > 0:
                        sales[str(store_id)][str(prod)] += qty
                        total += 1
            if page >= (data.get('pagesTotal') or 1):
                break
            page += 1

        logger.info("Aggregated %d line items across %d stores", total, len(sales))
        return sales

