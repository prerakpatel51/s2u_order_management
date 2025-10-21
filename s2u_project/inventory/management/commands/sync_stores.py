from typing import Dict, List, Optional
from uuid import UUID

from django.core.management.base import BaseCommand, CommandError

from inventory.korona import iter_paginated
from inventory.models import Store


class Command(BaseCommand):
    help = "Synchronize Korona organizational units (stores) into the database."

    def handle(self, *args, **options):
        """Fetch stores from Korona and upsert them locally.

        Example (CLI):
            python manage.py sync_stores
        """
        stores_data = list(iter_paginated("organizationalUnits"))
        if not stores_data:
            raise CommandError("No organizational unit data returned from Korona.")

        existing = {store.korona_id: store for store in Store.objects.all()}
        seen_ids: List[UUID] = []
        created = 0
        updated = 0

        for item in stores_data:
            korona_id_raw = item.get("id")
            if not korona_id_raw:
                self.stderr.write("Skipping store without an ID.")
                continue

            try:
                korona_id = UUID(str(korona_id_raw))
            except ValueError:
                self.stderr.write(f"Skipping store with invalid UUID: {korona_id_raw}")
                continue

            seen_ids.append(korona_id)
            defaults = self._build_store_defaults(item)

            store, was_created = Store.objects.update_or_create(
                korona_id=korona_id,
                defaults=defaults,
            )
            if was_created:
                created += 1
            else:
                updated += 1

        # Optionally deactivate stores not returned anymore.
        missing_ids = set(existing.keys()) - set(seen_ids)
        if missing_ids:
            Store.objects.filter(korona_id__in=missing_ids).update(active=False)

        self.stdout.write(
            self.style.SUCCESS(
                f"Stores synchronized. Created: {created}, Updated: {updated}, Deactivated: {len(missing_ids)}"
            )
        )

    def _build_store_defaults(self, item: Dict) -> Dict:
        """Normalize Korona store payload into :class:`Store` defaults dict.

        Args:
            item: A single organizational unit payload from Korona.

        Returns:
            Dict of fields suitable for :func:`Store.objects.update_or_create`.
        """
        address = item.get("address") or {}
        return {
            "number": str(item.get("number", "")).strip(),
            "name": str(item.get("name", "")).strip(),
            "address_line1": address.get("addressLine1", "") or "",
            "address_line2": address.get("addressLine2", "") or "",
            "city": address.get("city", "") or "",
            "state": address.get("state", "") or "",
            "zip_code": address.get("zipCode", "") or "",
            "country": address.get("country", "") or "",
            "company": address.get("company", "") or "",
            "active": bool(item.get("active", True)),
            "warehouse": bool(item.get("warehouse", False)),
            "org_from_order": bool(item.get("orgFromOrder", False)),
            "receipt_share_enabled": bool(item.get("receiptShareEnabled", False)),
        }
