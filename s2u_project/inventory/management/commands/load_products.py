import csv
from pathlib import Path
from typing import Dict, List, Sequence
from uuid import UUID

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from inventory.korona import iter_paginated
from inventory.models import Product


def extract_barcode(product: dict) -> str:
    """Extract a barcode string from a Korona product payload.

    Args:
        product: Raw product dict as returned by the Korona API.

    Returns:
        The first barcode string if available, otherwise an empty string.

    Example:
        >>> extract_barcode({"codes": [{"productCode": "0123456789"}]})
        '0123456789'
    """
    codes = product.get("codes")
    if isinstance(codes, list) and codes:
        first = codes[0]
        if isinstance(first, dict):
            return str(first.get("productCode", "")).strip()
        if isinstance(first, str):
            return first.strip()
    return ""


def extract_supplier(product: dict) -> str:
    """Extract the primary supplier name from a Korona product payload.

    Args:
        product: Raw product dict from the Korona API.

    Returns:
        Supplier name if found, otherwise an empty string.

    Example:
        >>> extract_supplier({"supplierPrices": [{"supplier": {"name": "ACME"}}]})
        'ACME'
    """
    supplier_prices = product.get("supplierPrices") or []
    if isinstance(supplier_prices, list):
        for entry in supplier_prices:
            supplier = entry.get("supplier")
            if isinstance(supplier, dict):
                name = supplier.get("name")
                if isinstance(name, str) and name.strip():
                    return name.strip()
    return ""


def fetch_products() -> List[Dict[str, str]]:
    """Fetch all products from Korona and normalize fields for storage.

    Returns:
        A list of dicts with keys: id, number, name, barcode, supplier_name.

    Example:
        >>> products = fetch_products()
        >>> products[0].keys() >= {"number", "name"}
        True
    """
    rows: List[Dict[str, str]] = []
    for product in iter_paginated("products"):
        rows.append(
            {
                "id": product.get("id"),
                "number": str(product.get("number", "")).strip(),
                "name": str(product.get("name", "")).strip(),
                "barcode": extract_barcode(product),
                "supplier_name": extract_supplier(product),
            }
        )
    return rows


def save_products_csv(filename: Path, products: Sequence[Dict[str, str]]) -> None:
    """Write normalized product rows to a CSV file.

    Args:
        filename: Destination CSV path.
        products: Sequence of normalized product dicts.

    Example:
        >>> save_products_csv(Path("/tmp/products.csv"), [{"number": 1, "name": "Foo"}])
    """
    with filename.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=["number", "name", "barcode", "supplier_name"],
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(products)


class Command(BaseCommand):
    help = "Fetch products from Korona API, update the database, and write the CSV."

    def add_arguments(self, parser):
        default_csv = Path(settings.BASE_DIR).parent / "products_with_supplier.csv"
        parser.add_argument(
            "--csv",
            type=Path,
            default=default_csv,
            help=f"Where to write the CSV output (default: {default_csv})",
        )
        parser.add_argument(
            "--skip-csv",
            action="store_true",
            help="Do not write the CSV file; only update the database.",
        )

    def handle(self, *args, **options):
        """Run the synchronization logic.

        Example (CLI):
            - Sync products and write CSV (default path):
                python manage.py load_products

            - Sync without writing a CSV:
                python manage.py load_products --skip-csv

            - Custom CSV output path:
                python manage.py load_products --csv /tmp/products.csv
        """
        products = fetch_products()
        if not products:
            raise CommandError("No products were returned from the Korona API.")

        created, updated = self._save_to_db(products)
        csv_path: Path = options["csv"]
        if not options["skip_csv"]:
            save_products_csv(csv_path, products)
            self.stdout.write(self.style.SUCCESS(f"Wrote CSV to {csv_path}"))

        self.stdout.write(
            self.style.SUCCESS(
                f"Products synced successfully. Created: {created}, Updated: {updated}"
            )
        )

    def _save_to_db(self, products: Sequence[Dict[str, str]]) -> tuple[int, int]:
        """Insert or update products in the database.

        Args:
            products: Normalized product dicts from :func:`fetch_products`.

        Returns:
            A tuple of (created_count, updated_count).

        Example:
            >>> Command()._save_to_db([{"number": 1, "name": "Test"}])
            (1, 0)
        """
        created, updated = 0, 0
        for row in products:
            number_raw = row.get("number")
            name = str(row.get("name", "")).strip()
            barcode = str(row.get("barcode", "")).strip()
            supplier_name = str(row.get("supplier_name", "")).strip()
            korona_id = row.get("id")

            if not number_raw or not name:
                continue

            try:
                number = int(number_raw)
            except (TypeError, ValueError):
                self.stderr.write(
                    f"Skipping row with invalid product number: {number_raw!r}"
                )
                continue

            defaults = {
                "name": name,
                "barcode": barcode,
                "supplier_name": supplier_name,
            }

            if korona_id:
                try:
                    defaults["korona_id"] = UUID(str(korona_id))
                except ValueError:
                    self.stderr.write(
                        f"Invalid Korona UUID for product {number}: {korona_id}"
                    )

            _, was_created = Product.objects.update_or_create(
                number=number,
                defaults=defaults,
            )
            if was_created:
                created += 1
            else:
                updated += 1

        return created, updated
