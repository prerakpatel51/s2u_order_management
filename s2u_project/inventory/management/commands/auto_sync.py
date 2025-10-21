"""Auto-sync command to periodically fetch new stores and products from Korona API."""
import logging
from django.core.management import call_command
from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Auto-sync stores and products from Korona API. Use with cron or scheduler."

    def add_arguments(self, parser):
        parser.add_argument(
            "--stores-only",
            action="store_true",
            help="Only sync stores, skip products",
        )
        parser.add_argument(
            "--products-only",
            action="store_true",
            help="Only sync products, skip stores",
        )

    def handle(self, *args, **options):
        """Run scheduled syncs for stores and/or products.

        Example (CLI):
            # Sync both stores and products
            python manage.py auto_sync

            # Only stores
            python manage.py auto_sync --stores-only

            # Only products
            python manage.py auto_sync --products-only
        """
        stores_only = options.get("stores_only", False)
        products_only = options.get("products_only", False)

        if not products_only:
            self.stdout.write("Syncing stores from Korona...")
            try:
                call_command("sync_stores")
                logger.info("Store sync completed successfully")
            except Exception as exc:
                logger.error(f"Store sync failed: {exc}")
                self.stderr.write(self.style.ERROR(f"Store sync failed: {exc}"))

        if not stores_only:
            self.stdout.write("Syncing products from Korona...")
            try:
                call_command("load_products", "--skip-csv")
                logger.info("Product sync completed successfully")
            except Exception as exc:
                logger.error(f"Product sync failed: {exc}")
                self.stderr.write(self.style.ERROR(f"Product sync failed: {exc}"))

        self.stdout.write(self.style.SUCCESS("Auto-sync completed"))
