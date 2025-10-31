from __future__ import annotations

import logging
from celery import shared_task
from django.core.management import call_command

logger = logging.getLogger(__name__)


@shared_task
def sync_stores_task():
    logger.info("[celery] sync_stores start")
    call_command("sync_stores")
    logger.info("[celery] sync_stores done")


@shared_task
def load_products_task():
    logger.info("[celery] load_products --skip-csv start")
    call_command("load_products", "--skip-csv")
    logger.info("[celery] load_products done")


@shared_task
def sync_stocks_task():
    logger.info("[celery] sync_stocks start")
    call_command("sync_stocks")
    logger.info("[celery] sync_stocks done")


@shared_task
def sync_all_monthly_sales_task(days: int = 30):
    logger.info("[celery] sync_all_monthly_sales --days=%s start", days)
    call_command("sync_all_monthly_sales", "--days", str(days))
    logger.info("[celery] sync_all_monthly_sales done")


@shared_task
def nightly_full_sync(days: int = 30):
    """Run the full nightly chain sequentially.

    Intended to be scheduled by Celery Beat.
    """
    logger.info("[celery] nightly_full_sync (days=%s) start", days)
    call_command("sync_stores")
    call_command("load_products", "--skip-csv")
    call_command("sync_stocks")
    call_command("sync_all_monthly_sales", "--days", str(days))
    logger.info("[celery] nightly_full_sync done")

