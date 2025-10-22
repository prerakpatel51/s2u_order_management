"""Utility helpers for interacting with the Korona Cloud API."""

from __future__ import annotations

import logging
import os
import time
from typing import Dict, Generator, Iterable, Optional
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pybreaker import CircuitBreaker

# Read from environment variables for security - NO FALLBACKS for security
# These MUST be set in environment or .env file
KORONA_BASE = os.environ["KORONA_BASE_URL"]
KORONA_ACCOUNT_ID = os.environ["KORONA_ACCOUNT_ID"]
KORONA_USER = os.environ["KORONA_USER"]
KORONA_PASS = os.environ["KORONA_PASSWORD"]

logger = logging.getLogger(__name__)

# Circuit breaker configuration: open after 5 failures, reset after 30s
korona_breaker = CircuitBreaker(
    fail_max=5,
    reset_timeout=30,  # Seconds before trying again after opening
    exclude=[requests.HTTPError],  # Don't break on 4xx errors (client errors)
    name="korona_api"
)

# Use Django cache instead of in-memory dict for production
from .redis_client import r as redis_client
from .redis_client import scan_delete
CACHE_TTL_SECONDS = 3600  # 1 hour cache per requirement


def build_url(path: str) -> str:
    """Build a full Korona Cloud API URL for an account-scoped path.

    Args:
        path: Path relative to the account (e.g. "products"). Leading slashes are allowed.

    Returns:
        Fully-qualified URL string.

    Example:
        >>> build_url("products")  # doctest: +ELLIPSIS
        '.../accounts/.../products'
    """
    trimmed = path.lstrip("/")
    return f"{KORONA_BASE}/accounts/{KORONA_ACCOUNT_ID}/{trimmed}"


def get_session() -> requests.Session:
    """Get a requests session with retry logic and connection pooling.

    Returns:
        A configured ``requests.Session`` with auth, headers, and retry adapter.

    Example:
        >>> s = get_session()
        >>> isinstance(s.headers.get('User-Agent'), str)
        True
    """
    session = requests.Session()
    session.auth = (KORONA_USER, KORONA_PASS)
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "S2U-Inventory/1.0"
    })

    # Retry strategy: 3 retries with exponential backoff
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,  # 0.5s, 1s, 2s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=20,
        pool_block=False
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


@korona_breaker
def _api_request(session: requests.Session, url: str, params: dict, timeout: tuple) -> requests.Response:
    """Make an API request with circuit breaker protection.

    Args:
        session: Configured requests session.
        url: Fully-qualified endpoint URL.
        params: Query parameters to include.
        timeout: ``(connect, read)`` timeout tuple in seconds.

    Returns:
        The successful ``requests.Response`` object.
    """
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response


def iter_paginated(path: str, params: Optional[Dict[str, str]] = None) -> Generator[dict, None, None]:
    """Yield all results from a paginated Korona endpoint.

    Applies retries, sane timeouts, and a circuit breaker to protect the API.

    Args:
        path: Endpoint path relative to the account (e.g. "products").
        params: Optional query parameters merged into each page request.

    Yields:
        Each result item (dict) from the API across all pages.

    Example:
        >>> gen = iter_paginated('products')
        >>> hasattr(gen, '__iter__') and hasattr(gen, '__next__')
        True
    """
    session = get_session()
    page = 1
    while True:
        query = {"page": page, "size": 200, "omitPageCounts": True}
        if params:
            query.update(params)
        url = build_url(path)
        try:
            # Timeout: 5s connect, 15s read
            response = _api_request(session, url, query, timeout=(5, 15))
        except requests.RequestException as exc:
            logger.error("Korona request failed for %s?%s: %s", path, urlencode(query), exc)
            raise

        data = response.json() if response.content else {}
        items = data.get("results")
        if items is None:
            items = data if isinstance(data, list) else []

        if not items:
            break

        for item in items:
            yield item

        page += 1
        if page > 100:  # Safety limit: max 100 pages (20k records)
            logger.warning("Pagination limit reached for %s", path)
            break


def fetch_product_stocks(product_id: str, force_refresh: bool = False) -> Optional[dict]:
    """Return stock payload for a product ID with caching + breaker.

    Args:
        product_id: The Korona product UUID.
        force_refresh: If True, bypass cache and fetch fresh data.

    Returns:
        Parsed JSON payload (dict) or ``None`` when API returns 204.

    Example:
        >>> # Fetch, using cache when available
        >>> data = fetch_product_stocks('00000000-0000-0000-0000-000000000000')  # doctest: +SKIP
        >>> isinstance(data, (dict, type(None)))  # doctest: +SKIP
        True
    """
    cache_key = f"stock:{product_id}"

    # Check cache if not forcing refresh
    if not force_refresh:
        cached_raw = redis_client.get(cache_key)
        if cached_raw is not None:
            try:
                import json as _json
                cached_data = _json.loads(cached_raw)
            except Exception:
                cached_data = None
            if cached_data is not None:
                logger.debug("Redis cache hit for product %s", product_id)
                return cached_data

    # Fetch from API with circuit breaker
    session = get_session()
    url = build_url(f"products/{product_id}/stocks")
    try:
        # Timeout: 5s connect, 15s read
        response = _api_request(session, url, {}, timeout=(5, 15))
        if response.status_code == 204:
            data = None
        else:
            data = response.json()
    except requests.RequestException as exc:
        logger.error("Failed to fetch stock for product %s: %s", product_id, exc)
        # Try to return stale cache if available
        stale_data = None
        stale_raw = redis_client.get(cache_key)
        if stale_raw is not None:
            try:
                import json as _json
                stale_data = _json.loads(stale_raw)
            except Exception:
                stale_data = None
        if stale_data is not None:
            logger.warning("Returning stale cache for product %s due to API failure", product_id)
            return stale_data
        raise

    # Store in cache for 5 minutes
    try:
        import json as _json
        redis_client.set(cache_key, _json.dumps(data), ex=CACHE_TTL_SECONDS)
    except Exception:
        pass
    return data


def clear_stock_cache(product_id: Optional[str] = None) -> None:
    """Clear the stock cache for a specific product or all products.

    Args:
        product_id: When provided, clear only that product's cache. If ``None``,
            clear all stock cache entries (requires a cache backend with pattern delete).

    Example:
        >>> clear_stock_cache('00000000-0000-0000-0000-000000000000')  # doctest: +SKIP
    """
    if product_id:
        redis_client.delete(f"stock:{product_id}")
    else:
        scan_delete("stock:*")


def calculate_monthly_sales(product_id: str, store_id: str, days: int = 30) -> int:
    """Calculate monthly sales for a product at a single store.

    Uses the receipts API to sum quantities and extrapolate to a 30‑day period.

    Args:
        product_id: Korona product UUID.
        store_id: Korona store/organizational unit UUID.
        days: Number of days to look back (default: 30).

    Returns:
        Integer quantity sold in the given lookback window.

    Example:
        >>> calculate_monthly_sales('prod-uuid', 'store-uuid', days=7)  # doctest: +SKIP
        12
    """
    from datetime import datetime, timedelta

    session = get_session()
    url = build_url("receipts")

    # Calculate date range (same as get_product_sales.py)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    from_time = start_date.strftime('%Y-%m-%dT00:00:00-07:00')
    to_time = end_date.strftime('%Y-%m-%dT23:59:59-07:00')

    total_qty = 0.0
    page = 1

    logger.info(f"Fetching receipts for product {product_id} at store {store_id} from {from_time} to {to_time}")

    while True:  # No page limit, use pagesTotal like the working script
        params = {
            'minBookingTime': from_time,
            'maxBookingTime': to_time,
            'page': page,
            'size': 100
        }

        try:
            response = _api_request(session, url, params, timeout=(5, 30))
            data = response.json()
            results = data.get('results', [])

            if not results:
                logger.info(f"No more results at page {page}")
                break

            logger.info(f"Processing page {page}, found {len(results)} receipts")

            # Process receipts (exact logic from get_product_sales.py)
            for receipt in results:
                # Skip voided or cancelled
                if receipt.get('voided') or receipt.get('cancelled'):
                    continue

                # Check store
                org_unit = receipt.get('organizationalUnit', {})
                if org_unit.get('id') != store_id:
                    continue

                # Check items for our product
                for item in receipt.get('items', []):
                    product = item.get('product', {})
                    if product.get('id') == product_id:
                        qty = item.get('quantity', 0)
                        total_qty += qty
                        logger.debug(f"Found sale: receipt={receipt.get('number')}, qty={qty}, total={total_qty}")

            # Check if more pages
            if page >= data.get('pagesTotal', 1):
                logger.info(f"Reached last page {page}")
                break
            page += 1

        except Exception as exc:
            logger.error(f"Error fetching receipts for product {product_id}: {exc}", exc_info=True)
            break

    logger.info(f"Product {product_id} at store {store_id}: {total_qty} units sold in {days} days")
    return int(total_qty)


def calculate_monthly_sales_bulk(product_id: str, stores: list[tuple[int, str]], days: int = 30) -> dict[int, int]:
    """Calculate monthly sales for a product across multiple stores efficiently.

    Fetch receipts once and aggregate per store, which is much faster than
    per‑store calls.

    Args:
        product_id: Korona product UUID.
        stores: List of ``(store_db_id, store_korona_id)`` tuples.
        days: Number of days to look back (default: 30).

    Returns:
        Mapping of ``store_db_id -> quantity_sold``.

    Example:
        >>> stores = [(1, 'store-uuid-1'), (2, 'store-uuid-2')]
        >>> calculate_monthly_sales_bulk('prod-uuid', stores)  # doctest: +SKIP
        {1: 5, 2: 0}
    """
    from datetime import datetime, timedelta

    # Initialize results dictionary
    results = {store_db_id: 0 for store_db_id, _ in stores}

    # Create mapping of korona_id -> db_id for quick lookup
    store_map = {str(korona_id): db_id for db_id, korona_id in stores}

    session = get_session()
    url = build_url("receipts")

    # Calculate date range (same as get_product_sales.py)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    from_time = start_date.strftime('%Y-%m-%dT00:00:00-07:00')
    to_time = end_date.strftime('%Y-%m-%dT23:59:59-07:00')

    page = 1
    total_receipts = 0

    logger.info(f"BULK: Fetching receipts for product {product_id} across {len(stores)} stores from {from_time} to {to_time}")

    while True:
        params = {
            'minBookingTime': from_time,
            'maxBookingTime': to_time,
            'page': page,
            'size': 100
        }

        try:
            response = _api_request(session, url, params, timeout=(5, 30))
            data = response.json()
            receipts = data.get('results', [])

            if not receipts:
                logger.info(f"BULK: No more results at page {page}")
                break

            total_receipts += len(receipts)
            logger.info(f"BULK: Processing page {page}, found {len(receipts)} receipts (total: {total_receipts})")

            # Process receipts for ALL stores at once
            for receipt in receipts:
                # Skip voided or cancelled
                if receipt.get('voided') or receipt.get('cancelled'):
                    continue

                # Get store ID
                org_unit = receipt.get('organizationalUnit', {})
                store_korona_id = org_unit.get('id')

                # Check if this receipt is for one of our stores
                if store_korona_id not in store_map:
                    continue

                # Check items for our product
                for item in receipt.get('items', []):
                    product = item.get('product', {})
                    if product.get('id') == product_id:
                        qty = item.get('quantity', 0)
                        store_db_id = store_map[store_korona_id]
                        results[store_db_id] += qty
                        logger.debug(f"BULK: Found sale at store {store_korona_id}: qty={qty}")

            # Check if more pages
            if page >= data.get('pagesTotal', 1):
                logger.info(f"BULK: Reached last page {page}")
                break
            page += 1

        except Exception as exc:
            logger.error(f"BULK: Error fetching receipts for product {product_id}: {exc}", exc_info=True)
            break

    # Log results summary
    for store_db_id, qty in results.items():
        logger.info(f"BULK: Store {store_db_id}: {qty} units sold in {days} days")

    logger.info(f"BULK: Completed - processed {total_receipts} receipts across {page} pages for {len(stores)} stores")

    return {db_id: int(qty) for db_id, qty in results.items()}
