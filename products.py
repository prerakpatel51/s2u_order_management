import csv
import requests
from pathlib import Path
from typing import Dict, Generator, List, Optional, Sequence, Union

# === Korona API Config ===
KORONA_BASE = "https://185.koronacloud.com/web/api/v3"
KORONA_ACCOUNT_ID = "5c3aa492-05d9-45d9-9ba9-2f3f82c9e8b8"
KORONA_USER = "testuser"
KORONA_PASS = "Kiranpatel@3210"


def _products_url() -> str:
    """Constructs the full products endpoint URL."""
    return f"{KORONA_BASE}/accounts/{KORONA_ACCOUNT_ID}/products"


def iter_products(page_size: int = 200) -> Generator[dict, None, None]:
    """Fetch all products (handles pagination automatically)."""
    page = 1
    session = requests.Session()
    auth = (KORONA_USER, KORONA_PASS)

    while True:
        params = {"page": page, "size": page_size, "omitPageCounts": True}
        try:
            response = session.get(_products_url(), auth=auth, params=params, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching products page {page}: {e}")
            break

        data = response.json() if response.content else {}
        # Korona API sometimes wraps results in {"results": [...]}
        items = data if isinstance(data, list) else data.get("results", data or [])
        if not items:
            break

        for product in items:
            yield product
        page += 1


def extract_barcode(p: dict) -> str:
    """Extracts the first productCode from the 'codes' list."""
    codes = p.get("codes")
    if isinstance(codes, list) and codes:
        first = codes[0]
        if isinstance(first, dict):
            return str(first.get("productCode", "")).strip()
        elif isinstance(first, str):
            return first.strip()
    return ""


def extract_supplier(p: dict) -> str:
    """
    Extracts the first supplier name from 'supplierPrices'.
    Returns supplier_name (string only).
    """
    supplier_prices = p.get("supplierPrices") or []
    if isinstance(supplier_prices, list) and supplier_prices:
        for s in supplier_prices:
            supplier = s.get("supplier")
            if isinstance(supplier, dict):
                name = supplier.get("name")
                if isinstance(name, str) and name.strip():
                    return name.strip()
    return ""

def get_all_products() -> List[Dict[str, str]]:
    """Fetch all products with number, name, barcode, and supplier name (no UUID)."""
    rows: List[Dict[str, str]] = []

    for p in iter_products(page_size=200):
        pnumber = str(p.get("number", "")).strip()   # 👈 Product Number
        pname = str(p.get("name", "")).strip()
        barcode = extract_barcode(p)
        supplier_name = extract_supplier(p)

        rows.append(
            {
                "number": pnumber,
                "name": pname,
                "barcode": barcode,
                "supplier_name": supplier_name,
            }
        )
    return rows


def save_products_csv(
    filename: Union[str, Path] = "products_with_supplier.csv",
    products: Optional[Sequence[Dict[str, str]]] = None,
) -> str:
    """Save the products with number, name, barcode, and supplier (no UUID)."""
    if products is None:
        products = get_all_products()

    path = Path(filename)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["number", "name", "barcode", "supplier_name"]
        )
        writer.writeheader()
        writer.writerows(products)

    print(f"✅ Saved {len(products)} products (without UUID) to '{path}'")
    return str(path)

if __name__ == "__main__":
    save_products_csv("products_with_supplier.csv")
