from django.core.management.base import BaseCommand
from inventory.korona import iter_paginated


class Command(BaseCommand):
    help = "Probe Korona products and print fields that look like supplier order codes."

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, default=10, help='Number of products to inspect (default 10)')

    def handle(self, *args, **options):
        limit = max(1, int(options['limit']))
        count = 0
        for p in iter_paginated('products'):
            num = p.get('number')
            name = (p.get('name') or '')[:50]
            codes = []
            # Collect candidates under supplierPrices
            for sp in (p.get('supplierPrices') or []):
                if not isinstance(sp, dict):
                    continue
                for key in (
                    'supplierProductNumber','supplierProductCode','supplierItemNumber','supplierSku',
                    'orderNumber','articleNumber','itemNumber'
                ):
                    v = sp.get(key)
                    if v:
                        codes.append((f'supplierPrices.{key}', str(v)))
                prod = sp.get('product')
                if isinstance(prod, dict):
                    for key in ('supplierProductNumber','orderNumber','articleNumber'):
                        v = prod.get(key)
                        if v:
                            codes.append((f'supplierPrices.product.{key}', str(v)))
            # Top level fallbacks
            for key in ('supplierProductNumber','orderNumber','articleNumber'):
                v = p.get(key)
                if v:
                    codes.append((key, str(v)))
            self.stdout.write(f"#{num} {name}")
            if codes:
                for k, v in codes[:6]:
                    self.stdout.write(f"  - {k}: {v}")
            else:
                self.stdout.write("  (no obvious order code fields)")
            self.stdout.write("")
            count += 1
            if count >= limit:
                break
