from django.db import models
from django.conf import settings


class Store(models.Model):
    """Represents a Korona organizational unit / store location."""

    korona_id = models.UUIDField(unique=True)
    number = models.CharField(max_length=32, db_index=True)
    name = models.CharField(max_length=255)
    address_line1 = models.CharField(max_length=255, blank=True)
    address_line2 = models.CharField(max_length=255, blank=True)
    city = models.CharField(max_length=128, blank=True)
    state = models.CharField(max_length=64, blank=True)
    zip_code = models.CharField(max_length=32, blank=True)
    country = models.CharField(max_length=64, blank=True)
    company = models.CharField(max_length=255, blank=True)
    active = models.BooleanField(default=True)
    warehouse = models.BooleanField(default=False)
    org_from_order = models.BooleanField(default=False)
    receipt_share_enabled = models.BooleanField(default=False)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return f"{self.name} ({self.number})"


class Product(models.Model):
    """Stores a single product fetched from Korona."""

    number = models.IntegerField(primary_key=True)
    korona_id = models.UUIDField(null=True, blank=True, unique=True)
    name = models.CharField(max_length=255, db_index=True)
    barcode = models.CharField(max_length=64, blank=True)
    supplier_name = models.CharField(max_length=255, blank=True)
    order_code = models.CharField(max_length=64, blank=True)

    stores = models.ManyToManyField(
        Store,
        through="ProductStock",
        related_name="products",
    )

    def __str__(self) -> str:
        return f"{self.number} - {self.name}"


class ProductBarcode(models.Model):
    """Additional barcodes for a product (Korona may return multiple)."""

    product = models.ForeignKey(
        Product,
        on_delete=models.CASCADE,
        related_name="barcodes",
    )
    code = models.CharField(max_length=64)

    class Meta:
        unique_together = ("product", "code")
        indexes = [models.Index(fields=["code"])]

    def __str__(self) -> str:
        return f"{self.product.number}: {self.code}"


class ProductStock(models.Model):
    """Inventory quantities for a product at a particular store."""

    product = models.ForeignKey(
        Product,
        on_delete=models.CASCADE,
        related_name="stocks",
    )
    store = models.ForeignKey(
        Store,
        on_delete=models.CASCADE,
        related_name="stocks",
    )
    actual = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    lent = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    max_level = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    ordered = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    reorder_level = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    average_purchase_price = models.DecimalField(
        max_digits=12,
        decimal_places=4,
        default=0,
    )
    listed = models.BooleanField(default=False)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("product", "store")
        ordering = ["store__name", "product__name"]

    def __str__(self) -> str:
        return f"{self.product.number} @ {self.store.number}"


class WeeklyOrderList(models.Model):
    """Represents a weekly order list for a specific store and date."""

    store = models.ForeignKey(Store, on_delete=models.CASCADE, related_name="weekly_lists")
    target_date = models.DateField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    finalized_at = models.DateTimeField(null=True, blank=True)
    finalized_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="finalized_lists",
    )

    class Meta:
        ordering = ["-target_date", "store__name", "-created_at"]

    def __str__(self) -> str:
        return f"{self.store.name} - {self.target_date.isoformat()}"


class WeeklyOrderItem(models.Model):
    """Items included within a weekly order list."""

    order_list = models.ForeignKey(
        WeeklyOrderList,
        on_delete=models.CASCADE,
        related_name="items",
    )
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="weekly_items")
    on_shelf = models.PositiveIntegerField(default=1)
    monthly_needed = models.PositiveIntegerField(default=0)
    system_stock = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    # Optional: supplier order code for ordering/reference
    order_code = models.CharField(max_length=64, blank=True)
    added_at = models.DateTimeField(auto_now_add=True)
    # Admin-only planning fields
    transfer_from = models.ForeignKey(
        Store,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="transfer_items",
    )
    transfer_bottles = models.PositiveIntegerField(default=0)
    joe = models.PositiveIntegerField(default=0)
    bt = models.PositiveIntegerField(default=0)
    sqw = models.PositiveIntegerField(default=0)

    class Meta:
        unique_together = ("order_list", "product")
        ordering = ["product__name"]

    def __str__(self) -> str:
        return f"{self.product.name} ({self.order_list})"


class MonthlySales(models.Model):
    """Cached monthly sales data for products at stores (30-day lookback)."""

    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="monthly_sales")
    store = models.ForeignKey(Store, on_delete=models.CASCADE, related_name="monthly_sales")
    quantity_sold = models.PositiveIntegerField(default=0, help_text="Units sold in last 30 days")
    days_calculated = models.PositiveIntegerField(default=30, help_text="Days used for calculation")
    calculated_at = models.DateTimeField(auto_now=True, db_index=True)

    class Meta:
        unique_together = ("product", "store")
        ordering = ["-calculated_at"]
        indexes = [
            models.Index(fields=["product", "store", "-calculated_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.product.number} @ {self.store.number}: {self.quantity_sold}/month"

    @property
    def is_stale(self) -> bool:
        """Check if data is older than 30 minutes."""
        from django.utils import timezone
        from datetime import timedelta
        return timezone.now() - self.calculated_at > timedelta(minutes=30)
