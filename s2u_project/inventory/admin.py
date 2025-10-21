from django.contrib import admin

from .models import Product, ProductStock, Store, WeeklyOrderItem, WeeklyOrderList, MonthlySales


@admin.register(Store)
class StoreAdmin(admin.ModelAdmin):
    list_display = ("name", "number", "city", "state", "active", "warehouse")
    search_fields = ("name", "number", "city", "state")
    list_filter = ("active", "warehouse")


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = ("number", "name", "barcode", "supplier_name")
    search_fields = ("number", "name", "barcode", "supplier_name")
    list_filter = ("supplier_name",)


@admin.register(ProductStock)
class ProductStockAdmin(admin.ModelAdmin):
    list_display = ("product", "store", "actual", "ordered", "listed", "updated_at")
    search_fields = ("product__name", "product__number", "store__name", "store__number")
    list_filter = ("store", "listed")


class WeeklyOrderItemInline(admin.TabularInline):
    model = WeeklyOrderItem
    extra = 0


@admin.register(WeeklyOrderList)
class WeeklyOrderListAdmin(admin.ModelAdmin):
    list_display = ("store", "target_date", "created_at", "finalized_at", "finalized_by")
    list_filter = ("store", "target_date")
    search_fields = ("store__name", "store__number")
    inlines = [WeeklyOrderItemInline]


@admin.register(WeeklyOrderItem)
class WeeklyOrderItemAdmin(admin.ModelAdmin):
    list_display = (
        "order_list",
        "product",
        "on_shelf",
        "monthly_needed",
        "system_stock",
        "transfer_from",
        "transfer_bottles",
        "joe",
        "bt",
        "sqw",
        "added_at",
    )
    list_filter = ("order_list__store",)
    search_fields = ("product__name", "product__number", "order_list__store__name")


@admin.register(MonthlySales)
class MonthlySalesAdmin(admin.ModelAdmin):
    list_display = ("product", "store", "quantity_sold", "days_calculated", "calculated_at")
    list_filter = ("store", "calculated_at")
    search_fields = ("product__name", "product__number", "store__name", "store__number")
    readonly_fields = ("calculated_at",)
