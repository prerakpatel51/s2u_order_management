from django.urls import path

from . import views

app_name = "inventory"

urlpatterns = [
    path("", views.home, name="home"),
    path("inventory/", views.product_search, name="product_search"),
    path("api/search/", views.product_search_api, name="product_search_api"),
    path("api/stock/", views.product_stock_api, name="product_stock_api"),
    path("api/products/refresh/", views.product_refresh_api, name="product_refresh_api"),
    path("api/monthly-sales/", views.monthly_sales_api, name="monthly_sales_api"),
    path("api/monthly-sales/bulk/", views.monthly_sales_bulk_api, name="monthly_sales_bulk_api"),
    # Global refresh orchestration (async)
    path("api/refresh/start/", views.refresh_all_start_api, name="refresh_all_start"),
    path("api/refresh/status/", views.refresh_all_status_api, name="refresh_all_status"),
    # Public About page
    path("about/", views.about, name="about"),
    # Weekly list URLs
    path("weekly/create/", views.weekly_list_create, name="weekly_list_create"),
    path("weekly/<int:list_id>/", views.weekly_list_detail, name="weekly_list_detail"),
    path("weekly/<int:list_id>/finalize/", views.weekly_finalize_list, name="weekly_finalize_list"),
    path("weekly/<int:list_id>/unfinalize/", views.weekly_unfinalize_list, name="weekly_unfinalize_list"),
    path("weekly/<int:list_id>/delete/", views.weekly_delete_list, name="weekly_delete_list"),
    path("weekly/<int:list_id>/search/", views.weekly_search_api, name="weekly_search"),
    path("weekly/<int:list_id>/add/", views.weekly_add_item_api, name="weekly_add_item"),
    path("weekly/<int:list_id>/item/<int:item_id>/", views.weekly_update_item_api, name="weekly_update_item"),
    path("weekly/<int:list_id>/item/<int:item_id>/delete/", views.weekly_delete_item_api, name="weekly_delete_item"),
    path("weekly/<int:list_id>/export/excel/", views.weekly_export_excel, name="weekly_export_excel"),
    path("weekly/<int:list_id>/export/pdf/", views.weekly_export_pdf, name="weekly_export_pdf"),
    path("weekly/<int:list_id>/export/excel/custom/", views.weekly_export_excel_custom, name="weekly_export_excel_custom"),
    path("weekly/<int:list_id>/export/pdf/custom/", views.weekly_export_pdf_custom, name="weekly_export_pdf_custom"),
    path("weekly/<int:list_id>/export/custom/", views.weekly_export_custom, name="weekly_export_custom"),
    # Admin user management
    path("admin/users/", views.user_manage, name="user_manage"),
]
