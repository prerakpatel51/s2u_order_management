"""
URL configuration for s2u_project project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import include, path
from django.contrib.auth import views as auth_views
from django_ratelimit.decorators import ratelimit
from inventory import views as inventory_views

# Rate-limited login view (5 attempts per minute per IP)
@ratelimit(key='ip', rate='5/m', method='POST', block=True)
def rate_limited_login(request, *args, **kwargs):
    """Render the Django login view with rate limiting applied.

    The decorator blocks clients that exceed 5 POSTs per minute per IP.

    Example:
        # Using curl to submit login credentials
        curl -X POST \
             -F "username=jane" -F "password=secret" \
             http://localhost:8000/accounts/login/
    """
    return auth_views.LoginView.as_view(template_name="inventory/login.html")(request, *args, **kwargs)

urlpatterns = [
    path("accounts/login/", rate_limited_login, name="login"),
    path("accounts/logout/", inventory_views.logout_view, name="logout"),
    path("", include("inventory.urls", namespace="inventory")),
    path("admin/", admin.site.urls),
]
