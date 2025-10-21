"""
Django settings for s2u_project.
Consolidated settings that adapt based on DJANGO_ENV environment variable.
"""

from pathlib import Path
import os

# Load .env file if it exists (for local development)
from dotenv import load_dotenv

# Load .env from project root (one level up from s2u_project/)
env_path = Path(__file__).resolve().parent.parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
    print(f"✅ Loaded environment variables from {env_path}")
else:
    print(f"⚠️  No .env file found at {env_path}")

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Determine environment
DJANGO_ENV = os.environ.get("DJANGO_ENV", "dev")
IS_PRODUCTION = DJANGO_ENV == "production"

# SECURITY WARNING: keep the secret key used in production secret!
if IS_PRODUCTION:
    SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY")
    if not SECRET_KEY:
        raise ValueError("DJANGO_SECRET_KEY environment variable must be set in production")
else:
    # For dev, use stable key from env to avoid session corruption between restarts
    SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY")
    if not SECRET_KEY:
        # Only generate a random key if not in env (will cause session corruption on restart)
        import secrets
        SECRET_KEY = secrets.token_urlsafe(50)
        print("⚠️  WARNING: DJANGO_SECRET_KEY not set in .env - using generated key (sessions will be lost on restart)")

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = not IS_PRODUCTION

# Allowed hosts
if IS_PRODUCTION:
    ALLOWED_HOSTS = os.environ.get("DJANGO_ALLOWED_HOSTS", "").split(",")
    if not ALLOWED_HOSTS or ALLOWED_HOSTS == [""]:
        raise ValueError("DJANGO_ALLOWED_HOSTS environment variable must be set in production")
else:
    ALLOWED_HOSTS = ['*']

# CSRF trusted origins (for development with ngrok)
if not IS_PRODUCTION:
    CSRF_TRUSTED_ORIGINS = [
        "https://cb816ee7f588.ngrok-free.app",
    ]

# Application definition
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "inventory.apps.InventoryConfig",
]

# Middleware (CSP enabled only in production)
if IS_PRODUCTION:
    MIDDLEWARE = [
        "django.middleware.security.SecurityMiddleware",
        "whitenoise.middleware.WhiteNoiseMiddleware",  # Serve static files in production
        "csp.middleware.CSPMiddleware",
        "django.contrib.sessions.middleware.SessionMiddleware",
        "django.middleware.common.CommonMiddleware",
        "django.middleware.csrf.CsrfViewMiddleware",
        "django.contrib.auth.middleware.AuthenticationMiddleware",
        "django.contrib.messages.middleware.MessageMiddleware",
        "django.middleware.clickjacking.XFrameOptionsMiddleware",
    ]
else:
    MIDDLEWARE = [
        "django.middleware.security.SecurityMiddleware",
        "whitenoise.middleware.WhiteNoiseMiddleware",  # Serve static files in dev too
        "django.contrib.sessions.middleware.SessionMiddleware",
        "django.middleware.common.CommonMiddleware",
        "django.middleware.csrf.CsrfViewMiddleware",
        "django.contrib.auth.middleware.AuthenticationMiddleware",
        "django.contrib.messages.middleware.MessageMiddleware",
        "django.middleware.clickjacking.XFrameOptionsMiddleware",
    ]

ROOT_URLCONF = "s2u_project.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "s2u_project.wsgi.application"

# Database
# SQLite for local development
DB_PATH = BASE_DIR / "db.sqlite3"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": DB_PATH,
    }
}

# Note: PostgreSQL support is available if needed in the future
# Just uncomment this block and set DATABASE_URL in .env
# if IS_PRODUCTION and os.environ.get("DATABASE_URL"):
#     import dj_database_url
#     DATABASES = {
#         "default": dj_database_url.config(
#             default=os.environ["DATABASE_URL"],
#             conn_max_age=600,
#             conn_health_checks=True,
#         )
#     }

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

# Internationalization
LANGUAGE_CODE = "en-us"
TIME_ZONE = "America/New_York"
USE_I18N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = "static/"
STATIC_ROOT = os.path.join(BASE_DIR, "staticfiles")

if IS_PRODUCTION:
    STATICFILES_STORAGE = "django.contrib.staticfiles.storage.ManifestStaticFilesStorage"

# Media files
MEDIA_URL = "media/"
MEDIA_ROOT = os.path.join(BASE_DIR, "media")

# Default primary key field type
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Auth redirects
LOGIN_URL = "login"
LOGIN_REDIRECT_URL = "inventory:home"
LOGOUT_REDIRECT_URL = "login"

# Security settings (production only)
if IS_PRODUCTION:
    SECURE_SSL_REDIRECT = True
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    SECURE_BROWSER_XSS_FILTER = True
    SECURE_CONTENT_TYPE_NOSNIFF = True
    X_FRAME_OPTIONS = "DENY"
    SECURE_HSTS_SECONDS = 31536000  # 1 year
    SECURE_HSTS_INCLUDE_SUBDOMAINS = True
    SECURE_HSTS_PRELOAD = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_AGE = 3600  # 1 hour
    SESSION_SAVE_EVERY_REQUEST = True
    CSRF_COOKIE_HTTPONLY = True
    CSRF_USE_SESSIONS = True
    CSRF_COOKIE_SAMESITE = 'Strict'
    SESSION_COOKIE_SAMESITE = 'Strict'
    SECURE_REFERRER_POLICY = "strict-origin-when-cross-origin"
    SECURE_CROSS_ORIGIN_OPENER_POLICY = "same-origin"

    # Permissions Policy
    PERMISSIONS_POLICY = {
        "geolocation": [],
        "microphone": [],
        "camera": [],
        "payment": [],
        "usb": [],
        "magnetometer": [],
        "gyroscope": [],
        "accelerometer": [],
    }

    # Content Security Policy
    CSP_DEFAULT_SRC = ("'self'",)
    CSP_SCRIPT_SRC = ("'self'", "'unsafe-inline'", "https://cdn.jsdelivr.net")
    CSP_STYLE_SRC = ("'self'", "'unsafe-inline'", "https://cdn.jsdelivr.net")
    CSP_IMG_SRC = ("'self'", "data:", "https:")
    CSP_FONT_SRC = ("'self'", "data:", "https://cdn.jsdelivr.net")
    CSP_CONNECT_SRC = ("'self'",)
    CSP_FRAME_ANCESTORS = ("'none'",)
    CSP_BASE_URI = ("'self'",)
    CSP_FORM_ACTION = ("'self'",)
    CSP_UPGRADE_INSECURE_REQUESTS = True
else:
    INTERNAL_IPS = ["127.0.0.1"]

# No Django cache configuration — app uses Redis directly (see inventory/redis_client.py)

# Email configuration
if IS_PRODUCTION:
    ADMINS = [
        ("Admin", os.environ.get("ADMIN_EMAIL", "admin@example.com")),
    ]
    EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
    EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
    EMAIL_PORT = int(os.environ.get("EMAIL_PORT", "587"))
    EMAIL_USE_TLS = True
    EMAIL_HOST_USER = os.environ.get("EMAIL_HOST_USER")
    EMAIL_HOST_PASSWORD = os.environ.get("EMAIL_HOST_PASSWORD")
    SERVER_EMAIL = os.environ.get("SERVER_EMAIL", "noreply@example.com")
else:
    EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"

# Logging configuration
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "{levelname} {asctime} {module} {message}",
            "style": "{",
        },
        "simple": {
            "format": "{levelname} {message}",
            "style": "{",
        },
    },
    "filters": {
        "require_debug_true": {
            "()": "django.utils.log.RequireDebugTrue",
        },
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "simple",
        },
        "file": {
            "level": "INFO",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(BASE_DIR, "logs", "django.log"),
            "maxBytes": 1024 * 1024 * 10,  # 10MB
            "backupCount": 5,
            "formatter": "verbose",
        },
    },
    "root": {
        "handlers": ["console", "file"],
        "level": "INFO",
    },
    "loggers": {
        "django": {
            "handlers": ["console", "file"],
            "level": os.getenv("DJANGO_LOG_LEVEL", "INFO"),
            "propagate": False,
        },
        "inventory": {
            "handlers": ["console", "file"],
            "level": "INFO",
            "propagate": False,
        },
    },
}

# In development, remove file handler entirely and use console only
if not IS_PRODUCTION:
    LOGGING["handlers"].pop("file", None)
    LOGGING["root"]["handlers"] = ["console"]
    LOGGING["loggers"]["django"]["handlers"] = ["console"]
    LOGGING["loggers"]["inventory"]["handlers"] = ["console"]

# Add email handler in production
if IS_PRODUCTION:
    # Ensure log directory exists when using file handler in production
    os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)
    LOGGING["handlers"]["mail_admins"] = {
        "level": "ERROR",
        "class": "django.utils.log.AdminEmailHandler",
        "include_html": True,
    }
    LOGGING["loggers"]["django"]["handlers"].append("mail_admins")
else:
    # In development, already removed file handler above
    pass
