import os

# ==============================================================================
# APACHE SUPERSET CONFIGURATION (Camada 5 + Redis Cache - Slide 18)
# ==============================================================================
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "lakehouse_opensource_superset_secret_key_2026")

# Metadados no PostgreSQL
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{os.environ.get('POSTGRES_USER', 'lakehouse')}:"
    f"{os.environ.get('POSTGRES_PASSWORD', 'lakehouse123')}@postgres:5432/superset_db"
)

# Cache Sincronizado no Redis (Slide 18: Zero Recálculo Desnecessário)
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_meta_",
    "CACHE_REDIS_HOST": "redis",
    "CACHE_REDIS_PORT": 6379,
    "CACHE_REDIS_DB": 1,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 600,
    "CACHE_KEY_PREFIX": "superset_results_",
    "CACHE_REDIS_HOST": "redis",
    "CACHE_REDIS_PORT": 6379,
    "CACHE_REDIS_DB": 2,
}

FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 600,
    "CACHE_KEY_PREFIX": "superset_filter_",
    "CACHE_REDIS_HOST": "redis",
    "CACHE_REDIS_PORT": 6379,
    "CACHE_REDIS_DB": 3,
}

EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 600,
    "CACHE_KEY_PREFIX": "superset_explore_",
    "CACHE_REDIS_HOST": "redis",
    "CACHE_REDIS_PORT": 6379,
    "CACHE_REDIS_DB": 4,
}

WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
PREVENT_UNSAFE_DB_CONNECTIONS = False
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}
