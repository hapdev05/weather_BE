import os

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": os.environ.get("USER", "hapdev"),
    "password": "",
    "dbname": "weather_dw",
}

DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

ADMIN_DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/postgres"
)

DATASET_PATH = os.path.join(os.path.dirname(__file__), "..", "dataset_weather.csv")
MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")

TARGETS = ["temperature_c", "humidity_percent", "wind_speed_m_s", "rain_mm"]
NEARBY_DAYS = 7

SCHEMA_DW = "dw"
SCHEMA_STAGING = "staging"
SCHEMA_FEATURES = "features"
