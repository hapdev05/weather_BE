"""
Setup PostgreSQL Data Warehouse for Weather Prediction.

Star Schema Design:
  - dim_district: District dimension
  - dim_date: Date dimension (year, month, day, day_of_year, ...)
  - dim_hour: Hour dimension (hour, period_of_day)
  - fact_weather_hourly: Fact table with actual weather measurements
  - fact_weather_forecast: Forecast results stored back into DW

Feature Store (schema: features):
  - agg_daily_weather: Daily aggregated stats per district
  - agg_historical_same_day: Same month-day stats across previous years
  - mv_training_features: Materialized view combining all features for ML
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text
from config import DB_CONFIG, DATABASE_URL, ADMIN_DATABASE_URL, SCHEMA_DW, SCHEMA_STAGING, SCHEMA_FEATURES


def create_database():
    print("Creating database 'weather_dw'...")
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        dbname="postgres",
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'weather_dw'")
    if cur.fetchone():
        print("  Database already exists, dropping and recreating...")
        cur.execute("DROP DATABASE weather_dw")

    cur.execute("CREATE DATABASE weather_dw")
    print("  Database created.")
    cur.close()
    conn.close()


def create_schemas(engine):
    print("Creating schemas...")
    with engine.begin() as conn:
        for schema in [SCHEMA_DW, SCHEMA_STAGING, SCHEMA_FEATURES]:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            print(f"  Schema '{schema}' created.")


def create_dimension_tables(engine):
    print("Creating dimension tables...")
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_DW}.dim_district (
                district_id   SERIAL PRIMARY KEY,
                district_name VARCHAR(100) NOT NULL UNIQUE
            );
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_DW}.dim_date (
                date_id       SERIAL PRIMARY KEY,
                full_date     DATE NOT NULL UNIQUE,
                year          SMALLINT NOT NULL,
                month         SMALLINT NOT NULL,
                day           SMALLINT NOT NULL,
                day_of_year   SMALLINT NOT NULL,
                day_of_week   SMALLINT NOT NULL,
                week_of_year  SMALLINT NOT NULL,
                quarter       SMALLINT NOT NULL,
                month_name    VARCHAR(20) NOT NULL,
                is_weekend    BOOLEAN NOT NULL,
                month_day     SMALLINT NOT NULL  -- month*100+day for same-day matching
            );
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_DW}.dim_hour (
                hour_id        SMALLINT PRIMARY KEY,
                hour           SMALLINT NOT NULL,
                period_of_day  VARCHAR(20) NOT NULL
            );
        """))

    print("  Dimension tables created.")


def create_fact_table(engine):
    print("Creating fact table...")
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_DW}.fact_weather_hourly (
                weather_id      SERIAL PRIMARY KEY,
                district_id     INT NOT NULL REFERENCES {SCHEMA_DW}.dim_district(district_id),
                date_id         INT NOT NULL REFERENCES {SCHEMA_DW}.dim_date(date_id),
                hour_id         SMALLINT NOT NULL REFERENCES {SCHEMA_DW}.dim_hour(hour_id),
                temperature_c       REAL,
                humidity_percent    REAL,
                wind_speed_m_s      REAL,
                rain_mm             REAL
            );
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_fact_district
                ON {SCHEMA_DW}.fact_weather_hourly(district_id);
            CREATE INDEX IF NOT EXISTS idx_fact_date
                ON {SCHEMA_DW}.fact_weather_hourly(date_id);
            CREATE INDEX IF NOT EXISTS idx_fact_district_date
                ON {SCHEMA_DW}.fact_weather_hourly(district_id, date_id);
        """))

    print("  Fact table created with indexes.")


def create_forecast_table(engine):
    print("Creating forecast table...")
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_DW}.fact_weather_forecast (
                forecast_id     SERIAL PRIMARY KEY,
                district_id     INT NOT NULL REFERENCES {SCHEMA_DW}.dim_district(district_id),
                date_id         INT NOT NULL,
                hour_id         SMALLINT NOT NULL REFERENCES {SCHEMA_DW}.dim_hour(hour_id),
                temperature_c       REAL,
                humidity_percent    REAL,
                wind_speed_m_s      REAL,
                rain_mm             REAL,
                rain_probability    REAL,
                predicted_at        TIMESTAMP DEFAULT NOW(),
                UNIQUE (district_id, date_id, hour_id)
            );
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_forecast_district
                ON {SCHEMA_DW}.fact_weather_forecast(district_id);
            CREATE INDEX IF NOT EXISTS idx_forecast_date
                ON {SCHEMA_DW}.fact_weather_forecast(date_id);
            CREATE INDEX IF NOT EXISTS idx_forecast_district_date
                ON {SCHEMA_DW}.fact_weather_forecast(district_id, date_id);
        """))

    print("  Forecast table created with indexes.")


def create_staging_table(engine):
    print("Creating staging table...")
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_STAGING}.raw_weather (
                district        VARCHAR(100),
                datetime        TIMESTAMP,
                temperature_c       REAL,
                humidity_percent    REAL,
                wind_speed_m_s      REAL,
                rain_mm             REAL
            );
        """))
    print("  Staging table created.")


def create_feature_tables(engine):
    print("Creating feature store tables...")
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_FEATURES}.agg_daily_weather (
                district_id     INT NOT NULL,
                date_id         INT NOT NULL,
                year            SMALLINT,
                month_day       SMALLINT,
                temp_mean       REAL, temp_std  REAL, temp_min  REAL, temp_max  REAL,
                hum_mean        REAL, hum_std   REAL, hum_min   REAL, hum_max   REAL,
                wind_mean       REAL, wind_std  REAL, wind_min  REAL, wind_max  REAL,
                rain_mean       REAL, rain_std  REAL, rain_min  REAL, rain_max  REAL,
                rain_total      REAL,
                PRIMARY KEY (district_id, date_id)
            );
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_FEATURES}.agg_historical_same_day (
                district_id     INT NOT NULL,
                year            SMALLINT NOT NULL,
                month_day       SMALLINT NOT NULL,
                hist_temp_mean  REAL, hist_temp_std  REAL, hist_temp_min  REAL, hist_temp_max  REAL,
                hist_hum_mean   REAL, hist_hum_std   REAL, hist_hum_min   REAL, hist_hum_max   REAL,
                hist_wind_mean  REAL, hist_wind_std  REAL, hist_wind_min  REAL, hist_wind_max  REAL,
                hist_rain_mean  REAL, hist_rain_std  REAL, hist_rain_min  REAL, hist_rain_max  REAL,
                years_count     SMALLINT,
                PRIMARY KEY (district_id, year, month_day)
            );
        """))

    print("  Feature tables created.")


def populate_hour_dimension(engine):
    print("Populating dim_hour...")
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {SCHEMA_DW}.dim_hour"))
        for h in range(24):
            if h < 6:
                period = "Night"
            elif h < 12:
                period = "Morning"
            elif h < 18:
                period = "Afternoon"
            else:
                period = "Evening"
            conn.execute(text(f"""
                INSERT INTO {SCHEMA_DW}.dim_hour (hour_id, hour, period_of_day)
                VALUES (:hid, :h, :period)
            """), {"hid": h, "h": h, "period": period})
    print("  dim_hour populated (24 rows).")


def setup():
    create_database()
    engine = create_engine(DATABASE_URL)
    create_schemas(engine)
    create_dimension_tables(engine)
    create_fact_table(engine)
    create_forecast_table(engine)
    create_staging_table(engine)
    create_feature_tables(engine)
    populate_hour_dimension(engine)
    engine.dispose()
    print("\nDatabase setup complete!")


if __name__ == "__main__":
    setup()
