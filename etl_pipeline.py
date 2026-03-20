"""
ETL Pipeline: CSV -> Staging -> Transform -> Data Warehouse.

Step 1 (Extract):  Load raw CSV into staging.raw_weather
Step 2 (Transform): Populate dim_district, dim_date from staging data
Step 3 (Load):      Join dimensions and insert into fact_weather_hourly
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from config import DATABASE_URL, DATASET_PATH, SCHEMA_DW, SCHEMA_STAGING
import time


def extract_to_staging(engine, csv_path: str):
    print("\n[EXTRACT] Loading CSV into staging...")
    t0 = time.time()

    df = pd.read_csv(csv_path)
    df.columns = ["district", "datetime", "temperature_c", "humidity_percent", "wind_speed_m_s", "rain_mm"]
    df["datetime"] = pd.to_datetime(df["datetime"])

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA_STAGING}.raw_weather"))

    df.to_sql(
        "raw_weather",
        engine,
        schema=SCHEMA_STAGING,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10000,
    )

    print(f"  Loaded {len(df):,} rows in {time.time() - t0:.1f}s")
    return df


def transform_dim_district(engine):
    print("\n[TRANSFORM] Populating dim_district...")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA_DW}.dim_district CASCADE"))
        conn.execute(text(f"""
            INSERT INTO {SCHEMA_DW}.dim_district (district_name)
            SELECT DISTINCT district
            FROM {SCHEMA_STAGING}.raw_weather
            ORDER BY district
        """))
        result = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_DW}.dim_district"))
        print(f"  {result.scalar()} districts loaded.")


def transform_dim_date(engine):
    print("\n[TRANSFORM] Populating dim_date...")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA_DW}.dim_date CASCADE"))
        conn.execute(text(f"""
            INSERT INTO {SCHEMA_DW}.dim_date
                (full_date, year, month, day, day_of_year, day_of_week,
                 week_of_year, quarter, month_name, is_weekend, month_day)
            SELECT DISTINCT
                d::date                                         AS full_date,
                EXTRACT(YEAR FROM d)::smallint                  AS year,
                EXTRACT(MONTH FROM d)::smallint                 AS month,
                EXTRACT(DAY FROM d)::smallint                   AS day,
                EXTRACT(DOY FROM d)::smallint                   AS day_of_year,
                EXTRACT(ISODOW FROM d)::smallint                AS day_of_week,
                EXTRACT(WEEK FROM d)::smallint                  AS week_of_year,
                EXTRACT(QUARTER FROM d)::smallint               AS quarter,
                TO_CHAR(d, 'Month')                             AS month_name,
                EXTRACT(ISODOW FROM d) IN (6, 7)               AS is_weekend,
                (EXTRACT(MONTH FROM d)*100 + EXTRACT(DAY FROM d))::smallint AS month_day
            FROM (
                SELECT datetime::date AS d
                FROM {SCHEMA_STAGING}.raw_weather
            ) sub
            ORDER BY full_date
        """))
        result = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_DW}.dim_date"))
        print(f"  {result.scalar()} dates loaded.")


def load_fact_table(engine):
    print("\n[LOAD] Populating fact_weather_hourly...")
    t0 = time.time()

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA_DW}.fact_weather_hourly"))
        conn.execute(text(f"""
            INSERT INTO {SCHEMA_DW}.fact_weather_hourly
                (district_id, date_id, hour_id,
                 temperature_c, humidity_percent, wind_speed_m_s, rain_mm)
            SELECT
                dd.district_id,
                dt.date_id,
                EXTRACT(HOUR FROM rw.datetime)::smallint AS hour_id,
                rw.temperature_c,
                rw.humidity_percent,
                rw.wind_speed_m_s,
                rw.rain_mm
            FROM {SCHEMA_STAGING}.raw_weather rw
            JOIN {SCHEMA_DW}.dim_district dd ON dd.district_name = rw.district
            JOIN {SCHEMA_DW}.dim_date dt ON dt.full_date = rw.datetime::date
        """))

        result = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_DW}.fact_weather_hourly"))
        print(f"  {result.scalar():,} rows loaded in {time.time() - t0:.1f}s")


def run_etl(csv_path: str = None):
    if csv_path is None:
        csv_path = DATASET_PATH

    engine = create_engine(DATABASE_URL)
    extract_to_staging(engine, csv_path)
    transform_dim_district(engine)
    transform_dim_date(engine)
    load_fact_table(engine)
    engine.dispose()
    print("\nETL pipeline complete!")


if __name__ == "__main__":
    run_etl()
