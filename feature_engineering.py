"""
Feature Engineering using PostgreSQL.

V2 improvements:
  - district_id as feature (location-aware)
  - Historical same-day PER HOUR (not just per day) for precise hourly patterns
  - Hourly historical stats table for same month_day + hour across years
  - is_rainy flags in lag features for two-stage rain model
"""

from sqlalchemy import create_engine, text
from config import DATABASE_URL, SCHEMA_DW, SCHEMA_FEATURES, NEARBY_DAYS
import time


def create_extra_tables(engine):
    """Create the hourly historical table if not exists."""
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_FEATURES}.agg_historical_same_day_hour (
                district_id     INT NOT NULL,
                year            SMALLINT NOT NULL,
                month_day       SMALLINT NOT NULL,
                hour            SMALLINT NOT NULL,
                hist_h_temp_mean  REAL, hist_h_temp_std  REAL,
                hist_h_hum_mean   REAL, hist_h_hum_std   REAL,
                hist_h_wind_mean  REAL, hist_h_wind_std  REAL,
                hist_h_rain_mean  REAL, hist_h_rain_max  REAL,
                hist_h_rain_prob  REAL,
                years_count       SMALLINT,
                PRIMARY KEY (district_id, year, month_day, hour)
            );
        """))


def compute_daily_aggregates(engine):
    print("\n[FEATURES] Computing daily aggregates...")
    t0 = time.time()

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA_FEATURES}.agg_daily_weather"))
        conn.execute(text(f"""
            INSERT INTO {SCHEMA_FEATURES}.agg_daily_weather
                (district_id, date_id, year, month_day,
                 temp_mean, temp_std, temp_min, temp_max,
                 hum_mean, hum_std, hum_min, hum_max,
                 wind_mean, wind_std, wind_min, wind_max,
                 rain_mean, rain_std, rain_min, rain_max,
                 rain_total)
            SELECT
                f.district_id, f.date_id, d.year, d.month_day,
                AVG(f.temperature_c),  STDDEV(f.temperature_c),
                MIN(f.temperature_c),  MAX(f.temperature_c),
                AVG(f.humidity_percent),  STDDEV(f.humidity_percent),
                MIN(f.humidity_percent),  MAX(f.humidity_percent),
                AVG(f.wind_speed_m_s),  STDDEV(f.wind_speed_m_s),
                MIN(f.wind_speed_m_s),  MAX(f.wind_speed_m_s),
                AVG(f.rain_mm),  STDDEV(f.rain_mm),
                MIN(f.rain_mm),  MAX(f.rain_mm),
                SUM(f.rain_mm)
            FROM {SCHEMA_DW}.fact_weather_hourly f
            JOIN {SCHEMA_DW}.dim_date d ON d.date_id = f.date_id
            GROUP BY f.district_id, f.date_id, d.year, d.month_day
        """))

        result = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_FEATURES}.agg_daily_weather"))
        print(f"  {result.scalar():,} daily aggregates in {time.time() - t0:.1f}s")


def compute_historical_same_day(engine):
    print("\n[FEATURES] Computing historical same-day features (daily)...")
    t0 = time.time()

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA_FEATURES}.agg_historical_same_day"))
        conn.execute(text(f"""
            INSERT INTO {SCHEMA_FEATURES}.agg_historical_same_day
                (district_id, year, month_day,
                 hist_temp_mean, hist_temp_std, hist_temp_min, hist_temp_max,
                 hist_hum_mean, hist_hum_std, hist_hum_min, hist_hum_max,
                 hist_wind_mean, hist_wind_std, hist_wind_min, hist_wind_max,
                 hist_rain_mean, hist_rain_std, hist_rain_min, hist_rain_max,
                 years_count)
            SELECT
                cur.district_id, cur.year, cur.month_day,
                AVG(prev.temp_mean),  AVG(prev.temp_std),
                MIN(prev.temp_min),   MAX(prev.temp_max),
                AVG(prev.hum_mean),   AVG(prev.hum_std),
                MIN(prev.hum_min),    MAX(prev.hum_max),
                AVG(prev.wind_mean),  AVG(prev.wind_std),
                MIN(prev.wind_min),   MAX(prev.wind_max),
                AVG(prev.rain_mean),  AVG(prev.rain_std),
                MIN(prev.rain_min),   MAX(prev.rain_max),
                COUNT(DISTINCT prev.year)::smallint
            FROM {SCHEMA_FEATURES}.agg_daily_weather cur
            JOIN {SCHEMA_FEATURES}.agg_daily_weather prev
                ON prev.district_id = cur.district_id
                AND prev.month_day = cur.month_day
                AND prev.year < cur.year
            GROUP BY cur.district_id, cur.year, cur.month_day
        """))

        result = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_FEATURES}.agg_historical_same_day"))
        print(f"  {result.scalar():,} daily historical records in {time.time() - t0:.1f}s")


def compute_historical_same_day_hour(engine):
    """Historical stats for exact same month_day + hour across previous years."""
    print("\n[FEATURES] Computing historical same-day-hour features...")
    t0 = time.time()

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA_FEATURES}.agg_historical_same_day_hour"))
        conn.execute(text(f"""
            INSERT INTO {SCHEMA_FEATURES}.agg_historical_same_day_hour
                (district_id, year, month_day, hour,
                 hist_h_temp_mean, hist_h_temp_std,
                 hist_h_hum_mean, hist_h_hum_std,
                 hist_h_wind_mean, hist_h_wind_std,
                 hist_h_rain_mean, hist_h_rain_max, hist_h_rain_prob,
                 years_count)
            SELECT
                cur.district_id,
                cur_d.year,
                cur_d.month_day,
                cur.hour_id AS hour,
                AVG(prev.temperature_c),
                STDDEV(prev.temperature_c),
                AVG(prev.humidity_percent),
                STDDEV(prev.humidity_percent),
                AVG(prev.wind_speed_m_s),
                STDDEV(prev.wind_speed_m_s),
                AVG(prev.rain_mm),
                MAX(prev.rain_mm),
                AVG(CASE WHEN prev.rain_mm > 0.1 THEN 1.0 ELSE 0.0 END),
                COUNT(DISTINCT prev_d.year)::smallint
            FROM {SCHEMA_DW}.fact_weather_hourly cur
            JOIN {SCHEMA_DW}.dim_date cur_d ON cur_d.date_id = cur.date_id
            JOIN {SCHEMA_DW}.dim_date prev_d
                ON prev_d.month_day = cur_d.month_day
                AND prev_d.year < cur_d.year
            JOIN {SCHEMA_DW}.fact_weather_hourly prev
                ON prev.district_id = cur.district_id
                AND prev.date_id = prev_d.date_id
                AND prev.hour_id = cur.hour_id
            GROUP BY cur.district_id, cur_d.year, cur_d.month_day, cur.hour_id
        """))

        result = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_FEATURES}.agg_historical_same_day_hour"))
        print(f"  {result.scalar():,} hourly historical records in {time.time() - t0:.1f}s")


TRAINING_QUERY = f"""
SELECT
    f.weather_id,
    dd.district_name,
    f.district_id,
    dt.full_date,
    dt.year,
    dt.month,
    dt.day,
    dt.day_of_year,
    dt.day_of_week,
    dt.is_weekend,
    dt.month_day,
    dh.hour,
    dh.hour_id,

    SIN(2 * PI() * dh.hour / 24.0)           AS hour_sin,
    COS(2 * PI() * dh.hour / 24.0)           AS hour_cos,
    SIN(2 * PI() * dt.day_of_year / 365.25)  AS doy_sin,
    COS(2 * PI() * dt.day_of_year / 365.25)  AS doy_cos,
    SIN(2 * PI() * dt.month / 12.0)          AS month_sin,
    COS(2 * PI() * dt.month / 12.0)          AS month_cos,

    -- Historical same-day (daily level)
    h.hist_temp_mean, h.hist_temp_std, h.hist_temp_min, h.hist_temp_max,
    h.hist_hum_mean,  h.hist_hum_std,  h.hist_hum_min,  h.hist_hum_max,
    h.hist_wind_mean, h.hist_wind_std, h.hist_wind_min, h.hist_wind_max,
    h.hist_rain_mean, h.hist_rain_std, h.hist_rain_min, h.hist_rain_max,
    h.years_count,

    -- Historical same-day-hour (hourly level - precise)
    hh.hist_h_temp_mean, hh.hist_h_temp_std,
    hh.hist_h_hum_mean,  hh.hist_h_hum_std,
    hh.hist_h_wind_mean, hh.hist_h_wind_std,
    hh.hist_h_rain_mean, hh.hist_h_rain_max, hh.hist_h_rain_prob,

    -- Lag features (same district, same hour, previous days)
    lag1.temperature_c  AS lag_1d_temp,   lag1.humidity_percent AS lag_1d_hum,
    lag1.wind_speed_m_s AS lag_1d_wind,   lag1.rain_mm          AS lag_1d_rain,
    lag2.temperature_c  AS lag_2d_temp,   lag2.humidity_percent AS lag_2d_hum,
    lag2.wind_speed_m_s AS lag_2d_wind,   lag2.rain_mm          AS lag_2d_rain,
    lag3.temperature_c  AS lag_3d_temp,   lag3.humidity_percent AS lag_3d_hum,
    lag3.wind_speed_m_s AS lag_3d_wind,   lag3.rain_mm          AS lag_3d_rain,
    lag4.temperature_c  AS lag_4d_temp,   lag4.humidity_percent AS lag_4d_hum,
    lag4.wind_speed_m_s AS lag_4d_wind,   lag4.rain_mm          AS lag_4d_rain,
    lag5.temperature_c  AS lag_5d_temp,   lag5.humidity_percent AS lag_5d_hum,
    lag5.wind_speed_m_s AS lag_5d_wind,   lag5.rain_mm          AS lag_5d_rain,
    lag6.temperature_c  AS lag_6d_temp,   lag6.humidity_percent AS lag_6d_hum,
    lag6.wind_speed_m_s AS lag_6d_wind,   lag6.rain_mm          AS lag_6d_rain,
    lag7.temperature_c  AS lag_7d_temp,   lag7.humidity_percent AS lag_7d_hum,
    lag7.wind_speed_m_s AS lag_7d_wind,   lag7.rain_mm          AS lag_7d_rain,

    -- Rain flags for nearby days (for two-stage rain model)
    CASE WHEN lag1.rain_mm > 0.1 THEN 1 ELSE 0 END AS lag_1d_is_rainy,
    CASE WHEN lag2.rain_mm > 0.1 THEN 1 ELSE 0 END AS lag_2d_is_rainy,
    CASE WHEN lag3.rain_mm > 0.1 THEN 1 ELSE 0 END AS lag_3d_is_rainy,

    -- Targets
    f.temperature_c,
    f.humidity_percent,
    f.wind_speed_m_s,
    f.rain_mm,
    CASE WHEN f.rain_mm > 0.1 THEN 1 ELSE 0 END AS is_rainy

FROM {SCHEMA_DW}.fact_weather_hourly f
JOIN {SCHEMA_DW}.dim_district dd ON dd.district_id = f.district_id
JOIN {SCHEMA_DW}.dim_date dt     ON dt.date_id = f.date_id
JOIN {SCHEMA_DW}.dim_hour dh     ON dh.hour_id = f.hour_id

LEFT JOIN {SCHEMA_FEATURES}.agg_historical_same_day h
    ON h.district_id = f.district_id
    AND h.year = dt.year
    AND h.month_day = dt.month_day

LEFT JOIN {SCHEMA_FEATURES}.agg_historical_same_day_hour hh
    ON hh.district_id = f.district_id
    AND hh.year = dt.year
    AND hh.month_day = dt.month_day
    AND hh.hour = dh.hour_id

LEFT JOIN {SCHEMA_DW}.fact_weather_hourly lag1
    ON lag1.district_id = f.district_id AND lag1.hour_id = f.hour_id
    AND lag1.date_id = (SELECT date_id FROM {SCHEMA_DW}.dim_date WHERE full_date = dt.full_date - INTERVAL '1 day')
LEFT JOIN {SCHEMA_DW}.fact_weather_hourly lag2
    ON lag2.district_id = f.district_id AND lag2.hour_id = f.hour_id
    AND lag2.date_id = (SELECT date_id FROM {SCHEMA_DW}.dim_date WHERE full_date = dt.full_date - INTERVAL '2 days')
LEFT JOIN {SCHEMA_DW}.fact_weather_hourly lag3
    ON lag3.district_id = f.district_id AND lag3.hour_id = f.hour_id
    AND lag3.date_id = (SELECT date_id FROM {SCHEMA_DW}.dim_date WHERE full_date = dt.full_date - INTERVAL '3 days')
LEFT JOIN {SCHEMA_DW}.fact_weather_hourly lag4
    ON lag4.district_id = f.district_id AND lag4.hour_id = f.hour_id
    AND lag4.date_id = (SELECT date_id FROM {SCHEMA_DW}.dim_date WHERE full_date = dt.full_date - INTERVAL '4 days')
LEFT JOIN {SCHEMA_DW}.fact_weather_hourly lag5
    ON lag5.district_id = f.district_id AND lag5.hour_id = f.hour_id
    AND lag5.date_id = (SELECT date_id FROM {SCHEMA_DW}.dim_date WHERE full_date = dt.full_date - INTERVAL '5 days')
LEFT JOIN {SCHEMA_DW}.fact_weather_hourly lag6
    ON lag6.district_id = f.district_id AND lag6.hour_id = f.hour_id
    AND lag6.date_id = (SELECT date_id FROM {SCHEMA_DW}.dim_date WHERE full_date = dt.full_date - INTERVAL '6 days')
LEFT JOIN {SCHEMA_DW}.fact_weather_hourly lag7
    ON lag7.district_id = f.district_id AND lag7.hour_id = f.hour_id
    AND lag7.date_id = (SELECT date_id FROM {SCHEMA_DW}.dim_date WHERE full_date = dt.full_date - INTERVAL '7 days')

ORDER BY dd.district_name, dt.full_date, dh.hour
"""


def create_materialized_view(engine):
    print("\n[FEATURES] Creating materialized view for training data...")
    t0 = time.time()

    with engine.begin() as conn:
        conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {SCHEMA_FEATURES}.mv_training_features"))
        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW {SCHEMA_FEATURES}.mv_training_features AS
            {TRAINING_QUERY}
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_mv_district
                ON {SCHEMA_FEATURES}.mv_training_features(district_name);
            CREATE INDEX IF NOT EXISTS idx_mv_date
                ON {SCHEMA_FEATURES}.mv_training_features(full_date);
        """))

        result = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_FEATURES}.mv_training_features"))
        print(f"  Materialized view created: {result.scalar():,} rows in {time.time() - t0:.1f}s")


def run_feature_engineering():
    engine = create_engine(DATABASE_URL)
    create_extra_tables(engine)
    compute_daily_aggregates(engine)
    compute_historical_same_day(engine)
    compute_historical_same_day_hour(engine)
    create_materialized_view(engine)
    engine.dispose()
    print("\nFeature engineering complete!")


if __name__ == "__main__":
    run_feature_engineering()
