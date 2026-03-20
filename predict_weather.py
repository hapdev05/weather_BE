"""
Weather Prediction V3 - Save forecasts to Data Warehouse.

Flow:
  predict day N -> save to dw.fact_weather_forecast
  predict day N+1 -> lag features pull from actual (fact_weather_hourly)
                     + fallback to forecast (fact_weather_forecast) if actual unavailable

This enables chained multi-day forecasting entirely within the DW.
"""

import pandas as pd
import numpy as np
import joblib
import os
import sys
from datetime import timedelta
from sqlalchemy import create_engine, text

from config import DATABASE_URL, MODEL_DIR, TARGETS, SCHEMA_DW, SCHEMA_FEATURES, NEARBY_DAYS


def load_artifacts():
    models = {}
    for target in ["temperature_c", "humidity_percent", "wind_speed_m_s"]:
        models[target] = joblib.load(os.path.join(MODEL_DIR, f"xgb_{target}.joblib"))

    models["rain_clf"] = joblib.load(os.path.join(MODEL_DIR, "xgb_rain_classifier.joblib"))
    rain_reg_path = os.path.join(MODEL_DIR, "xgb_rain_regressor.joblib")
    models["rain_reg"] = joblib.load(rain_reg_path) if os.path.exists(rain_reg_path) else None

    feature_cols = joblib.load(os.path.join(MODEL_DIR, "feature_columns.joblib"))
    return models, feature_cols


# ---------------------------------------------------------------------------
#  Database helpers
# ---------------------------------------------------------------------------

def ensure_date_in_dim(conn, target_date: pd.Timestamp):
    """Insert the prediction date into dim_date if it doesn't exist yet."""
    exists = conn.execute(text(f"""
        SELECT date_id FROM {SCHEMA_DW}.dim_date WHERE full_date = :d
    """), {"d": target_date.date()}).fetchone()

    if exists:
        return exists[0]

    conn.execute(text(f"""
        INSERT INTO {SCHEMA_DW}.dim_date
            (full_date, year, month, day, day_of_year, day_of_week,
             week_of_year, quarter, month_name, is_weekend, month_day)
        VALUES (
            :full_date, :year, :month, :day, :doy, :dow,
            :woy, :quarter, :month_name, :is_weekend, :month_day
        )
        ON CONFLICT (full_date) DO NOTHING
    """), {
        "full_date": target_date.date(),
        "year": target_date.year,
        "month": target_date.month,
        "day": target_date.day,
        "doy": target_date.timetuple().tm_yday,
        "dow": target_date.isoweekday(),
        "woy": target_date.isocalendar()[1],
        "quarter": (target_date.month - 1) // 3 + 1,
        "month_name": target_date.strftime("%B"),
        "is_weekend": target_date.isoweekday() >= 6,
        "month_day": target_date.month * 100 + target_date.day,
    })

    result = conn.execute(text(f"""
        SELECT date_id FROM {SCHEMA_DW}.dim_date WHERE full_date = :d
    """), {"d": target_date.date()}).fetchone()
    return result[0]


def fetch_district_id(conn, district_name: str) -> int:
    result = conn.execute(text(f"""
        SELECT district_id FROM {SCHEMA_DW}.dim_district WHERE district_name = :name
    """), {"name": district_name})
    row = result.fetchone()
    return row[0] if row else None


def fetch_historical_same_day(conn, district_name: str, year: int, month_day: int) -> dict:
    result = conn.execute(text(f"""
        SELECT h.*
        FROM {SCHEMA_FEATURES}.agg_historical_same_day h
        JOIN {SCHEMA_DW}.dim_district dd ON dd.district_id = h.district_id
        WHERE dd.district_name = :district
          AND h.year = :year AND h.month_day = :month_day
    """), {"district": district_name, "year": year, "month_day": month_day})
    row = result.fetchone()
    return dict(row._mapping) if row else {}


def fetch_historical_same_day_hour(conn, district_name: str, year: int, month_day: int, hour: int) -> dict:
    result = conn.execute(text(f"""
        SELECT hh.*
        FROM {SCHEMA_FEATURES}.agg_historical_same_day_hour hh
        JOIN {SCHEMA_DW}.dim_district dd ON dd.district_id = hh.district_id
        WHERE dd.district_name = :district
          AND hh.year = :year AND hh.month_day = :month_day AND hh.hour = :hour
    """), {"district": district_name, "year": year, "month_day": month_day, "hour": hour})
    row = result.fetchone()
    return dict(row._mapping) if row else {}


def fetch_lag_data(conn, district_name: str, target_date: str, hour: int) -> dict:
    """
    Fetch lag data from BOTH actual measurements and previous forecasts.
    Priority: actual data first, fallback to forecast if actual unavailable.
    """
    result = conn.execute(text(f"""
        WITH actual AS (
            SELECT
                dt.full_date,
                (CAST(:target_date AS date) - dt.full_date) AS days_back,
                f.temperature_c, f.humidity_percent, f.wind_speed_m_s, f.rain_mm,
                'actual' AS source
            FROM {SCHEMA_DW}.fact_weather_hourly f
            JOIN {SCHEMA_DW}.dim_district dd ON dd.district_id = f.district_id
            JOIN {SCHEMA_DW}.dim_date dt ON dt.date_id = f.date_id
            JOIN {SCHEMA_DW}.dim_hour dh ON dh.hour_id = f.hour_id
            WHERE dd.district_name = :district
              AND dh.hour = :hour
              AND dt.full_date >= (CAST(:target_date AS date) - INTERVAL '{NEARBY_DAYS} days')
              AND dt.full_date < CAST(:target_date AS date)
        ),
        forecast AS (
            SELECT
                dt.full_date,
                (CAST(:target_date AS date) - dt.full_date) AS days_back,
                fc.temperature_c, fc.humidity_percent, fc.wind_speed_m_s, fc.rain_mm,
                'forecast' AS source
            FROM {SCHEMA_DW}.fact_weather_forecast fc
            JOIN {SCHEMA_DW}.dim_district dd ON dd.district_id = fc.district_id
            JOIN {SCHEMA_DW}.dim_date dt ON dt.date_id = fc.date_id
            JOIN {SCHEMA_DW}.dim_hour dh ON dh.hour_id = fc.hour_id
            WHERE dd.district_name = :district
              AND dh.hour = :hour
              AND dt.full_date >= (CAST(:target_date AS date) - INTERVAL '{NEARBY_DAYS} days')
              AND dt.full_date < CAST(:target_date AS date)
        ),
        combined AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY full_date ORDER BY CASE source WHEN 'actual' THEN 0 ELSE 1 END
            ) AS rn
            FROM (SELECT * FROM actual UNION ALL SELECT * FROM forecast) all_data
        )
        SELECT full_date, days_back, temperature_c, humidity_percent,
               wind_speed_m_s, rain_mm, source
        FROM combined WHERE rn = 1
        ORDER BY full_date DESC
    """), {"district": district_name, "target_date": target_date, "hour": hour})

    lags = {}
    for row in result:
        r = dict(row._mapping)
        lags[int(r["days_back"])] = r
    return lags


# ---------------------------------------------------------------------------
#  Save forecast to DW
# ---------------------------------------------------------------------------

def save_forecast_to_dw(conn, district_id: int, date_id: int, result_df: pd.DataFrame):
    """Upsert forecast results into fact_weather_forecast."""
    conn.execute(text(f"""
        DELETE FROM {SCHEMA_DW}.fact_weather_forecast
        WHERE district_id = :did AND date_id = :dtid
    """), {"did": district_id, "dtid": date_id})

    for _, row in result_df.iterrows():
        conn.execute(text(f"""
            INSERT INTO {SCHEMA_DW}.fact_weather_forecast
                (district_id, date_id, hour_id,
                 temperature_c, humidity_percent, wind_speed_m_s,
                 rain_mm, rain_probability)
            VALUES (:did, :dtid, :hid, :temp, :hum, :wind, :rain, :rain_prob)
        """), {
            "did": district_id,
            "dtid": date_id,
            "hid": int(row["hour"]),
            "temp": float(row["temperature_c"]),
            "hum": float(row["humidity_percent"]),
            "wind": float(row["wind_speed_m_s"]),
            "rain": float(row["rain_mm"]),
            "rain_prob": float(row.get("rain_prob", 0)),
        })


# ---------------------------------------------------------------------------
#  Feature building
# ---------------------------------------------------------------------------

def build_feature_row(hour: int, target_date: pd.Timestamp, district_id: int,
                      hist: dict, hist_hour: dict, lags: dict) -> dict:
    month = target_date.month
    day = target_date.day
    doy = target_date.timetuple().tm_yday
    dow = target_date.isoweekday()

    row = {
        "district_id": district_id,
        "month": month, "day": day, "day_of_year": doy, "day_of_week": dow, "hour": hour,
        "hour_sin": np.sin(2 * np.pi * hour / 24),
        "hour_cos": np.cos(2 * np.pi * hour / 24),
        "doy_sin": np.sin(2 * np.pi * doy / 365.25),
        "doy_cos": np.cos(2 * np.pi * doy / 365.25),
        "month_sin": np.sin(2 * np.pi * month / 12),
        "month_cos": np.cos(2 * np.pi * month / 12),
    }

    for key in ["hist_temp_mean", "hist_temp_std", "hist_temp_min", "hist_temp_max",
                "hist_hum_mean", "hist_hum_std", "hist_hum_min", "hist_hum_max",
                "hist_wind_mean", "hist_wind_std", "hist_wind_min", "hist_wind_max",
                "hist_rain_mean", "hist_rain_std", "hist_rain_min", "hist_rain_max",
                "years_count"]:
        row[key] = hist.get(key, np.nan)

    for key in ["hist_h_temp_mean", "hist_h_temp_std",
                "hist_h_hum_mean", "hist_h_hum_std",
                "hist_h_wind_mean", "hist_h_wind_std",
                "hist_h_rain_mean", "hist_h_rain_max", "hist_h_rain_prob"]:
        row[key] = hist_hour.get(key, np.nan)

    var_map = {"temp": "temperature_c", "hum": "humidity_percent",
               "wind": "wind_speed_m_s", "rain": "rain_mm"}

    for lag_day in range(1, NEARBY_DAYS + 1):
        lag_data = lags.get(lag_day, {})
        for short, db_col in var_map.items():
            row[f"lag_{lag_day}d_{short}"] = lag_data.get(db_col, np.nan)

    for d in range(1, 4):
        rain_val = row.get(f"lag_{d}d_rain", np.nan)
        row[f"lag_{d}d_is_rainy"] = int(rain_val > 0.1) if not (isinstance(rain_val, float) and np.isnan(rain_val)) else 0

    for short in ["temp", "hum", "wind", "rain"]:
        vals_3 = [row.get(f"lag_{d}d_{short}", np.nan) for d in range(1, 4)]
        vals_7 = [row.get(f"lag_{d}d_{short}", np.nan) for d in range(1, 8)]
        v3 = [v for v in vals_3 if not (isinstance(v, float) and np.isnan(v))]
        v7 = [v for v in vals_7 if not (isinstance(v, float) and np.isnan(v))]

        row[f"rolling_3d_{short}_mean"] = np.mean(v3) if v3 else np.nan
        row[f"rolling_7d_{short}_mean"] = np.mean(v7) if v7 else np.nan
        row[f"rolling_3d_{short}_std"] = np.std(v3) if len(v3) > 1 else np.nan

        l1 = row.get(f"lag_1d_{short}", np.nan)
        l3 = row.get(f"lag_3d_{short}", np.nan)
        is_nan_l1 = isinstance(l1, float) and np.isnan(l1)
        is_nan_l3 = isinstance(l3, float) and np.isnan(l3)
        row[f"trend_3d_{short}"] = (l1 - l3) if not (is_nan_l1 or is_nan_l3) else np.nan

    row["recent_rain_count"] = sum(row.get(f"lag_{d}d_is_rainy", 0) for d in range(1, 4))

    return row


# ---------------------------------------------------------------------------
#  Predict + save
# ---------------------------------------------------------------------------

def predict(district: str, predict_date: str, save_to_dw: bool = True) -> pd.DataFrame:
    models, feature_cols = load_artifacts()
    target_date = pd.to_datetime(predict_date)
    month_day = target_date.month * 100 + target_date.day

    engine = create_engine(DATABASE_URL)
    conn = engine.connect()

    districts = pd.read_sql(
        f"SELECT district_name FROM {SCHEMA_DW}.dim_district ORDER BY district_name", conn,
    )["district_name"].tolist()

    if district not in districts:
        print(f"  District '{district}' not found.")
        conn.close()
        engine.dispose()
        return None

    district_id = fetch_district_id(conn, district)
    hist = fetch_historical_same_day(conn, district, target_date.year, month_day)

    rows = []
    for hour in range(24):
        hist_hour = fetch_historical_same_day_hour(conn, district, target_date.year, month_day, hour)
        lags = fetch_lag_data(conn, district, predict_date, hour)
        row = build_feature_row(hour, target_date, district_id, hist, hist_hour, lags)
        rows.append(row)

    X = pd.DataFrame(rows)
    for col in feature_cols:
        if col not in X.columns:
            X[col] = np.nan
    X = X[feature_cols]

    results = {"hour": list(range(24))}

    for target in ["temperature_c", "humidity_percent", "wind_speed_m_s"]:
        preds = models[target].predict(X)
        if target == "humidity_percent":
            preds = np.clip(preds, 0, 100)
        results[target] = np.round(preds, 2)

    rain_prob = models["rain_clf"].predict_proba(X)[:, 1]
    rain_pred = np.zeros(24)
    is_rainy = rain_prob > 0.5
    if models["rain_reg"] is not None and is_rainy.sum() > 0:
        rain_pred[is_rainy] = np.maximum(models["rain_reg"].predict(X[is_rainy]), 0)
    results["rain_mm"] = np.round(rain_pred, 2)
    results["rain_prob"] = np.round(rain_prob * 100, 1)

    result_df = pd.DataFrame(results)

    if save_to_dw:
        write_conn = engine.connect()
        with write_conn.begin():
            date_id = ensure_date_in_dim(write_conn, target_date)
            save_forecast_to_dw(write_conn, district_id, date_id, result_df)
        write_conn.close()
        print(f"  Forecast saved to DW: {district} {predict_date} ({24} hours)")

    conn.close()
    engine.dispose()

    return result_df


def predict_range(district: str, start_date: str, num_days: int) -> dict:
    """Predict multiple consecutive days. Each day's forecast feeds into the next as lag data."""
    print(f"\n  Chained forecast: {district} | {start_date} + {num_days} days")
    print(f"  {'='*55}")

    all_results = {}
    current = pd.to_datetime(start_date)

    for i in range(num_days):
        date_str = current.strftime("%Y-%m-%d")
        print(f"\n  [{i+1}/{num_days}] Predicting {date_str}...")
        df = predict(district, date_str, save_to_dw=True)
        if df is not None:
            all_results[date_str] = df
            summary_temp = f"{df['temperature_c'].min():.1f}-{df['temperature_c'].max():.1f}°C"
            summary_rain = f"{df['rain_mm'].sum():.1f}mm"
            print(f"          Temp: {summary_temp} | Rain: {summary_rain}")
        current += timedelta(days=1)

    print(f"\n  {'='*55}")
    print(f"  Done! {len(all_results)} days forecasted and saved to DW.")
    return all_results


# ---------------------------------------------------------------------------
#  Display
# ---------------------------------------------------------------------------

def weather_desc(temp, hum, wind, rain, rain_prob):
    parts = []
    if rain > 5:
        parts.append("Mua to")
    elif rain > 1:
        parts.append("Mua nhe")
    elif rain > 0.1:
        parts.append("Mua phun")
    elif rain_prob > 40:
        parts.append(f"Co the mua ({rain_prob:.0f}%)")
    elif temp > 33:
        parts.append("Nang nong")
    elif temp > 28:
        parts.append("Nang")
    else:
        parts.append("Mat me")

    if hum > 85:
        parts.append("do am cao")
    if wind > 15:
        parts.append("gio manh")
    elif wind > 10:
        parts.append("gio vua")
    return ", ".join(parts)


def display(district: str, date: str, df: pd.DataFrame):
    print("\n" + "=" * 90)
    print(f"  DU BAO THOI TIET - {district.replace('_', ' ')}")
    print(f"  Ngay: {date}")
    print("=" * 90)
    print(f"{'Gio':>5} | {'Nhiet do':>10} | {'Do am':>8} | {'Gio (m/s)':>10} | {'Mua (mm)':>9} | {'P(mua)':>7} | Mo ta")
    print("-" * 90)

    for _, r in df.iterrows():
        h = int(r["hour"])
        t, hu, w, ra = r["temperature_c"], r["humidity_percent"], r["wind_speed_m_s"], r["rain_mm"]
        rp = r.get("rain_prob", 0)
        desc = weather_desc(t, hu, w, ra, rp)
        print(f"{h:02d}:00 | {t:>8.1f} °C | {hu:>6.1f} % | {w:>8.1f}   | {ra:>7.2f}   | {rp:>5.1f}% | {desc}")

    print("-" * 90)
    print(f"\n  TONG KET:")
    print(f"    Nhiet do: {df['temperature_c'].min():.1f} - {df['temperature_c'].max():.1f} °C "
          f"(TB: {df['temperature_c'].mean():.1f} °C)")
    print(f"    Do am TB: {df['humidity_percent'].mean():.1f}%")
    print(f"    Tong mua: {df['rain_mm'].sum():.2f} mm")
    print(f"    Gio max : {df['wind_speed_m_s'].max():.1f} m/s")
    print("=" * 90)


def main():
    if len(sys.argv) < 3:
        print("Usage:")
        print("  python predict_weather.py <district> <date>             # 1 ngay, luu vao DW")
        print("  python predict_weather.py <district> <date> <num_days>  # nhieu ngay lien tiep")
        print("\nVi du:")
        print("  python predict_weather.py Ngu_Hanh_Son 2026-03-20")
        print("  python predict_weather.py My_Khe_Beach 2026-03-20 7    # 7 ngay lien tiep")
        engine = create_engine(DATABASE_URL)
        districts = pd.read_sql(
            f"SELECT district_name FROM {SCHEMA_DW}.dim_district ORDER BY district_name", engine,
        )["district_name"].tolist()
        engine.dispose()
        print("\nCac dia diem:")
        for d in districts:
            print(f"  - {d}")
        return

    district = sys.argv[1]
    date = sys.argv[2]
    num_days = int(sys.argv[3]) if len(sys.argv) > 3 else 1

    if num_days == 1:
        result_df = predict(district, date, save_to_dw=True)
        if result_df is not None:
            display(district, date, result_df)
    else:
        results = predict_range(district, date, num_days)
        if results:
            for d, df in results.items():
                display(district, d, df)


if __name__ == "__main__":
    main()
