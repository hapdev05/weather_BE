"""
Weather Prediction API - Da Nang

Endpoints:
  GET  /api/health                          Health check
  GET  /api/districts                       List all districts
  POST /api/predict                         Predict 1 day
  POST /api/predict/range                   Predict multiple days (chained)
  GET  /api/forecasts                       List saved forecasts
  GET  /api/forecasts/{district}/{date}     Get specific forecast from DW
  GET  /api/actual/{district}/{date}        Get actual weather data from DW
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from datetime import datetime, timedelta
import pandas as pd

from api.database import get_conn, engine
from api.schemas import (
    PredictRequest, PredictRangeRequest,
    ForecastResponse, RangeForecastResponse, HourlyForecast, DailySummary,
    DistrictInfo, ActualWeather, ActualResponse, SavedForecastEntry,
)
from config import SCHEMA_DW, SCHEMA_FEATURES
from predict_weather import predict as run_predict, weather_desc

app = FastAPI(
    title="Weather Prediction API - Da Nang",
    description="Du bao thoi tiet Da Nang su dung XGBoost + PostgreSQL Data Warehouse",
    version="2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def build_forecast_response(district: str, date: str, df: pd.DataFrame, saved: bool) -> ForecastResponse:
    hourly = []
    for _, r in df.iterrows():
        t, hu, w, ra = r["temperature_c"], r["humidity_percent"], r["wind_speed_m_s"], r["rain_mm"]
        rp = r.get("rain_prob", 0)
        hourly.append(HourlyForecast(
            hour=int(r["hour"]),
            temperature_c=round(t, 1),
            humidity_percent=round(hu, 1),
            wind_speed_m_s=round(w, 1),
            rain_mm=round(ra, 2),
            rain_probability=round(rp, 1),
            description=weather_desc(t, hu, w, ra, rp),
        ))

    summary = DailySummary(
        temp_min=round(float(df["temperature_c"].min()), 1),
        temp_max=round(float(df["temperature_c"].max()), 1),
        temp_avg=round(float(df["temperature_c"].mean()), 1),
        humidity_avg=round(float(df["humidity_percent"].mean()), 1),
        rain_total=round(float(df["rain_mm"].sum()), 2),
        wind_max=round(float(df["wind_speed_m_s"].max()), 1),
    )

    return ForecastResponse(
        district=district, date=date,
        hourly=hourly, summary=summary, saved_to_dw=saved,
    )


# ── Health ───────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": str(e)}


# ── Districts ────────────────────────────────────────────────────────────────

@app.get("/api/districts", response_model=list[DistrictInfo])
def list_districts(conn=Depends(get_conn)):
    rows = conn.execute(text(f"""
        SELECT district_id, district_name
        FROM {SCHEMA_DW}.dim_district ORDER BY district_name
    """))
    return [DistrictInfo(district_id=r[0], district_name=r[1]) for r in rows]


# ── Predict 1 day ───────────────────────────────────────────────────────────

@app.post("/api/predict", response_model=ForecastResponse)
def predict_one_day(req: PredictRequest):
    try:
        pd.to_datetime(req.date)
    except ValueError:
        raise HTTPException(400, f"Invalid date: {req.date}")

    df = run_predict(req.district, req.date, save_to_dw=True)
    if df is None:
        raise HTTPException(404, f"District '{req.district}' not found")

    return build_forecast_response(req.district, req.date, df, saved=True)


# ── Predict range (chained) ─────────────────────────────────────────────────

@app.post("/api/predict/range", response_model=RangeForecastResponse)
def predict_multi_day(req: PredictRangeRequest):
    try:
        start = pd.to_datetime(req.start_date)
    except ValueError:
        raise HTTPException(400, f"Invalid date: {req.start_date}")

    forecasts = []
    current = start
    for _ in range(req.num_days):
        date_str = current.strftime("%Y-%m-%d")
        df = run_predict(req.district, date_str, save_to_dw=True)
        if df is None:
            raise HTTPException(404, f"District '{req.district}' not found")
        forecasts.append(build_forecast_response(req.district, date_str, df, saved=True))
        current += timedelta(days=1)

    return RangeForecastResponse(
        district=req.district,
        start_date=req.start_date,
        num_days=req.num_days,
        forecasts=forecasts,
    )


# ── Saved forecasts ─────────────────────────────────────────────────────────

@app.get("/api/forecasts", response_model=list[SavedForecastEntry])
def list_forecasts(conn=Depends(get_conn)):
    rows = conn.execute(text(f"""
        SELECT
            dd.district_name, dt.full_date, COUNT(*) AS hours,
            MIN(fc.temperature_c) AS temp_min, MAX(fc.temperature_c) AS temp_max,
            SUM(fc.rain_mm) AS rain_total,
            MAX(fc.predicted_at)::text AS predicted_at
        FROM {SCHEMA_DW}.fact_weather_forecast fc
        JOIN {SCHEMA_DW}.dim_district dd ON dd.district_id = fc.district_id
        JOIN {SCHEMA_DW}.dim_date dt ON dt.date_id = fc.date_id
        GROUP BY dd.district_name, dt.full_date
        ORDER BY dt.full_date DESC, dd.district_name
    """))
    return [
        SavedForecastEntry(
            district=r[0], date=str(r[1]), hours_count=r[2],
            temp_min=round(r[3], 1), temp_max=round(r[4], 1),
            rain_total=round(r[5], 2), predicted_at=r[6],
        ) for r in rows
    ]


@app.get("/api/forecasts/{district}/{date}", response_model=ForecastResponse)
def get_forecast(district: str, date: str, conn=Depends(get_conn)):
    rows = conn.execute(text(f"""
        SELECT
            dh.hour,
            fc.temperature_c, fc.humidity_percent, fc.wind_speed_m_s,
            fc.rain_mm, fc.rain_probability
        FROM {SCHEMA_DW}.fact_weather_forecast fc
        JOIN {SCHEMA_DW}.dim_district dd ON dd.district_id = fc.district_id
        JOIN {SCHEMA_DW}.dim_date dt ON dt.date_id = fc.date_id
        JOIN {SCHEMA_DW}.dim_hour dh ON dh.hour_id = fc.hour_id
        WHERE dd.district_name = :district AND dt.full_date = CAST(:date AS date)
        ORDER BY dh.hour
    """), {"district": district, "date": date})

    data = [dict(r._mapping) for r in rows]
    if not data:
        raise HTTPException(404, f"No forecast found for {district} on {date}")

    hourly = []
    for r in data:
        t, hu, w, ra, rp = r["temperature_c"], r["humidity_percent"], r["wind_speed_m_s"], r["rain_mm"], r["rain_probability"] or 0
        hourly.append(HourlyForecast(
            hour=r["hour"],
            temperature_c=round(t, 1), humidity_percent=round(hu, 1),
            wind_speed_m_s=round(w, 1), rain_mm=round(ra, 2),
            rain_probability=round(rp, 1),
            description=weather_desc(t, hu, w, ra, rp),
        ))

    temps = [r["temperature_c"] for r in data]
    summary = DailySummary(
        temp_min=round(min(temps), 1), temp_max=round(max(temps), 1),
        temp_avg=round(sum(temps) / len(temps), 1),
        humidity_avg=round(sum(r["humidity_percent"] for r in data) / len(data), 1),
        rain_total=round(sum(r["rain_mm"] for r in data), 2),
        wind_max=round(max(r["wind_speed_m_s"] for r in data), 1),
    )

    return ForecastResponse(
        district=district, date=date,
        hourly=hourly, summary=summary, saved_to_dw=True,
    )


# ── Actual data ──────────────────────────────────────────────────────────────

@app.get("/api/actual/{district}/{date}", response_model=ActualResponse)
def get_actual(district: str, date: str, conn=Depends(get_conn)):
    rows = conn.execute(text(f"""
        SELECT
            dh.hour, dh.period_of_day,
            f.temperature_c, f.humidity_percent, f.wind_speed_m_s, f.rain_mm
        FROM {SCHEMA_DW}.fact_weather_hourly f
        JOIN {SCHEMA_DW}.dim_district dd ON dd.district_id = f.district_id
        JOIN {SCHEMA_DW}.dim_date dt ON dt.date_id = f.date_id
        JOIN {SCHEMA_DW}.dim_hour dh ON dh.hour_id = f.hour_id
        WHERE dd.district_name = :district AND dt.full_date = CAST(:date AS date)
        ORDER BY dh.hour
    """), {"district": district, "date": date})

    data = [dict(r._mapping) for r in rows]
    if not data:
        raise HTTPException(404, f"No actual data for {district} on {date}")

    hourly = [ActualWeather(
        hour=r["hour"],
        temperature_c=round(r["temperature_c"], 1),
        humidity_percent=round(r["humidity_percent"], 1),
        wind_speed_m_s=round(r["wind_speed_m_s"], 1),
        rain_mm=round(r["rain_mm"], 2),
        period_of_day=r["period_of_day"],
    ) for r in data]

    temps = [r["temperature_c"] for r in data]
    summary = DailySummary(
        temp_min=round(min(temps), 1), temp_max=round(max(temps), 1),
        temp_avg=round(sum(temps) / len(temps), 1),
        humidity_avg=round(sum(r["humidity_percent"] for r in data) / len(data), 1),
        rain_total=round(sum(r["rain_mm"] for r in data), 2),
        wind_max=round(max(r["wind_speed_m_s"] for r in data), 1),
    )

    return ActualResponse(district=district, date=date, hourly=hourly, summary=summary)
