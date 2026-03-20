"""
Weather Prediction API - Da Nang

Auto-scheduler: moi ngay tu dong predict 2 ngay tiep theo cho TAT CA 53 districts,
luu vao Data Warehouse. FE chi can GET data.

FE Endpoints:
  GET  /api/weather                             Forecast tat ca districts (summary)
  GET  /api/weather/{district}                  Forecast 1 district (2 ngay, hourly)
  GET  /api/weather/{district}/{date}           Forecast 1 district, 1 ngay
  GET  /api/districts                           Danh sach districts
  GET  /api/actual/{district}/{date}            Data thuc te tu DW

Admin Endpoints:
  POST /api/admin/run-forecast                  Trigger batch forecast thu cong
  GET  /api/admin/forecast-status               Xem trang thai batch cuoi cung
  GET  /api/health                              Health check
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from apscheduler.schedulers.background import BackgroundScheduler

from api.database import get_conn, engine
from api.schemas import (
    ForecastResponse, HourlyForecast, DailySummary,
    DistrictInfo, ActualWeather, ActualResponse, SavedForecastEntry,
)
from api.scheduler import run_batch_forecast, scheduled_daily_forecast, get_last_actual_date
from config import SCHEMA_DW, SCHEMA_FEATURES
from predict_weather import weather_desc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

scheduler = BackgroundScheduler()
last_batch_result: dict = {}

SCHEDULE_HOUR = 0
SCHEDULE_MINUTE = 5


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(
        scheduled_daily_forecast,
        trigger="cron",
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE,
        id="daily_forecast",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started: daily forecast at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d}")
    yield
    scheduler.shutdown()
    logger.info("Scheduler stopped")


app = FastAPI(
    title="Weather Prediction API - Da Nang",
    description="Auto-forecast 53 districts, 2 ngay tiep theo, luu vao PostgreSQL Data Warehouse.",
    version="3.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════════════════════════════════════
#  FE ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/weather", summary="Forecast tat ca districts")
def get_all_weather_summary(conn=Depends(get_conn)):
    """
    Tra ve summary forecast cua tat ca districts cho cac ngay da predict.
    FE dung endpoint nay de hien thi danh sach.
    """
    rows = conn.execute(text(f"""
        SELECT
            dd.district_name,
            dt.full_date,
            ROUND(MIN(fc.temperature_c)::numeric, 1) AS temp_min,
            ROUND(MAX(fc.temperature_c)::numeric, 1) AS temp_max,
            ROUND(AVG(fc.temperature_c)::numeric, 1) AS temp_avg,
            ROUND(AVG(fc.humidity_percent)::numeric, 1) AS humidity_avg,
            ROUND(SUM(fc.rain_mm)::numeric, 2) AS rain_total,
            ROUND(MAX(fc.wind_speed_m_s)::numeric, 1) AS wind_max,
            ROUND(AVG(fc.rain_probability)::numeric, 1) AS rain_prob_avg
        FROM {SCHEMA_DW}.fact_weather_forecast fc
        JOIN {SCHEMA_DW}.dim_district dd ON dd.district_id = fc.district_id
        JOIN {SCHEMA_DW}.dim_date dt ON dt.date_id = fc.date_id
        GROUP BY dd.district_name, dt.full_date
        ORDER BY dt.full_date, dd.district_name
    """))

    results = []
    for r in rows:
        results.append({
            "district": r[0],
            "date": str(r[1]),
            "summary": {
                "temp_min": float(r[2]),
                "temp_max": float(r[3]),
                "temp_avg": float(r[4]),
                "humidity_avg": float(r[5]),
                "rain_total": float(r[6]),
                "wind_max": float(r[7]),
                "rain_prob_avg": float(r[8]) if r[8] else 0,
            }
        })
    return results


@app.get("/api/weather/{district}", summary="Forecast 1 district (tat ca ngay da predict)")
def get_district_weather(district: str, conn=Depends(get_conn)):
    """
    Tra ve hourly forecast cho 1 district, tat ca cac ngay da predict.
    FE dung endpoint nay khi user chon 1 district.
    """
    rows = conn.execute(text(f"""
        SELECT
            dt.full_date, dh.hour,
            fc.temperature_c, fc.humidity_percent, fc.wind_speed_m_s,
            fc.rain_mm, fc.rain_probability
        FROM {SCHEMA_DW}.fact_weather_forecast fc
        JOIN {SCHEMA_DW}.dim_district dd ON dd.district_id = fc.district_id
        JOIN {SCHEMA_DW}.dim_date dt ON dt.date_id = fc.date_id
        JOIN {SCHEMA_DW}.dim_hour dh ON dh.hour_id = fc.hour_id
        WHERE dd.district_name = :district
        ORDER BY dt.full_date, dh.hour
    """), {"district": district})

    data = [dict(r._mapping) for r in rows]
    if not data:
        raise HTTPException(404, f"No forecast for '{district}'")

    dates = sorted(set(r["full_date"] for r in data))
    forecasts = []

    for date in dates:
        day_data = [r for r in data if r["full_date"] == date]
        hourly = []
        for r in day_data:
            t, hu, w, ra = r["temperature_c"], r["humidity_percent"], r["wind_speed_m_s"], r["rain_mm"]
            rp = r["rain_probability"] or 0
            hourly.append({
                "hour": r["hour"],
                "temperature_c": round(t, 1),
                "humidity_percent": round(hu, 1),
                "wind_speed_m_s": round(w, 1),
                "rain_mm": round(ra, 2),
                "rain_probability": round(rp, 1),
                "description": weather_desc(t, hu, w, ra, rp),
            })

        temps = [r["temperature_c"] for r in day_data]
        forecasts.append({
            "date": str(date),
            "hourly": hourly,
            "summary": {
                "temp_min": round(min(temps), 1),
                "temp_max": round(max(temps), 1),
                "temp_avg": round(sum(temps) / len(temps), 1),
                "humidity_avg": round(sum(r["humidity_percent"] for r in day_data) / len(day_data), 1),
                "rain_total": round(sum(r["rain_mm"] for r in day_data), 2),
                "wind_max": round(max(r["wind_speed_m_s"] for r in day_data), 1),
            }
        })

    return {"district": district, "forecasts": forecasts}


@app.get("/api/weather/{district}/{date}", summary="Forecast 1 district, 1 ngay")
def get_district_weather_date(district: str, date: str, conn=Depends(get_conn)):
    """
    Tra ve hourly forecast cho 1 district, 1 ngay cu the.
    """
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
        raise HTTPException(404, f"No forecast for {district} on {date}")

    hourly = []
    for r in data:
        t, hu, w, ra = r["temperature_c"], r["humidity_percent"], r["wind_speed_m_s"], r["rain_mm"]
        rp = r["rain_probability"] or 0
        hourly.append({
            "hour": r["hour"],
            "temperature_c": round(t, 1),
            "humidity_percent": round(hu, 1),
            "wind_speed_m_s": round(w, 1),
            "rain_mm": round(ra, 2),
            "rain_probability": round(rp, 1),
            "description": weather_desc(t, hu, w, ra, rp),
        })

    temps = [r["temperature_c"] for r in data]
    summary = {
        "temp_min": round(min(temps), 1),
        "temp_max": round(max(temps), 1),
        "temp_avg": round(sum(temps) / len(temps), 1),
        "humidity_avg": round(sum(r["humidity_percent"] for r in data) / len(data), 1),
        "rain_total": round(sum(r["rain_mm"] for r in data), 2),
        "wind_max": round(max(r["wind_speed_m_s"] for r in data), 1),
    }

    return {"district": district, "date": date, "hourly": hourly, "summary": summary}


@app.get("/api/districts", summary="Danh sach districts")
def list_districts(conn=Depends(get_conn)):
    rows = conn.execute(text(f"""
        SELECT district_id, district_name FROM {SCHEMA_DW}.dim_district ORDER BY district_name
    """))
    return [{"district_id": r[0], "district_name": r[1]} for r in rows]


@app.get("/api/actual/{district}/{date}", summary="Actual weather data tu DW")
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

    hourly = [{
        "hour": r["hour"],
        "temperature_c": round(r["temperature_c"], 1),
        "humidity_percent": round(r["humidity_percent"], 1),
        "wind_speed_m_s": round(r["wind_speed_m_s"], 1),
        "rain_mm": round(r["rain_mm"], 2),
        "period_of_day": r["period_of_day"],
    } for r in data]

    temps = [r["temperature_c"] for r in data]
    summary = {
        "temp_min": round(min(temps), 1),
        "temp_max": round(max(temps), 1),
        "temp_avg": round(sum(temps) / len(temps), 1),
        "humidity_avg": round(sum(r["humidity_percent"] for r in data) / len(data), 1),
        "rain_total": round(sum(r["rain_mm"] for r in data), 2),
        "wind_max": round(max(r["wind_speed_m_s"] for r in data), 1),
    }

    return {"district": district, "date": date, "hourly": hourly, "summary": summary}


# ═══════════════════════════════════════════════════════════════════════════
#  ADMIN ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/admin/run-forecast", summary="Trigger batch forecast thu cong")
def trigger_forecast(
    background_tasks: BackgroundTasks,
    base_date: str = Query(default=None, description="Base date (YYYY-MM-DD). Default: last actual date"),
    forecast_days: int = Query(default=2, ge=1, le=7, description="So ngay du doan"),
):
    """
    Trigger batch predict cho TAT CA districts.
    Chay background, tra ve ngay lap tuc.
    """
    global last_batch_result
    last_batch_result = {"status": "running", "started_at": datetime.now().isoformat()}

    def _run():
        global last_batch_result
        try:
            result = run_batch_forecast(base_date=base_date, forecast_days=forecast_days)
            result["status"] = "completed"
            result["completed_at"] = datetime.now().isoformat()
            last_batch_result = result
        except Exception as e:
            last_batch_result = {
                "status": "error",
                "error": str(e),
                "completed_at": datetime.now().isoformat(),
            }

    background_tasks.add_task(_run)

    return {
        "message": f"Batch forecast started for all districts, {forecast_days} days from {base_date or 'last actual date'}",
        "status": "running",
        "check_progress": "/api/admin/forecast-status",
    }


@app.get("/api/admin/forecast-status", summary="Trang thai batch forecast")
def forecast_status():
    if not last_batch_result:
        return {"status": "no_batch_run_yet"}
    return last_batch_result


@app.get("/api/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        last_actual = get_last_actual_date()

        with engine.connect() as conn:
            fc_count = conn.execute(text(
                f"SELECT COUNT(DISTINCT district_id || '-' || date_id) FROM {SCHEMA_DW}.fact_weather_forecast"
            )).scalar()

        return {
            "status": "ok",
            "database": "connected",
            "last_actual_date": last_actual,
            "forecasts_in_dw": fc_count,
            "scheduler_running": scheduler.running,
            "next_auto_forecast": str(scheduler.get_job("daily_forecast").next_run_time)
                if scheduler.get_job("daily_forecast") else None,
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}
