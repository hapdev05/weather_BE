"""
Batch forecast scheduler.

Runs daily: predict next FORECAST_DAYS for ALL districts, save to DW.

Auto-schedule:
  - Moi ngay luc 00:05 → predict 2 ngay tiep theo cho 53 districts
  - Khi server start → check xem da co forecast cho ngay mai chua, neu chua thi chay ngay

Manual trigger:
  - POST /api/admin/run-forecast
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

from config import DATABASE_URL, SCHEMA_DW
from predict_weather import predict as run_predict

logger = logging.getLogger("scheduler")

FORECAST_DAYS = 2


def get_all_districts() -> list[str]:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT district_name FROM {SCHEMA_DW}.dim_district ORDER BY district_name"
        ))
        districts = [r[0] for r in rows]
    engine.dispose()
    return districts


def get_last_actual_date() -> str:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT MAX(dt.full_date)
            FROM {SCHEMA_DW}.fact_weather_hourly f
            JOIN {SCHEMA_DW}.dim_date dt ON dt.date_id = f.date_id
        """)).fetchone()
    engine.dispose()
    return str(row[0]) if row and row[0] else datetime.now().strftime("%Y-%m-%d")


def get_forecast_dates_in_dw() -> list[str]:
    """Check which dates already have forecasts in DW."""
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT DISTINCT dt.full_date
            FROM {SCHEMA_DW}.fact_weather_forecast fc
            JOIN {SCHEMA_DW}.dim_date dt ON dt.date_id = fc.date_id
            ORDER BY dt.full_date
        """))
        dates = [str(r[0]) for r in rows]
    engine.dispose()
    return dates


def check_and_run_if_needed():
    """
    Check xem da co forecast cho ngay mai + ngay kia chua.
    Neu chua → tu dong chay batch forecast.
    Duoc goi khi server start va moi ngay boi scheduler.
    """
    last_actual = get_last_actual_date()
    base = datetime.strptime(last_actual, "%Y-%m-%d")

    needed_dates = []
    for d in range(1, FORECAST_DAYS + 1):
        needed_dates.append((base + timedelta(days=d)).strftime("%Y-%m-%d"))

    existing = get_forecast_dates_in_dw()

    # Check: co du forecast cho tat ca districts cho cac ngay can thiet?
    engine = create_engine(DATABASE_URL)
    missing = False
    with engine.connect() as conn:
        for date_str in needed_dates:
            count = conn.execute(text(f"""
                SELECT COUNT(DISTINCT fc.district_id)
                FROM {SCHEMA_DW}.fact_weather_forecast fc
                JOIN {SCHEMA_DW}.dim_date dt ON dt.date_id = fc.date_id
                WHERE dt.full_date = CAST(:d AS date)
            """), {"d": date_str}).scalar()

            if count < 53:
                missing = True
                logger.info(f"  Date {date_str}: {count}/53 districts → NEED FORECAST")
            else:
                logger.info(f"  Date {date_str}: {count}/53 districts → OK")
    engine.dispose()

    if missing:
        logger.info(f"Running batch forecast: base={last_actual}, days={FORECAST_DAYS}")
        return run_batch_forecast(base_date=last_actual, forecast_days=FORECAST_DAYS)
    else:
        logger.info("All forecasts up to date. Skipping.")
        return {"status": "skipped", "message": "Forecasts already exist", "dates": needed_dates}


def run_batch_forecast(base_date: str = None, forecast_days: int = FORECAST_DAYS) -> dict:
    """
    Predict next `forecast_days` for ALL districts starting from base_date + 1.
    """
    if base_date is None:
        base_date = get_last_actual_date()

    base = datetime.strptime(base_date, "%Y-%m-%d")
    districts = get_all_districts()

    forecast_dates = [(base + timedelta(days=d)).strftime("%Y-%m-%d") for d in range(1, forecast_days + 1)]

    logger.info(f"BATCH START: base={base_date}, dates={forecast_dates}, districts={len(districts)}")

    results = {
        "status": "running",
        "base_date": base_date,
        "forecast_days": forecast_days,
        "dates": forecast_dates,
        "districts_count": len(districts),
        "details": [],
        "errors": [],
        "started_at": datetime.now().isoformat(),
    }

    total = len(districts) * len(forecast_dates)
    done = 0

    for date_str in forecast_dates:
        for district in districts:
            try:
                df = run_predict(district, date_str, save_to_dw=True)
                done += 1

                if df is not None:
                    results["details"].append({
                        "district": district,
                        "date": date_str,
                        "temp_min": round(float(df["temperature_c"].min()), 1),
                        "temp_max": round(float(df["temperature_c"].max()), 1),
                        "rain_total": round(float(df["rain_mm"].sum()), 2),
                    })

                if done % 20 == 0:
                    pct = round(done / total * 100)
                    logger.info(f"  Progress: {done}/{total} ({pct}%)")

            except Exception as e:
                logger.error(f"  Error: {district} {date_str}: {e}")
                results["errors"].append({"district": district, "date": date_str, "error": str(e)})
                done += 1

    results["total_forecasts"] = done
    results["success"] = done - len(results["errors"])
    results["status"] = "completed"
    results["completed_at"] = datetime.now().isoformat()

    logger.info(f"BATCH DONE: {results['success']}/{total} OK, {len(results['errors'])} errors")
    return results


def scheduled_daily_forecast():
    """Called by APScheduler at configured time each day."""
    logger.info("=" * 50)
    logger.info("SCHEDULED DAILY FORECAST TRIGGERED")
    logger.info("=" * 50)
    try:
        result = check_and_run_if_needed()
        logger.info(f"Result: {result.get('status')} - {result.get('success', 0)} forecasts")
    except Exception as e:
        logger.error(f"SCHEDULED FORECAST FAILED: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    print("Running batch forecast manually...")
    result = run_batch_forecast()
    print(f"\nDone: {result['success']}/{result['total_forecasts']} | Errors: {len(result['errors'])}")
    print(f"Dates: {result['dates']}")
