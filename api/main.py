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
import pytz
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from sqlalchemy import text
from apscheduler.schedulers.background import BackgroundScheduler

from api.database import get_conn, engine
from api.schemas import (
    ForecastResponse, HourlyForecast, DailySummary,
    DistrictInfo, ActualWeather, ActualResponse, SavedForecastEntry,
)
from api.scheduler import (
    run_batch_forecast, scheduled_daily_forecast,
    get_last_actual_date, check_and_run_if_needed,
)
from config import SCHEMA_DW, SCHEMA_FEATURES
from predict_weather import weather_desc

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("api")

ICT = pytz.timezone("Asia/Ho_Chi_Minh")
scheduler = BackgroundScheduler(timezone=ICT)
last_batch_result: dict = {}

SCHEDULE_HOUR = 0
SCHEDULE_MINUTE = 5


def _startup_check():
    """Check khi server start: neu chua co forecast cho ngay mai → chay ngay."""
    global last_batch_result
    logger.info("Startup check: verifying forecasts...")
    try:
        result = check_and_run_if_needed()
        last_batch_result = result
        logger.info(f"Startup check done: {result.get('status')}")
    except Exception as e:
        logger.error(f"Startup check failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Schedule daily forecast
    scheduler.add_job(
        scheduled_daily_forecast,
        trigger="cron",
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE,
        timezone=ICT,
        id="daily_forecast",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started: daily forecast at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d}")

    # Startup: check if forecasts exist, run if missing
    import threading
    threading.Thread(target=_startup_check, daemon=True).start()

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


# ═══════════════════════════════════════════════════════════════════════════
#  DASHBOARD - Xem data truc tiep tren browser
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse, summary="Dashboard xem data")
def dashboard(conn=Depends(get_conn)):
    stats = {}
    for label, q in [
        ("Actual rows", f"SELECT COUNT(*) FROM {SCHEMA_DW}.fact_weather_hourly"),
        ("Forecast rows", f"SELECT COUNT(*) FROM {SCHEMA_DW}.fact_weather_forecast"),
        ("Districts", f"SELECT COUNT(*) FROM {SCHEMA_DW}.dim_district"),
        ("Dates", f"SELECT COUNT(*) FROM {SCHEMA_DW}.dim_date"),
    ]:
        stats[label] = conn.execute(text(q)).scalar()

    fc_rows = conn.execute(text(f"""
        SELECT dd.district_name, dt.full_date,
            ROUND(MIN(fc.temperature_c)::numeric,1) t_min,
            ROUND(MAX(fc.temperature_c)::numeric,1) t_max,
            ROUND(AVG(fc.humidity_percent)::numeric,1) hum,
            ROUND(SUM(fc.rain_mm)::numeric,2) rain,
            ROUND(MAX(fc.wind_speed_m_s)::numeric,1) wind
        FROM {SCHEMA_DW}.fact_weather_forecast fc
        JOIN {SCHEMA_DW}.dim_district dd ON dd.district_id = fc.district_id
        JOIN {SCHEMA_DW}.dim_date dt ON dt.date_id = fc.date_id
        GROUP BY dd.district_name, dt.full_date
        ORDER BY dt.full_date, dd.district_name
    """))
    fc_data = [dict(r._mapping) for r in fc_rows]

    actual_rows = conn.execute(text(f"""
        SELECT dd.district_name, dt.full_date,
            ROUND(MIN(f.temperature_c)::numeric,1) t_min,
            ROUND(MAX(f.temperature_c)::numeric,1) t_max,
            ROUND(AVG(f.humidity_percent)::numeric,1) hum,
            ROUND(SUM(f.rain_mm)::numeric,2) rain
        FROM {SCHEMA_DW}.fact_weather_hourly f
        JOIN {SCHEMA_DW}.dim_district dd ON dd.district_id = f.district_id
        JOIN {SCHEMA_DW}.dim_date dt ON dt.date_id = f.date_id
        WHERE dt.full_date >= (SELECT MAX(full_date) - INTERVAL '2 days' FROM {SCHEMA_DW}.dim_date
            WHERE date_id IN (SELECT DISTINCT date_id FROM {SCHEMA_DW}.fact_weather_hourly))
        GROUP BY dd.district_name, dt.full_date
        ORDER BY dt.full_date DESC, dd.district_name
        LIMIT 120
    """))
    actual_data = [dict(r._mapping) for r in actual_rows]

    stat_cards = "".join(
        f'<div class="card"><div class="num">{v:,}</div><div class="lbl">{k}</div></div>'
        for k, v in stats.items()
    )

    dates = sorted(set(str(r["full_date"]) for r in fc_data))
    date_tabs = "".join(f'<button class="tab" onclick="showDate(this,\'{d}\')">{d}</button>' for d in dates)

    fc_tables = {}
    for d in dates:
        rows_html = ""
        for r in [x for x in fc_data if str(x["full_date"]) == d]:
            rain_cls = "rain" if r["rain"] > 0 else ""
            rows_html += f"""<tr class="{rain_cls}">
                <td>{r['district_name']}</td>
                <td>{r['t_min']}</td><td>{r['t_max']}</td>
                <td>{r['hum']}%</td><td>{r['rain']}mm</td><td>{r['wind']}m/s</td>
            </tr>"""
        fc_tables[d] = rows_html

    fc_divs = ""
    for i, (d, html) in enumerate(fc_tables.items()):
        display = "block" if i == 0 else "none"
        fc_divs += f"""<div class="date-table" id="dt-{d}" style="display:{display}">
            <table><thead><tr><th>District</th><th>Min°C</th><th>Max°C</th><th>Humidity</th><th>Rain</th><th>Wind</th></tr></thead>
            <tbody>{html}</tbody></table></div>"""

    actual_html = ""
    for r in actual_data[:60]:
        actual_html += f"""<tr>
            <td>{r['district_name']}</td><td>{r['full_date']}</td>
            <td>{r['t_min']}</td><td>{r['t_max']}</td>
            <td>{r['hum']}%</td><td>{r['rain']}mm</td>
        </tr>"""

    sched_info = ""
    job = scheduler.get_job("daily_forecast")
    if job:
        sched_info = f"Next auto-forecast: {job.next_run_time}"

    batch_status = last_batch_result.get("status", "none") if last_batch_result else "none"
    batch_html = f'<span style="color:#22c55e">&#9679;</span> Last batch: {batch_status}'
    if last_batch_result.get("success"):
        batch_html += f' ({last_batch_result["success"]} forecasts)'

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Weather DW Dashboard</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:20px}}
h1{{text-align:center;font-size:1.8rem;margin-bottom:8px;color:#38bdf8}}
.sub{{text-align:center;color:#94a3b8;margin-bottom:24px;font-size:.9rem}}
.cards{{display:flex;gap:16px;justify-content:center;margin-bottom:32px;flex-wrap:wrap}}
.card{{background:#1e293b;border-radius:12px;padding:20px 32px;text-align:center;min-width:140px;border:1px solid #334155}}
.card .num{{font-size:1.8rem;font-weight:700;color:#38bdf8}}
.card .lbl{{color:#94a3b8;font-size:.85rem;margin-top:4px}}
h2{{font-size:1.2rem;margin:24px 0 12px;color:#f1f5f9;border-bottom:1px solid #334155;padding-bottom:8px}}
.tabs{{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap}}
.tab{{background:#1e293b;border:1px solid #334155;color:#e2e8f0;padding:8px 20px;border-radius:8px;cursor:pointer;font-size:.9rem}}
.tab:hover,.tab.active{{background:#38bdf8;color:#0f172a;border-color:#38bdf8}}
table{{width:100%;border-collapse:collapse;background:#1e293b;border-radius:8px;overflow:hidden}}
th{{background:#334155;padding:10px 14px;text-align:left;font-size:.8rem;text-transform:uppercase;color:#94a3b8}}
td{{padding:8px 14px;border-bottom:1px solid #1e293b;font-size:.9rem}}
tr:hover{{background:#334155}}
tr.rain td{{background:#1e3a5f}}
.api-box{{background:#1e293b;border-radius:8px;padding:16px;margin-top:24px;border:1px solid #334155}}
.api-box code{{color:#38bdf8;font-size:.85rem}}
.api-row{{display:flex;gap:8px;align-items:center;padding:4px 0}}
.method{{background:#334155;color:#22c55e;padding:2px 8px;border-radius:4px;font-size:.75rem;font-weight:700;min-width:45px;text-align:center}}
.method.post{{color:#f59e0b}}
.batch-btn{{background:#38bdf8;color:#0f172a;border:none;padding:10px 24px;border-radius:8px;cursor:pointer;font-weight:700;font-size:.9rem;margin:8px 0}}
.batch-btn:hover{{background:#0ea5e9}}
#batch-result{{color:#94a3b8;margin-top:8px;font-size:.85rem}}
</style></head><body>
<h1>Weather Data Warehouse Dashboard</h1>
<p class="sub">{sched_info} | {batch_html}</p>
<div class="cards">{stat_cards}</div>

<h2>Forecasts (from DW)</h2>
<div class="tabs">{date_tabs}</div>
{fc_divs}

<h2>Actual Data (recent)</h2>
<table><thead><tr><th>District</th><th>Date</th><th>Min°C</th><th>Max°C</th><th>Humidity</th><th>Rain</th></tr></thead>
<tbody>{actual_html}</tbody></table>

<h2>Admin</h2>
<button class="batch-btn" onclick="runBatch()">Run Batch Forecast</button>
<button class="batch-btn" onclick="checkStatus()" style="background:#334155;color:#e2e8f0">Check Status</button>
<div id="batch-result"></div>

<h2>API Endpoints</h2>
<div class="api-box">
<div class="api-row"><span class="method">GET</span><code><a href="/api/weather" style="color:#38bdf8">/api/weather</a></code> — All forecasts summary</div>
<div class="api-row"><span class="method">GET</span><code><a href="/api/weather/My_Khe_Beach" style="color:#38bdf8">/api/weather/{{district}}</a></code> — 1 district forecast</div>
<div class="api-row"><span class="method">GET</span><code>/api/weather/{{district}}/{{date}}</code> — 1 district, 1 date</div>
<div class="api-row"><span class="method">GET</span><code><a href="/api/districts" style="color:#38bdf8">/api/districts</a></code> — List districts</div>
<div class="api-row"><span class="method">GET</span><code>/api/actual/{{district}}/{{date}}</code> — Actual data</div>
<div class="api-row"><span class="method post">POST</span><code>/api/admin/run-forecast</code> — Trigger batch</div>
<div class="api-row"><span class="method">GET</span><code><a href="/api/admin/forecast-status" style="color:#38bdf8">/api/admin/forecast-status</a></code> — Batch status</div>
<div class="api-row"><span class="method">GET</span><code><a href="/api/health" style="color:#38bdf8">/api/health</a></code> — Health check</div>
<div class="api-row"><span class="method">GET</span><code><a href="/docs" style="color:#38bdf8">/docs</a></code> — Swagger UI</div>
</div>
<script>
function showDate(btn,d){{
  document.querySelectorAll('.date-table').forEach(e=>e.style.display='none');
  document.querySelectorAll('.tab').forEach(e=>e.classList.remove('active'));
  document.getElementById('dt-'+d).style.display='block';
  btn.classList.add('active');
}}
document.querySelector('.tab')?.classList.add('active');
async function runBatch(){{
  document.getElementById('batch-result').textContent='Starting...';
  const r=await fetch('/api/admin/run-forecast',{{method:'POST'}});
  const d=await r.json();
  document.getElementById('batch-result').textContent=d.message;
}}
async function checkStatus(){{
  const r=await fetch('/api/admin/forecast-status');
  const d=await r.json();
  let txt=`Status: ${{d.status}}`;
  if(d.success) txt+=` | ${{d.success}}/${{d.total_forecasts}} forecasts`;
  if(d.dates) txt+=` | Dates: ${{d.dates.join(', ')}}`;
  document.getElementById('batch-result').textContent=txt;
}}
</script></body></html>"""


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
