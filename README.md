# Weather Prediction System - Da Nang

Du bao thoi tiet theo gio cho 53 dia diem tai Da Nang, su dung **XGBoost** + **PostgreSQL Data Warehouse** + **FastAPI**.

## Muc luc

- [Kien truc tong quan](#kien-truc-tong-quan)
- [Cau truc project](#cau-truc-project)
- [Cai dat](#cai-dat)
- [Pipeline workflow](#pipeline-workflow)
- [Data Warehouse Schema](#data-warehouse-schema)
- [Feature Engineering](#feature-engineering)
- [Model Training](#model-training)
- [API Backend](#api-backend)
- [Huong dan su dung](#huong-dan-su-dung)

---

## Kien truc tong quan

```
                         ┌─────────────────────────────────────────┐
                         │              WORKFLOW                    │
                         │                                         │
  dataset_weather.csv ──►│  1. Setup DB    (setup_database.py)     │
   (1.4M rows)          │  2. ETL         (etl_pipeline.py)       │
   53 districts          │  3. Features    (feature_engineering.py) │
   2023-03 → 2026-03    │  4. Train       (train_weather_model.py) │
                         └──────────────────┬──────────────────────┘
                                            │
                    ┌───────────────────────┼────────────────────────┐
                    ▼                       ▼                        ▼
          ┌─────────────────┐    ┌───────────────────┐    ┌──────────────────┐
          │   PostgreSQL    │    │   XGBoost Models   │    │   FastAPI (BE)   │
          │   Data Warehouse│◄──►│   models/*.joblib  │◄──►│   api/main.py    │
          │                 │    │                    │    │                  │
          │  dw.*           │    │  temperature_c     │    │  GET /api/weather│
          │  features.*     │    │  humidity_percent  │    │  GET /api/actual │
          │  staging.*      │    │  wind_speed_m_s    │    │  POST /admin/... │
          └─────────────────┘    │  rain_classifier   │    └────────┬─────────┘
                                 │  rain_regressor    │             │
                                 └────────────────────┘             ▼
                                                              ┌──────────┐
                                                              │ Frontend │
                                                              │  (FE)   │
                                                              └──────────┘
```

### Luong du lieu tu dong

```
Moi ngay luc 00:05 (scheduler):
  1. Lay last_actual_date tu DW  (vi du: 2026-03-19)
  2. Predict ngay +1, +2         (2026-03-20, 2026-03-21)
  3. Predict cho TAT CA 53 districts
  4. Luu ket qua vao dw.fact_weather_forecast
  5. FE goi GET /api/weather de lay data

Ngay tiep theo:
  - Actual data moi duoc nap vao DW
  - Scheduler predict tiep 2 ngay moi
  - Forecast cu duoc ghi de boi forecast moi (chinh xac hon vi co them actual data)
```

---

## Cau truc project

```
train model/
│
├── config.py                    # Cau hinh DB, paths, constants
├── setup_database.py            # Tao database + DW schema (star schema)
├── etl_pipeline.py              # ETL: CSV → Staging → Dimensions → Fact
├── feature_engineering.py       # Tinh features, materialized view
├── train_weather_model.py       # Train XGBoost models
├── predict_weather.py           # Predict + luu forecast vao DW
├── workflow.py                  # Orchestrator chay toan bo pipeline
├── requirements.txt             # Python dependencies
├── README.md                    # Tai lieu nay
│
├── api/                         # Backend API (FastAPI)
│   ├── __init__.py
│   ├── main.py                  # FastAPI app + endpoints + dashboard
│   ├── database.py              # SQLAlchemy connection pool
│   ├── schemas.py               # Pydantic request/response models
│   └── scheduler.py             # APScheduler batch forecast
│
└── models/                      # Trained models (output)
    ├── xgb_temperature_c.joblib
    ├── xgb_humidity_percent.joblib
    ├── xgb_wind_speed_m_s.joblib
    ├── xgb_rain_classifier.joblib
    ├── xgb_rain_regressor.joblib
    └── feature_columns.joblib
```

---

## Cai dat

### Yeu cau

- Python 3.11+
- PostgreSQL 14+
- macOS / Linux

### Buoc 1: Cai dependencies

```bash
cd "train model"
pip install -r requirements.txt
```

### Buoc 2: Khoi dong PostgreSQL

```bash
brew services start postgresql@17
```

### Buoc 3: Chay full pipeline

```bash
python workflow.py
```

Pipeline se tu dong:
1. Tao database `weather_dw` va tat ca tables
2. Load 1.4M rows tu CSV vao DW
3. Tinh features (daily aggregates, historical same-day, hourly historical)
4. Train 5 XGBoost models

### Buoc 4: Chay API server

```bash
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000
```

---

## Pipeline workflow

### workflow.py

| Step | Module | Thoi gian | Mo ta |
|------|--------|-----------|-------|
| 1/4 | `setup_database.py` | ~1s | Tao DB `weather_dw`, 3 schemas, dimension tables, fact tables, indexes |
| 2/4 | `etl_pipeline.py` | ~77s | Extract CSV → staging, Transform dimensions, Load fact table |
| 3/4 | `feature_engineering.py` | ~50s | Tinh daily/hourly aggregates, historical same-day, materialized view |
| 4/4 | `train_weather_model.py` | ~67s | Train 5 models (temp, humidity, wind, rain_clf, rain_reg) |

**Tong: ~180s** cho full pipeline.

### Chay tung buoc rieng le

```bash
python workflow.py --step setup
python workflow.py --step etl
python workflow.py --step features
python workflow.py --step train
```

---

## Data Warehouse Schema

**Database:** `weather_dw` | **Engine:** PostgreSQL 17

### Tong quan schemas

| Schema | Vai tro | So table |
|--------|---------|----------|
| `staging` | Vung dem chua raw data tu CSV | 1 |
| `dw` | Data Warehouse chinh (Star Schema) | 5 |
| `features` | Feature Store cho ML | 3 + 1 materialized view |

---

### Schema: `staging`

#### `staging.raw_weather` (1,399,200 rows)

Vung dem nhan data tu CSV truoc khi transform.

| Column | Type | Mo ta |
|--------|------|-------|
| district | VARCHAR(100) | Ten dia diem |
| datetime | TIMESTAMP | Thoi gian do |
| temperature_c | REAL | Nhiet do (°C) |
| humidity_percent | REAL | Do am (%) |
| wind_speed_m_s | REAL | Toc do gio (m/s) |
| rain_mm | REAL | Luong mua (mm) |

---

### Schema: `dw` (Star Schema)

```
                        ┌──────────────────┐
                        │  dim_district    │
                        │  PK: district_id │
                        │  district_name   │
                        │  (53 rows)       │
                        └────────┬─────────┘
                                 │
  ┌──────────────────┐  ┌───────┴────────────────────┐  ┌──────────────────┐
  │   dim_date       │  │  fact_weather_hourly       │  │   dim_hour       │
  │   PK: date_id    ├──┤  PK: weather_id            ├──┤  PK: hour_id     │
  │   full_date      │  │  FK: district_id           │  │  hour            │
  │   year, month,   │  │  FK: date_id               │  │  period_of_day   │
  │   day, day_of_   │  │  FK: hour_id               │  │  (24 rows)       │
  │   year, quarter  │  │  temperature_c             │  └──────────────────┘
  │   month_day      │  │  humidity_percent          │
  │   is_weekend     │  │  wind_speed_m_s            │
  │   (1,103 rows)   │  │  rain_mm                   │
  └──────────────────┘  │  (1,399,200 rows)          │
                        └───────┬────────────────────┘
                                │
                        ┌───────┴────────────────────┐
                        │  fact_weather_forecast     │
                        │  PK: forecast_id           │
                        │  FK: district_id           │
                        │  FK: date_id, hour_id      │
                        │  temperature_c             │
                        │  humidity_percent          │
                        │  wind_speed_m_s            │
                        │  rain_mm                   │
                        │  rain_probability          │
                        │  predicted_at              │
                        │  (2,592 rows)              │
                        └────────────────────────────┘
```

#### `dw.dim_district` (53 rows)

| Column | Type | PK | Mo ta |
|--------|------|-----|-------|
| district_id | SERIAL | PK | ID tu tang |
| district_name | VARCHAR(100) | UNIQUE | Ten dia diem (vd: My_Khe_Beach, Ba_Na_Hills) |

#### `dw.dim_date` (1,103 rows)

| Column | Type | PK | Mo ta |
|--------|------|-----|-------|
| date_id | SERIAL | PK | ID tu tang |
| full_date | DATE | UNIQUE | Ngay day du (2023-03-16 → 2026-03-22) |
| year | SMALLINT | | Nam |
| month | SMALLINT | | Thang |
| day | SMALLINT | | Ngay |
| day_of_year | SMALLINT | | Ngay thu may trong nam (1-366) |
| day_of_week | SMALLINT | | Thu trong tuan (1=Mon, 7=Sun) |
| week_of_year | SMALLINT | | Tuan thu may |
| quarter | SMALLINT | | Quy (1-4) |
| month_name | VARCHAR(20) | | Ten thang (January, February, ...) |
| is_weekend | BOOLEAN | | Co phai cuoi tuan? |
| month_day | SMALLINT | | month*100+day, dung de match cung ngay qua cac nam |

#### `dw.dim_hour` (24 rows)

| Column | Type | PK | Mo ta |
|--------|------|-----|-------|
| hour_id | SMALLINT | PK | 0-23 |
| hour | SMALLINT | | Gio |
| period_of_day | VARCHAR(20) | | Night (0-5), Morning (6-11), Afternoon (12-17), Evening (18-23) |

#### `dw.fact_weather_hourly` (1,399,200 rows) - DU LIEU THUC TE

Fact table chinh chua do luong thoi tiet thuc te theo gio.

| Column | Type | FK | Mo ta |
|--------|------|-----|-------|
| weather_id | SERIAL | PK | |
| district_id | INT | → dim_district | Dia diem |
| date_id | INT | → dim_date | Ngay |
| hour_id | SMALLINT | → dim_hour | Gio |
| temperature_c | REAL | | Nhiet do (°C) |
| humidity_percent | REAL | | Do am (%) |
| wind_speed_m_s | REAL | | Toc do gio (m/s) |
| rain_mm | REAL | | Luong mua (mm) |

**Indexes:**
- `idx_fact_district` (district_id)
- `idx_fact_date` (date_id)
- `idx_fact_district_date` (district_id, date_id)

#### `dw.fact_weather_forecast` (2,592 rows) - KET QUA DU BAO

Luu ket qua du bao tu model. Moi lan predict, data moi ghi de data cu cung district+date.

| Column | Type | FK | Mo ta |
|--------|------|-----|-------|
| forecast_id | SERIAL | PK | |
| district_id | INT | → dim_district | Dia diem |
| date_id | INT | → dim_date | Ngay du bao |
| hour_id | SMALLINT | → dim_hour | Gio |
| temperature_c | REAL | | Nhiet do du bao (°C) |
| humidity_percent | REAL | | Do am du bao (%) |
| wind_speed_m_s | REAL | | Gio du bao (m/s) |
| rain_mm | REAL | | Mua du bao (mm) |
| rain_probability | REAL | | Xac suat mua (0-100%) |
| predicted_at | TIMESTAMP | | Thoi diem chay predict |

**UNIQUE constraint:** (district_id, date_id, hour_id)
**Indexes:** idx_forecast_district, idx_forecast_date, idx_forecast_district_date

---

### Schema: `features` (Feature Store)

#### `features.agg_daily_weather` (58,300 rows)

Thong ke ngay cho moi district. Duoc tinh tu `fact_weather_hourly`.

| Column | Type | Mo ta |
|--------|------|-------|
| district_id | INT | PK (composite) |
| date_id | INT | PK (composite) |
| year | SMALLINT | Nam |
| month_day | SMALLINT | month*100+day |
| temp_mean, temp_std, temp_min, temp_max | REAL | Thong ke nhiet do trong ngay |
| hum_mean, hum_std, hum_min, hum_max | REAL | Thong ke do am |
| wind_mean, wind_std, wind_min, wind_max | REAL | Thong ke gio |
| rain_mean, rain_std, rain_min, rain_max | REAL | Thong ke mua |
| rain_total | REAL | Tong luong mua ca ngay |

#### `features.agg_historical_same_day` (38,902 rows)

Thong ke CUNG NGAY/THANG tu cac NAM TRUOC. Vd: de predict 20/3/2026, lay trung binh cua 20/3/2023, 20/3/2024, 20/3/2025.

| Column | Type | Mo ta |
|--------|------|-------|
| district_id | INT | PK |
| year | SMALLINT | PK - nam hien tai |
| month_day | SMALLINT | PK - ngay can predict |
| hist_temp_mean, hist_temp_std, hist_temp_min, hist_temp_max | REAL | TB nhiet do cung ngay cac nam truoc |
| hist_hum_mean, hist_hum_std, hist_hum_min, hist_hum_max | REAL | TB do am |
| hist_wind_mean, hist_wind_std, hist_wind_min, hist_wind_max | REAL | TB gio |
| hist_rain_mean, hist_rain_std, hist_rain_min, hist_rain_max | REAL | TB mua |
| years_count | SMALLINT | So nam co data |

#### `features.agg_historical_same_day_hour` (933,648 rows)

Giong `agg_historical_same_day` nhung chi tiet theo TUNG GIO. Giup model hoc pattern gio (vd: 6h sang luon mat hon 14h chieu).

| Column | Type | Mo ta |
|--------|------|-------|
| district_id | INT | PK |
| year | SMALLINT | PK |
| month_day | SMALLINT | PK |
| hour | SMALLINT | PK - gio cu the |
| hist_h_temp_mean, hist_h_temp_std | REAL | TB nhiet do cung ngay+gio cac nam truoc |
| hist_h_hum_mean, hist_h_hum_std | REAL | TB do am |
| hist_h_wind_mean, hist_h_wind_std | REAL | TB gio |
| hist_h_rain_mean, hist_h_rain_max | REAL | TB mua |
| hist_h_rain_prob | REAL | Xac suat mua (0.0-1.0) |
| years_count | SMALLINT | So nam co data |

#### `features.mv_training_features` (Materialized View, 1,399,200 rows)

Ket hop TAT CA features thanh 1 view duy nhat phuc vu training. Join tu fact + dim + features bang SQL.

Bao gom: temporal features + historical daily + historical hourly + lag 1-7 ngay + rain flags.

---

## Feature Engineering

### Tong cong: 86 features

| Nhom | So luong | Mo ta |
|------|----------|-------|
| **Temporal** | 12 | district_id, month, day, day_of_year, day_of_week, hour, hour_sin/cos, doy_sin/cos, month_sin/cos |
| **Historical daily** | 17 | hist_temp/hum/wind/rain (mean, std, min, max) + years_count |
| **Historical hourly** | 9 | hist_h_temp/hum/wind (mean, std) + rain (mean, max, prob) |
| **Lag features** | 28 | Cung gio, 1-7 ngay truoc: temp, hum, wind, rain |
| **Rain flags** | 3 | lag_1/2/3d_is_rainy (0/1) |
| **Rolling stats** | 17 | rolling_3d/7d_mean, rolling_3d_std, trend_3d + recent_rain_count |

### Logic lay lag data (predict_weather.py)

Khi predict ngay D, lay lag tu 2 nguon:

```
Priority:  actual (fact_weather_hourly)  >  forecast (fact_weather_forecast)

Vd: predict 22/3/2026
  lag 1 ngay (21/3) → lay tu fact_weather_forecast (da predict truoc do)
  lag 2 ngay (20/3) → lay tu fact_weather_forecast
  lag 3 ngay (19/3) → lay tu fact_weather_hourly (actual data)
  lag 4 ngay (18/3) → lay tu fact_weather_hourly
  ...
```

SQL dung `UNION ALL` + `ROW_NUMBER() OVER (PARTITION BY full_date ORDER BY source)` de uu tien actual.

---

## Model Training

### train_weather_model.py

#### Models

| Model | Target | Algorithm | Mo ta |
|-------|--------|-----------|-------|
| `xgb_temperature_c.joblib` | temperature_c | XGBRegressor | Nhiet do |
| `xgb_humidity_percent.joblib` | humidity_percent | XGBRegressor | Do am |
| `xgb_wind_speed_m_s.joblib` | wind_speed_m_s | XGBRegressor | Toc do gio |
| `xgb_rain_classifier.joblib` | is_rainy (0/1) | XGBClassifier | Stage 1: Co mua hay khong? |
| `xgb_rain_regressor.joblib` | rain_mm | XGBRegressor | Stage 2: Luong mua (chi train tren mau co mua) |

#### Two-stage rain model

```
Input features
    │
    ├──► Rain Classifier (XGBClassifier)
    │        │
    │        ├── P(rain) <= 0.5  →  rain_mm = 0
    │        │
    │        └── P(rain) > 0.5   →  Rain Regressor (XGBRegressor) → rain_mm
    │
    └──► Output: rain_mm + rain_probability
```

#### Hyperparameters

```python
XGBRegressor(
    n_estimators=600, max_depth=9, learning_rate=0.05,
    subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
    reg_alpha=0.1, reg_lambda=1.0, tree_method="hist"
)
```

#### Sample weighting

Data gan day duoc uu tien hon (weight 1.0 → 3.0 theo nam):

```python
weight = 1.0 + ((year - min_year) / (max_year - min_year)) * 2.0
```

#### Evaluation (train 85% / test 15%)

| Target | MAE | RMSE | R² |
|--------|-----|------|-----|
| temperature_c | 1.01 °C | 1.29 °C | 0.838 |
| humidity_percent | 4.13 % | 5.45 % | 0.738 |
| wind_speed_m_s | 2.63 m/s | 3.41 m/s | 0.447 |
| rain_mm (combined) | 0.72 mm | 2.12 mm | - |

---

## API Backend

### api/main.py (FastAPI v3)

**Server:** `http://localhost:8000`
**Swagger:** `http://localhost:8000/docs`
**Dashboard:** `http://localhost:8000/`

### Endpoints cho Frontend

| Method | Endpoint | Response | Mo ta |
|--------|----------|----------|-------|
| GET | `/api/weather` | JSON array | Summary forecast TAT CA districts, tat ca ngay da predict |
| GET | `/api/weather/{district}` | JSON | Hourly forecast 1 district, tat ca ngay da predict |
| GET | `/api/weather/{district}/{date}` | JSON | Hourly forecast 1 district, 1 ngay |
| GET | `/api/districts` | JSON array | Danh sach 53 dia diem |
| GET | `/api/actual/{district}/{date}` | JSON | Data thuc te tu DW |

### Endpoints Admin

| Method | Endpoint | Mo ta |
|--------|----------|-------|
| POST | `/api/admin/run-forecast` | Trigger batch predict thu cong (chay background) |
| GET | `/api/admin/forecast-status` | Xem trang thai/ket qua batch cuoi cung |
| GET | `/api/health` | Health check + scheduler status + next run time |

### Auto Scheduler

- **Lib:** APScheduler (BackgroundScheduler)
- **Schedule:** Moi ngay luc 00:05
- **Batch:** Predict 2 ngay tiep theo cho 53 districts = 106 forecasts
- **Thoi gian:** ~30s cho 1 batch

### Vi du response

**GET /api/weather/Ba_Na_Hills/2026-03-20**

```json
{
  "district": "Ba_Na_Hills",
  "date": "2026-03-20",
  "hourly": [
    {
      "hour": 0,
      "temperature_c": 13.8,
      "humidity_percent": 91.9,
      "wind_speed_m_s": 7.4,
      "rain_mm": 0.0,
      "rain_probability": 4.7,
      "description": "Mat me, do am cao"
    },
    ...
  ],
  "summary": {
    "temp_min": 13.8,
    "temp_max": 22.6,
    "temp_avg": 17.3,
    "humidity_avg": 81.0,
    "rain_total": 0.0,
    "wind_max": 18.8
  }
}
```

---

## Huong dan su dung

### Chay lan dau (full pipeline + API)

```bash
# 1. Chay full pipeline (tao DB, ETL, features, train)
python workflow.py

# 2. Start API server
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000

# 3. Trigger batch forecast cho tat ca districts
curl -X POST "http://localhost:8000/api/admin/run-forecast"

# 4. Mo browser xem data
open http://localhost:8000/docs
```

### Predict tu command line

```bash
# 1 ngay, 1 district
python predict_weather.py My_Khe_Beach 2026-03-20

# 7 ngay lien tiep (chained)
python predict_weather.py Ba_Na_Hills 2026-03-20 7
```

### Xem data trong PostgreSQL (pgAdmin)

1. Mo pgAdmin, connect toi `localhost:5432`
2. Database: `weather_dw`
3. Schemas: `dw` → Tables → click phai → View/Edit Data
4. Hoac mo Query Tool va chay SQL

```sql
-- Xem forecast
SELECT dd.district_name, dt.full_date, dh.hour,
       fc.temperature_c, fc.humidity_percent, fc.rain_mm, fc.rain_probability
FROM dw.fact_weather_forecast fc
JOIN dw.dim_district dd ON dd.district_id = fc.district_id
JOIN dw.dim_date dt ON dt.date_id = fc.date_id
JOIN dw.dim_hour dh ON dh.hour_id = fc.hour_id
ORDER BY dd.district_name, dt.full_date, dh.hour;
```

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.13 |
| Database | PostgreSQL 17 |
| DW Schema | Star Schema (Kimball) |
| ML Model | XGBoost 3.2 |
| API Framework | FastAPI 0.135 |
| Scheduler | APScheduler 3.11 |
| ORM | SQLAlchemy 2.0 |
| DB Driver | psycopg2 |
