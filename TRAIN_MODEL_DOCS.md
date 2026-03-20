# Train Model Documentation

Tai lieu chi tiet ve quy trinh train model du bao thoi tiet Da Nang.

## Muc luc

- [1. Tong quan](#1-tong-quan)
- [2. Du lieu dau vao](#2-du-lieu-dau-vao)
- [3. Feature Engineering](#3-feature-engineering)
  - [3.1 Temporal features](#31-temporal-features)
  - [3.2 Historical same-day features](#32-historical-same-day-features)
  - [3.3 Historical same-day-hour features](#33-historical-same-day-hour-features)
  - [3.4 Lag features (nearby days)](#34-lag-features-nearby-days)
  - [3.5 Rolling & trend features](#35-rolling--trend-features)
  - [3.6 Rain flags](#36-rain-flags)
  - [3.7 Materialized view](#37-materialized-view)
- [4. Data split & Sample weighting](#4-data-split--sample-weighting)
- [5. Models](#5-models)
  - [5.1 Temperature model](#51-temperature-model)
  - [5.2 Humidity model](#52-humidity-model)
  - [5.3 Wind speed model](#53-wind-speed-model)
  - [5.4 Rain model (Two-stage)](#54-rain-model-two-stage)
- [6. Evaluation](#6-evaluation)
- [7. Prediction flow](#7-prediction-flow)
- [8. Cac function chinh](#8-cac-function-chinh)
- [9. Model artifacts](#9-model-artifacts)
- [10. Cach retrain](#10-cach-retrain)

---

## 1. Tong quan

```
┌──────────────────────┐     ┌────────────────────────┐     ┌───────────────────┐
│  PostgreSQL DW       │     │  feature_engineering.py │     │ train_weather_    │
│                      │     │                        │     │ model.py          │
│  fact_weather_hourly │────►│  compute_daily_agg()   │────►│                   │
│  dim_district        │     │  compute_hist_same_day │     │  load_training()  │
│  dim_date            │     │  compute_hist_hour()   │     │  add_rolling()    │
│  dim_hour            │     │  create_mat_view()     │     │  train()          │
│                      │     │                        │     │                   │
│  1,399,200 rows      │     │  mv_training_features  │     │  5 XGBoost models │
│  53 districts        │     │  86 features           │     │  → models/*.joblib│
│  1,100 days          │     │  933K+ rows            │     │                   │
└──────────────────────┘     └────────────────────────┘     └───────────────────┘
```

**Bai toan:** Time-series regression voi yeu to seasonal
- Input: 86 features (temporal + historical + lag)
- Output: 4 targets (nhiet do, do am, gio, mua) cho tung gio
- Dac diem: du doan dua tren **cung ngay cac nam truoc** + **cac ngay gan nhat**

---

## 2. Du lieu dau vao

### Dataset goc: `dataset_weather.csv`

| Thong so | Gia tri |
|----------|---------|
| Tong rows | 1,399,200 |
| Districts | 53 dia diem Da Nang |
| Khoang thoi gian | 2023-03-16 → 2026-03-19 |
| Tan suat | Moi gio (24 rows/ngay/district) |
| So ngay | ~1,100 ngay |

### 6 columns goc

| Column | Type | Don vi | Vi du |
|--------|------|--------|-------|
| district | string | | My_Khe_Beach, Ba_Na_Hills |
| datetime | timestamp | | 2025-06-15 14:00 |
| temperature_C | float | °C | 27.3 |
| humidity_percent | float | % | 78 |
| wind_speed_m_s | float | m/s | 12.5 |
| rain_mm | float | mm | 0.2 |

### 4 targets can predict

| Target | Mo ta | Phan bo |
|--------|-------|---------|
| temperature_c | Nhiet do | 13-39°C, phan phoi gan normal |
| humidity_percent | Do am | 30-100%, lech phai |
| wind_speed_m_s | Toc do gio | 0-30 m/s, lech phai manh |
| rain_mm | Luong mua | 0-50mm, ~75% gia tri = 0 (rat sparse) |

---

## 3. Feature Engineering

**File:** `feature_engineering.py`
**Tong so features:** 86

### 3.1 Temporal features (12 features)

Encode thong tin thoi gian, su dung sin/cos cho tinh tuan hoan.

| Feature | Cong thuc | Muc dich |
|---------|-----------|----------|
| district_id | ID tu dim_district | Phan biet vi tri dia ly |
| month | 1-12 | Mua trong nam |
| day | 1-31 | Ngay trong thang |
| day_of_year | 1-366 | Vi tri trong nam |
| day_of_week | 1-7 (Mon-Sun) | Ngay trong tuan |
| hour | 0-23 | Gio trong ngay |
| hour_sin | sin(2π × hour / 24) | Tuan hoan gio |
| hour_cos | cos(2π × hour / 24) | Tuan hoan gio |
| doy_sin | sin(2π × day_of_year / 365.25) | Tuan hoan nam |
| doy_cos | cos(2π × day_of_year / 365.25) | Tuan hoan nam |
| month_sin | sin(2π × month / 12) | Tuan hoan thang |
| month_cos | cos(2π × month / 12) | Tuan hoan thang |

**Tai sao dung sin/cos?**
Gio 23 va gio 0 thuc ra gan nhau, nhung neu dung raw value (23 vs 0) model khong hieu.
Sin/cos encode bao toan tinh lien tuc tuan hoan:
- hour=0: sin=0, cos=1
- hour=6: sin=1, cos=0
- hour=12: sin=0, cos=-1
- hour=23: sin≈-0.26, cos≈0.97 (gan voi hour=0)

### 3.2 Historical same-day features (17 features)

**Table:** `features.agg_historical_same_day`
**Logic:** De predict ngay 20/3/2026, lay thong ke cua ngay 20/3 tu **tat ca nam truoc** (2023, 2024, 2025).

```
Predict 20/3/2026:
  ┌─ 20/3/2023: temp_mean=24.1, temp_std=2.3, rain_total=0.5
  ├─ 20/3/2024: temp_mean=25.3, temp_std=1.8, rain_total=0.0
  └─ 20/3/2025: temp_mean=23.8, temp_std=2.1, rain_total=12.3
       │
       ▼
  hist_temp_mean = AVG(24.1, 25.3, 23.8) = 24.4
  hist_temp_min  = MIN(...)
  hist_temp_max  = MAX(...)
  years_count    = 3
```

| Feature | Mo ta |
|---------|-------|
| hist_temp_mean, hist_temp_std, hist_temp_min, hist_temp_max | TB nhiet do cung ngay cac nam truoc |
| hist_hum_mean, hist_hum_std, hist_hum_min, hist_hum_max | TB do am |
| hist_wind_mean, hist_wind_std, hist_wind_min, hist_wind_max | TB gio |
| hist_rain_mean, hist_rain_std, hist_rain_min, hist_rain_max | TB mua |
| years_count | So nam co data (1-3) |

**SQL:**

```sql
-- Tu agg_daily_weather, lay cac nam truoc cung month_day
SELECT cur.district_id, cur.year, cur.month_day,
       AVG(prev.temp_mean), MIN(prev.temp_min), MAX(prev.temp_max), ...
FROM features.agg_daily_weather cur
JOIN features.agg_daily_weather prev
  ON prev.district_id = cur.district_id
  AND prev.month_day = cur.month_day    -- cung ngay/thang
  AND prev.year < cur.year              -- chi lay nam TRUOC
GROUP BY cur.district_id, cur.year, cur.month_day
```

### 3.3 Historical same-day-hour features (9 features)

**Table:** `features.agg_historical_same_day_hour`
**Logic:** Giong 3.2 nhung chi tiet theo **tung gio**. Giup model hoc pattern: 6h sang luon mat hon 14h chieu.

| Feature | Mo ta |
|---------|-------|
| hist_h_temp_mean, hist_h_temp_std | Nhiet do TB cung ngay + cung gio cac nam truoc |
| hist_h_hum_mean, hist_h_hum_std | Do am |
| hist_h_wind_mean, hist_h_wind_std | Gio |
| hist_h_rain_mean, hist_h_rain_max | Mua |
| hist_h_rain_prob | Xac suat mua (0.0-1.0) = AVG(CASE WHEN rain>0.1 THEN 1 ELSE 0) |

**Vi du:** Predict 20/3/2026 luc 14:00

```
  20/3/2023 14:00: temp=28.5, rain=0.0
  20/3/2024 14:00: temp=30.1, rain=0.0
  20/3/2025 14:00: temp=27.8, rain=1.2
       │
       ▼
  hist_h_temp_mean = 28.8
  hist_h_rain_prob = 0.33  (1/3 nam co mua)
```

### 3.4 Lag features - nearby days (28 features)

**Logic:** Lay gia tri thuc te cung gio, cung district, tu 1-7 ngay truoc.
Khi predict, uu tien actual data, fallback sang forecast da luu trong DW.

| Feature | Mo ta |
|---------|-------|
| lag_1d_temp, lag_1d_hum, lag_1d_wind, lag_1d_rain | Hom qua, cung gio |
| lag_2d_temp, lag_2d_hum, lag_2d_wind, lag_2d_rain | 2 ngay truoc |
| lag_3d_* | 3 ngay truoc |
| lag_4d_* | 4 ngay truoc |
| lag_5d_* | 5 ngay truoc |
| lag_6d_* | 6 ngay truoc |
| lag_7d_* | 7 ngay truoc |

**SQL (training - materialized view):**

```sql
LEFT JOIN dw.fact_weather_hourly lag1
  ON lag1.district_id = f.district_id
  AND lag1.hour_id = f.hour_id
  AND lag1.date_id = (
    SELECT date_id FROM dw.dim_date
    WHERE full_date = dt.full_date - INTERVAL '1 day'
  )
-- ... tuong tu cho lag2 → lag7
```

**SQL (prediction - actual + forecast fallback):**

```sql
WITH actual AS (
  SELECT ..., 'actual' AS source FROM dw.fact_weather_hourly ...
),
forecast AS (
  SELECT ..., 'forecast' AS source FROM dw.fact_weather_forecast ...
),
combined AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY full_date
    ORDER BY CASE source WHEN 'actual' THEN 0 ELSE 1 END  -- uu tien actual
  ) AS rn
  FROM (SELECT * FROM actual UNION ALL SELECT * FROM forecast)
)
SELECT * FROM combined WHERE rn = 1
```

### 3.5 Rolling & trend features (16 features)

Tinh tu lag features (trong Python, sau khi load tu PostgreSQL).

| Feature | Cong thuc | Mo ta |
|---------|-----------|-------|
| rolling_3d_{var}_mean | mean(lag_1d, lag_2d, lag_3d) | Trung binh 3 ngay gan nhat |
| rolling_7d_{var}_mean | mean(lag_1d ... lag_7d) | Trung binh 7 ngay |
| rolling_3d_{var}_std | std(lag_1d, lag_2d, lag_3d) | Bien dong 3 ngay |
| trend_3d_{var} | lag_1d - lag_3d | Xu huong tang/giam |

`{var}` = temp, hum, wind, rain → 4 × 4 = **16 features**

**Vi du:** trend_3d_temp = 25°C (hom qua) - 22°C (3 ngay truoc) = +3°C → dang am len

### 3.6 Rain flags (4 features)

| Feature | Mo ta |
|---------|-------|
| lag_1d_is_rainy | 1 neu hom qua mua (rain > 0.1mm), 0 neu khong |
| lag_2d_is_rainy | 2 ngay truoc |
| lag_3d_is_rainy | 3 ngay truoc |
| recent_rain_count | Tong so ngay mua trong 3 ngay gan nhat (0-3) |

Cac features nay dac biet quan trong cho **Rain Classifier** (Stage 1).

### 3.7 Materialized view

**`features.mv_training_features`** - Ket hop TAT CA features thanh 1 view.

```
fact_weather_hourly
  JOIN dim_district, dim_date, dim_hour                 → temporal
  LEFT JOIN agg_historical_same_day                     → historical daily
  LEFT JOIN agg_historical_same_day_hour                → historical hourly
  LEFT JOIN fact_weather_hourly AS lag1 ... lag7         → lag features
  + CASE WHEN rain > 0.1 THEN 1 ELSE 0 END             → rain flags
  + sin/cos cyclical encoding                           → temporal encoding
```

**Output:** 1,399,200 rows × ~70 columns (+ rolling features tinh trong Python)

---

## 4. Data split & Sample weighting

### Time-based split (khong random!)

```
|←───────────── 85% TRAIN ──────────────►|←── 15% TEST ──►|
2023-03 ─────────────────────────────── ~2025-10 ────── 2026-03

Quan trong: KHONG dung random split vi day la time-series.
Data tuong lai khong duoc lot vao training set.
```

- Total samples (sau drop NaN): **469,368**
- Train: ~399K rows
- Test: ~70K rows
- NaN xay ra o nam dau tien (khong co historical data) va 7 ngay dau (khong co lag)

### Sample weighting

Data nam gan day duoc uu tien hon de model thich ung voi khi hau hien tai.

```python
weight = 1.0 + ((year - 2023) / (2026 - 2023)) * 2.0

  2023 → weight = 1.0
  2024 → weight = 1.67
  2025 → weight = 2.33
  2026 → weight = 3.0
```

---

## 5. Models

### 5.1 Temperature model

**File output:** `models/xgb_temperature_c.joblib`

| Thong so | Gia tri |
|----------|---------|
| Algorithm | XGBRegressor |
| n_estimators | 600 |
| max_depth | 9 |
| learning_rate | 0.05 |
| subsample | 0.8 |
| colsample_bytree | 0.8 |
| min_child_weight | 5 |
| reg_alpha | 0.1 |
| reg_lambda | 1.0 |
| tree_method | hist |

| Metric | Gia tri |
|--------|---------|
| MAE | 1.01 °C |
| RMSE | 1.29 °C |
| R² | 0.838 |

### 5.2 Humidity model

**File output:** `models/xgb_humidity_percent.joblib`

Cung hyperparameters nhu temperature. Post-processing: `clip(0, 100)`.

| Metric | Gia tri |
|--------|---------|
| MAE | 4.13 % |
| RMSE | 5.45 % |
| R² | 0.738 |

### 5.3 Wind speed model

**File output:** `models/xgb_wind_speed_m_s.joblib`

Cung hyperparameters. Gio kho predict nhat do bien dong cao.

| Metric | Gia tri |
|--------|---------|
| MAE | 2.63 m/s |
| RMSE | 3.41 m/s |
| R² | 0.447 |

### 5.4 Rain model (Two-stage)

Mua rat kho predict vi ~75% gia tri = 0 (imbalanced). Giai phap: tach thanh 2 buoc.

```
                    Input (86 features)
                          │
                ┌─────────┴──────────┐
                ▼                    │
    ┌───────────────────┐            │
    │  STAGE 1          │            │
    │  Rain Classifier  │            │
    │  XGBClassifier    │            │
    │                   │            │
    │  "Co mua khong?"  │            │
    └────────┬──────────┘            │
             │                       │
    ┌────────┴────────┐              │
    │                 │              │
    ▼                 ▼              │
  P ≤ 0.5          P > 0.5          │
  rain = 0mm         │              │
                      ▼              ▼
              ┌───────────────────────┐
              │  STAGE 2              │
              │  Rain Regressor       │
              │  XGBRegressor         │
              │                       │
              │  "Mua bao nhieu mm?"  │
              │  (chi train tren mau  │
              │   thuc su co mua)     │
              └───────────┬───────────┘
                          │
                          ▼
                    rain_mm = max(prediction, 0)
```

#### Stage 1: Rain Classifier

**File output:** `models/xgb_rain_classifier.joblib`

| Thong so | Gia tri |
|----------|---------|
| Algorithm | XGBClassifier |
| n_estimators | 400 |
| max_depth | 7 |
| scale_pos_weight | n_neg / n_pos (tu dong can bang class) |
| eval_metric | logloss |
| Threshold | rain_mm > 0.1 → label = 1 (co mua) |

| Metric | Gia tri |
|--------|---------|
| Accuracy | 0.663 |
| F1 Score | 0.265 |
| Rain ratio (train) | 25.0% |
| Rain ratio (test) | 21.9% |

#### Stage 2: Rain Regressor

**File output:** `models/xgb_rain_regressor.joblib`

| Thong so | Gia tri |
|----------|---------|
| Algorithm | XGBRegressor |
| n_estimators | 400 |
| max_depth | 7 |
| Training data | Chi cac rows co rain_mm > 0.1 |
| Post-processing | max(prediction, 0) |

#### Combined result

| Metric | Gia tri |
|--------|---------|
| MAE | 0.72 mm |
| RMSE | 2.12 mm |

**Tai sao two-stage tot hon single model?**

| Approach | Predict cho ngay khong mua |
|----------|---------------------------|
| Single XGBRegressor | Predict 3-4mm (sai hoan toan) |
| Two-stage (clf + reg) | Predict 0mm (chinh xac) |

---

## 6. Evaluation

### Tong hop ket qua

| Target | MAE | RMSE | R² | Danh gia |
|--------|-----|------|-----|----------|
| temperature_c | 1.01 °C | 1.29 °C | 0.838 | Tot - sai ~1°C |
| humidity_percent | 4.13 % | 5.45 % | 0.738 | Kha - sai ~4% |
| wind_speed_m_s | 2.63 m/s | 3.41 m/s | 0.447 | Trung binh - gio bien dong nhieu |
| rain_mm | 0.72 mm | 2.12 mm | - | Two-stage fix van de mua |

### So sanh voi actual data (Ngu_Hanh_Son 2026-03-20)

| Metric | Model predict | Actual (API) | Sai so |
|--------|--------------|--------------|--------|
| Temp 06:00 | 20.5°C | 19.0°C | +1.5°C |
| Temp 12:00 | 29.2°C | 27.6°C | +1.6°C |
| Temp TB ngay | 24.1°C | 23.4°C | +0.7°C |
| Rain tong ngay | 0.0mm | 0.0mm | chinh xac |

### Han che

- **Temperature ban dem:** model predict cao hon thuc te 1-2°C
- **Wind ban dem:** predict cao hon thuc te (do gio bien dong lon)
- **Rain:** classifier F1 con thap (0.265), kho phan biet mua nhe
- **Need more data:** chi co 3 nam data, can nhieu hon de historical features chinh xac

---

## 7. Prediction flow

### predict_weather.py

```
predict("Ngu_Hanh_Son", "2026-03-20")
  │
  ├── 1. load_artifacts()
  │       Load 5 models + feature_columns tu models/
  │
  ├── 2. Connect PostgreSQL
  │       fetch_district_id()
  │       fetch_historical_same_day()          → tu features.agg_historical_same_day
  │
  ├── 3. For hour = 0 → 23:
  │       fetch_historical_same_day_hour()     → tu features.agg_historical_same_day_hour
  │       fetch_lag_data()                     → tu fact_weather_hourly UNION fact_weather_forecast
  │       build_feature_row()                  → 86 features
  │
  ├── 4. Model inference
  │       temperature  = xgb_temperature_c.predict(X)
  │       humidity     = xgb_humidity_percent.predict(X) → clip(0,100)
  │       wind         = xgb_wind_speed_m_s.predict(X)
  │       rain_prob    = rain_classifier.predict_proba(X)[:,1]
  │       rain_mm      = rain_regressor.predict(X[prob>0.5]) if any
  │
  └── 5. save_forecast_to_dw()
          INSERT INTO dw.fact_weather_forecast (upsert)
```

### Chained prediction (nhieu ngay)

```
predict_range("My_Khe_Beach", "2026-03-20", num_days=3)

  Day 1: predict 20/3 → lag tu actual (13-19/3) → save to DW
  Day 2: predict 21/3 → lag tu actual (14-19/3) + forecast (20/3) → save to DW
  Day 3: predict 22/3 → lag tu actual (15-19/3) + forecast (20-21/3) → save to DW
```

---

## 8. Cac function chinh

### feature_engineering.py

| Function | Input | Output | Mo ta |
|----------|-------|--------|-------|
| `create_extra_tables(engine)` | engine | Table created | Tao bang agg_historical_same_day_hour |
| `compute_daily_aggregates(engine)` | fact_weather_hourly | 58,300 rows → agg_daily_weather | Thong ke ngay (mean,std,min,max) |
| `compute_historical_same_day(engine)` | agg_daily_weather | 38,902 rows → agg_historical_same_day | Cung ngay qua cac nam |
| `compute_historical_same_day_hour(engine)` | fact_weather_hourly + dim_date | 933,648 rows → agg_historical_same_day_hour | Cung ngay + cung gio qua cac nam |
| `create_materialized_view(engine)` | All tables | 1,399,200 rows → mv_training_features | Join tat ca features |
| `run_feature_engineering()` | | | Chay tat ca buoc tren |

### train_weather_model.py

| Function | Input | Output | Mo ta |
|----------|-------|--------|-------|
| `load_training_data()` | PostgreSQL mv_training_features | DataFrame 1.4M rows | Doc data tu DW |
| `add_rolling_features(df)` | DataFrame voi lag columns | DataFrame + 17 rolling columns | Tinh rolling mean, std, trend |
| `compute_sample_weights(df)` | DataFrame voi column year | numpy array weights | Weight 1.0 (2023) → 3.0 (2026) |
| `train()` | | 5 models + metrics | Train tat ca models |

### predict_weather.py

| Function | Input | Output | Mo ta |
|----------|-------|--------|-------|
| `load_artifacts()` | models/ directory | dict models + feature_cols | Load trained models |
| `ensure_date_in_dim(conn, date)` | Connection, date | date_id | Them ngay moi vao dim_date neu chua co |
| `fetch_district_id(conn, name)` | Connection, district name | district_id | Lay ID |
| `fetch_historical_same_day(conn, ...)` | district, year, month_day | dict stats | Lay historical daily |
| `fetch_historical_same_day_hour(conn, ...)` | district, year, month_day, hour | dict stats | Lay historical hourly |
| `fetch_lag_data(conn, ...)` | district, target_date, hour | dict lags | Lay lag data (actual + forecast fallback) |
| `build_feature_row(hour, ...)` | hour, date, district_id, hist, lags | dict 86 features | Xay feature vector cho 1 gio |
| `predict(district, date, save_to_dw)` | district name, date string | DataFrame 24 rows | Predict 24 gio + save DW |
| `predict_range(district, start, n)` | district, start_date, num_days | dict DataFrames | Predict nhieu ngay lien tiep |
| `save_forecast_to_dw(conn, ...)` | district_id, date_id, DataFrame | Rows in DW | Upsert forecast vao DW |

---

## 9. Model artifacts

Sau khi train, cac file sau duoc luu trong `models/`:

| File | Size | Mo ta |
|------|------|-------|
| `xgb_temperature_c.joblib` | ~15MB | XGBRegressor cho nhiet do |
| `xgb_humidity_percent.joblib` | ~15MB | XGBRegressor cho do am |
| `xgb_wind_speed_m_s.joblib` | ~15MB | XGBRegressor cho gio |
| `xgb_rain_classifier.joblib` | ~10MB | XGBClassifier: co mua? |
| `xgb_rain_regressor.joblib` | ~10MB | XGBRegressor: bao nhieu mm? |
| `feature_columns.joblib` | ~1KB | List 86 feature names (thu tu quan trong!) |

---

## 10. Cach retrain

### Khi co data moi

```bash
# 1. Nap data moi vao DW (cap nhat CSV hoac truc tiep INSERT)
python workflow.py --step etl

# 2. Tinh lai features
python workflow.py --step features

# 3. Train lai models
python workflow.py --step train
```

### Toan bo tu dau

```bash
python workflow.py
```

### Chi train lai (giu nguyen features)

```bash
python train_weather_model.py
```

### Tuy chinh hyperparameters

Sua truc tiep trong `train_weather_model.py`, function `train()`, phan `XGBRegressor(...)`.

### Them district moi

1. Them data vao CSV hoac staging table
2. Chay lai ETL: `python workflow.py --step etl`
3. Chay lai features: `python workflow.py --step features`
4. Train lai: `python workflow.py --step train`
