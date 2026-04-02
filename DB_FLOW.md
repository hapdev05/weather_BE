# Luồng Hoạt Động Database — Weather Prediction System

Database `weather_dw` hoạt động theo **2 luồng chính**:

- **Luồng 1 (One-time):** Khởi tạo & nạp dữ liệu lịch sử lần đầu
- **Luồng 2 (Daily):** Tự động cập nhật & dự báo mỗi ngày

---

## Luồng 1: Khởi tạo ban đầu (`python workflow.py`)

Chạy **một lần duy nhất** khi setup hệ thống. Toàn bộ pipeline mất ~180 giây.

```
dataset_weather.csv
(1.4 triệu dòng, 53 quận, 2023–2026)
        │
        ▼
┌───────────────────────────────────────────────────────┐
│  BƯỚC 1 — SETUP DATABASE (~1 giây)                    │
│  setup_database.py                                    │
│                                                       │
│  Tạo database: weather_dw                             │
│  Tạo 3 schemas: dw / staging / features              │
│  Tạo các bảng: dim_*, fact_*, agg_*                  │
│  Điền sẵn dim_hour (24 dòng: 0h → 23h)               │
└──────────────────────┬────────────────────────────────┘
                       │
                       ▼
┌───────────────────────────────────────────────────────┐
│  BƯỚC 2 — ETL PIPELINE (~77 giây)                     │
│  etl_pipeline.py                                      │
│                                                       │
│  [EXTRACT]                                            │
│  Đọc CSV → load vào staging.raw_weather               │
│  (1,399,200 dòng thô chưa xử lý)                     │
│                                                       │
│  [TRANSFORM]                                          │
│  Lấy DISTINCT district → INSERT dim_district (53)     │
│  Lấy DISTINCT date     → INSERT dim_date (1,103)      │
│  Tính các trường: year, month, day_of_week,           │
│                   is_weekend, month_day, ...           │
│                                                       │
│  [LOAD]                                               │
│  JOIN staging ⋈ dim_district ⋈ dim_date               │
│  → INSERT vào fact_weather_hourly (1,399,200 dòng)    │
└──────────────────────┬────────────────────────────────┘
                       │
                       ▼
┌───────────────────────────────────────────────────────┐
│  BƯỚC 3 — FEATURE ENGINEERING (~50 giây)              │
│  feature_engineering.py                               │
│                                                       │
│  Từ fact_weather_hourly, tính:                        │
│                                                       │
│  agg_daily_weather (58,300 dòng)                      │
│  → Thống kê ngày: mean/std/min/max của                │
│    nhiệt độ, độ ẩm, gió, mưa — theo từng quận/ngày   │
│                                                       │
│  agg_historical_same_day (38,902 dòng)                │
│  → Trung bình cùng ngày tháng qua các năm trước       │
│    VD: tất cả ngày 20/3 của 2023, 2024, 2025          │
│                                                       │
│  agg_historical_same_day_hour (933,648 dòng)          │
│  → Giống trên nhưng chi tiết theo từng giờ            │
│                                                       │
│  mv_training_features (Materialized View)             │
│  → JOIN tất cả features thành 1 view duy nhất         │
│    86 features/dòng — sẵn sàng cho model train        │
└──────────────────────┬────────────────────────────────┘
                       │
                       ▼
┌───────────────────────────────────────────────────────┐
│  BƯỚC 4 — TRAIN MODEL (~67 giây)                      │
│  train_weather_model.py                               │
│                                                       │
│  Đọc mv_training_features từ PostgreSQL               │
│  Train 5 XGBoost models:                             │
│    - xgb_temperature_c.joblib                         │
│    - xgb_humidity_percent.joblib                      │
│    - xgb_wind_speed_m_s.joblib                        │
│    - xgb_rain_classifier.joblib  (có mưa hay không?)  │
│    - xgb_rain_regressor.joblib   (lượng mưa bao nhiêu)│
│                                                       │
│  Lưu models vào thư mục models/                       │
└───────────────────────────────────────────────────────┘
```

---

## Luồng 2: Hoạt động hàng ngày (tự động)

Sau khi setup xong, hệ thống tự vận hành theo chu kỳ **mỗi 24 giờ**.

```
                    ┌──────────────────────────────────────┐
                    │   MỖI NGÀY LÚC 00:05 (Asia/HCM)     │
                    │   APScheduler kích hoạt              │
                    └─────────────┬────────────────────────┘
                                  │
                    ┌─────────────▼────────────────────────┐
                    │  KIỂM TRA: Forecast đã có chưa?      │
                    │                                      │
                    │  SELECT COUNT(DISTINCT district_id)  │
                    │  FROM dw.fact_weather_forecast        │
                    │  WHERE full_date = ngày mai          │
                    └──────┬────────────────┬──────────────┘
                           │                │
                      < 53 quận          đủ 53 quận
                           │                │
                           ▼                ▼
                    [CHẠY FORECAST]      [BỎ QUA]
                           │
            ┌──────────────▼──────────────────────────────┐
            │  BATCH FORECAST                             │
            │  api/scheduler.py → run_batch_forecast()    │
            │                                             │
            │  Với mỗi (quận × ngày): 53 × 2 = 106 lần  │
            │                                             │
            │  predict_weather.predict(district, date)    │
            │    │                                        │
            │    ├─ Lấy features từ PostgreSQL:           │
            │    │   • agg_historical_same_day            │
            │    │   • agg_historical_same_day_hour       │
            │    │   • lag 1–7 ngày trước:                │
            │    │     ưu tiên fact_weather_hourly (thực) │
            │    │     fallback fact_weather_forecast (dự)│
            │    │                                        │
            │    ├─ Gọi 5 XGBoost models dự báo 24 giờ   │
            │    │                                        │
            │    └─ INSERT/UPDATE vào:                    │
            │       dw.fact_weather_forecast              │
            │       UNIQUE(district_id, date_id, hour_id) │
            └─────────────────────────────────────────────┘
                                  │
                    ┌─────────────▼────────────────────────┐
                    │  Frontend gọi API                    │
                    │  GET /api/weather                    │
                    │  → Đọc từ dw.fact_weather_forecast   │
                    │  → Trả JSON về cho người dùng        │
                    └──────────────────────────────────────┘
```

---

## Chi tiết: Dữ liệu chảy qua từng bảng

| Thứ tự | Bảng | Chiều dữ liệu | Nguồn |
|--------|------|---------------|-------|
| 1 | `staging.raw_weather` | CSV → DB | `etl_pipeline.py` EXTRACT |
| 2 | `dw.dim_district` | staging → dw | `etl_pipeline.py` TRANSFORM |
| 3 | `dw.dim_date` | staging → dw | `etl_pipeline.py` TRANSFORM |
| 4 | `dw.dim_hour` | hard-code 0–23 | `setup_database.py` |
| 5 | `dw.fact_weather_hourly` | staging ⋈ dim_* → fact | `etl_pipeline.py` LOAD |
| 6 | `features.agg_daily_weather` | fact → features | `feature_engineering.py` |
| 7 | `features.agg_historical_same_day` | fact → features | `feature_engineering.py` |
| 8 | `features.agg_historical_same_day_hour` | fact → features | `feature_engineering.py` |
| 9 | `features.mv_training_features` | fact ⋈ features (view) | `feature_engineering.py` |
| 10 | `dw.fact_weather_forecast` | model → dw | `predict_weather.py` |

---

## Sơ đồ quan hệ giữa các bảng (đọc/ghi)

```
                         ┌────────────────────────┐
                         │  staging.raw_weather   │ ← ETL nạp CSV vào đây
                         └───────────┬────────────┘
                                     │ TRANSFORM
                    ┌────────────────┼──────────────────┐
                    ▼                ▼                  ▼
             dim_district        dim_date           dim_hour
                    │                │                  │
                    └────────────────┴──────────────────┘
                                     │ JOIN → LOAD
                                     ▼
                         ┌───────────────────────┐
                         │  fact_weather_hourly  │ ← Dữ liệu thực tế
                         └───────────┬───────────┘
                                     │ AGGREGATE
                    ┌────────────────┼───────────────────┐
                    ▼                ▼                   ▼
             agg_daily_      agg_historical_    agg_historical_
              weather          same_day          same_day_hour
                    └────────────────┴───────────────────┘
                                     │ JOIN → Materialized View
                                     ▼
                         ┌───────────────────────┐
                         │ mv_training_features  │ ← XGBoost đọc từ đây
                         └───────────┬───────────┘
                                     │ PREDICT
                                     ▼
                         ┌───────────────────────┐
                         │ fact_weather_forecast │ ← Kết quả dự báo lưu vào đây
                         └───────────────────────┘
                                     │ API đọc
                                     ▼
                              Frontend / Client
```

---

## Vòng đời của một dòng dữ liệu

```
[CSV: My_Khe_Beach, 2026-03-19 14:00, 32.5°C, 70%, 3.2 m/s, 0 mm]
      │
      ▼ EXTRACT
staging.raw_weather: ('My_Khe_Beach', '2026-03-19 14:00', 32.5, 70, 3.2, 0)
      │
      ▼ TRANSFORM + LOAD
fact_weather_hourly: (district_id=42, date_id=1103, hour_id=14, 32.5, 70, 3.2, 0)
      │
      ▼ AGGREGATE (theo ngày)
agg_daily_weather: (district_id=42, date_id=1103, temp_max=36.1, temp_mean=29.4, ...)
      │
      ▼ AGGREGATE (theo cùng ngày-tháng qua các năm)
agg_historical_same_day: (district_id=42, year=2026, month_day=319, hist_temp_mean=31.2, ...)
      │
      ▼ JOIN vào Materialized View → Train model
      │
      ▼ KHI DỰ BÁO (ngày 2026-03-21)
fact_weather_forecast: (district_id=42, date_id=1105, hour_id=14, temp=33.1, rain_prob=5.2%)
      │
      ▼ API trả về Frontend
{"hour": 14, "temperature_c": 33.1, "rain_probability": 5.2, "description": "Nắng nóng"}
```
