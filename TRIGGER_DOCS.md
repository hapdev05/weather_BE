# Trigger trong Weather Prediction System

> Tài liệu này bao gồm hai phần:
> - **Phần 1:** Application Trigger (trigger ở tầng Python/API)
> - **Phần 2:** Database Trigger (SQL Trigger của PostgreSQL)

## Trigger là gì?

**Trigger** (kích hoạt) là một **sự kiện hoặc điều kiện** khi xảy ra sẽ tự động khởi chạy một luồng xử lý nào đó — mà không cần con người bấm nút thủ công.

Trong project này, trigger **không phải** là SQL Trigger của PostgreSQL (loại trigger được gắn trực tiếp vào bảng DB). Thay vào đó, project sử dụng **hai loại trigger ở tầng ứng dụng (application-level)**.

---

## Hai loại Trigger trong project

### 1. 🕛 Time-based Trigger (Trigger theo thời gian)

> **"Mỗi ngày lúc 00:05, hệ thống tự động chạy dự báo thời tiết."**

**Thư viện sử dụng:** `APScheduler` (BackgroundScheduler)

**File:** `api/main.py` + `api/scheduler.py`

**Cách hoạt động:**

```
Server khởi động
      │
      ▼
APScheduler đăng ký job "daily_forecast"
  - trigger type: "cron"
  - hour = 0, minute = 5
  - timezone = Asia/Ho_Chi_Minh
      │
      ▼ (mỗi ngày 00:05)
scheduled_daily_forecast() ← được gọi tự động
      │
      ├─► check_and_run_if_needed()
      │       │
      │       ├─ Kiểm tra DW: ngày mai + ngày kia đã có forecast chưa?
      │       │
      │       ├─ Nếu THIẾU → run_batch_forecast()
      │       │                 ├─ Predict 53 quận × 2 ngày = 106 forecasts
      │       │                 └─ Lưu vào dw.fact_weather_forecast
      │       │
      │       └─ Nếu ĐỦ → bỏ qua (skip)
      │
      └─► Hoàn thành (~30 giây)
```

**Code tương ứng (`api/main.py`):**

```python
scheduler.add_job(
    scheduled_daily_forecast,   # hàm được gọi khi trigger
    trigger="cron",             # loại trigger: lịch cố định
    hour=0,
    minute=5,
    timezone=ICT,               # Asia/Ho_Chi_Minh
    id="daily_forecast",
)
scheduler.start()
```

---

### 2. 🚀 Event-based Trigger (Trigger theo sự kiện)

> **"Khi server khởi động, tự động kiểm tra và bổ sung forecast nếu thiếu."**
> **"Khi admin gọi API, kích hoạt forecast ngay lập tức."**

#### 2a. Startup Trigger — xảy ra khi server start

**File:** `api/main.py` — hàm `_startup_check()`

```
uvicorn khởi động FastAPI
      │
      ▼
lifespan() chạy
      │
      ├─► Khởi động APScheduler
      │
      └─► Thread riêng chạy _startup_check()
               │
               ▼
          check_and_run_if_needed()
               │
               ├─ Forecast ngày mai đã đủ 53 quận chưa?
               ├─ Forecast ngày kia đã đủ 53 quận chưa?
               │
               ├─ THIẾU → chạy run_batch_forecast() ngay
               └─ ĐỦ    → skip
```

> **Tại sao cần trigger này?**
> Nếu server bị tắt đột ngột vào đêm trước 00:05, forecast của ngày hôm sau sẽ không được tạo. Startup trigger đảm bảo khi server bật lại, hệ thống tự vá thiếu sót.

#### 2b. Manual Trigger — kích hoạt thủ công qua API

**Endpoint:** `POST /api/admin/run-forecast`

**File:** `api/main.py` — hàm `trigger_forecast()`

```
Admin gọi: POST /api/admin/run-forecast
      │
      ▼
FastAPI nhận request → trả về ngay: {"status": "running"}
      │
      └─► BackgroundTask chạy run_batch_forecast() song song
               │
               ├─ base_date: ngày bắt đầu tính (mặc định: hôm nay)
               ├─ forecast_days: số ngày dự báo (mặc định: 2, tối đa: 7)
               │
               └─ Predict 53 quận × N ngày → lưu vào DW
```

**Xem trạng thái sau khi trigger:**
```
GET /api/admin/forecast-status
→ {"status": "completed", "success": 106, "total_forecasts": 106, ...}
```

---

## Tổng quan ba loại Trigger

| # | Tên | Kích hoạt khi nào | File | Hàm |
|---|-----|--------------------|------|-----|
| 1 | **Cron Trigger** | Mỗi ngày 00:05 (Asia/HCM) | `main.py` | `scheduled_daily_forecast()` |
| 2 | **Startup Trigger** | Server vừa khởi động | `main.py` | `_startup_check()` |
| 3 | **Manual Trigger** | Admin gọi `POST /api/admin/run-forecast` | `main.py` | `trigger_forecast()` |

Cả 3 trigger đều dẫn đến cùng một đích: **`run_batch_forecast()`** — hàm thực hiện dự báo và lưu kết quả vào Data Warehouse.

---

## Luồng dữ liệu khi Trigger kích hoạt

```
Trigger (bất kỳ loại nào)
      │
      ▼
check_and_run_if_needed()
      │
      ▼
run_batch_forecast(base_date, forecast_days)
      │
      ├─ Lấy danh sách 53 quận từ dw.dim_district
      ├─ Tính các ngày cần dự báo: base_date + 1, +2 (hoặc +N)
      │
      └─ Với mỗi (quận × ngày):
              │
              ▼
         predict_weather.predict(district, date, save_to_dw=True)
              │
              ├─ Load features từ features.mv_training_features
              ├─ Gọi 5 XGBoost models
              ├─ Tính rain_probability từ Rain Classifier
              │
              └─► INSERT / UPDATE vào dw.fact_weather_forecast
                  (UNIQUE constraint: district_id + date_id + hour_id)
```

---

## Điều kiện bỏ qua (Skip Logic)

Trigger **không chạy lại** nếu forecast đã đủ. Hàm `check_and_run_if_needed()` kiểm tra:

```python
# Với mỗi ngày cần dự báo:
count = SELECT COUNT(DISTINCT district_id)
        FROM dw.fact_weather_forecast
        WHERE full_date = <ngày cần>

if count < 53:   # chưa đủ 53 quận → TRIGGER chạy
    run_batch_forecast(...)
else:            # đã đủ → BỎ QUA
    return {"status": "skipped"}
```

---

## So sánh với SQL Trigger (PostgreSQL)

| Tiêu chí | SQL Trigger (PostgreSQL) | Application Trigger (project này) |
|----------|--------------------------|-------------------------------------|
| Nơi định nghĩa | Trong database (SQL) | Trong Python code |
| Kích hoạt khi | INSERT / UPDATE / DELETE | Thời gian, sự kiện server, API call |
| Mục đích | Ràng buộc dữ liệu, audit log | Điều phối pipeline ML |
| Ví dụ | `AFTER INSERT ON fact_weather` | APScheduler cron, FastAPI background task |

Project này **có thể** dùng SQL Trigger để tự động tính toán aggregations mỗi khi có data mới vào `fact_weather_hourly`, nhưng đã chọn xử lý ở tầng Python để dễ debug và kiểm soát hơn.

---

---

# Phần 2: Database Trigger (SQL Trigger PostgreSQL)

## SQL Trigger là gì?

**SQL Trigger** là một đoạn code SQL được gắn trực tiếp vào một **bảng trong database**. Nó tự động thực thi khi có sự kiện `INSERT`, `UPDATE`, hoặc `DELETE` xảy ra trên bảng đó — mà không cần gọi từ ứng dụng.

```
Ứng dụng (Python/API)
      │
      ▼
  INSERT vào fact_weather_hourly
      │
      ▼
PostgreSQL tự động kích hoạt TRIGGER
      │
      ▼
  Trigger function chạy (PL/pgSQL)
      │
      ▼
  Cập nhật bảng khác / validate / log...
```

---

## Cú pháp tạo SQL Trigger

SQL Trigger gồm **2 bước**:

### Bước 1: Tạo Trigger Function

```sql
CREATE OR REPLACE FUNCTION tên_function()
RETURNS TRIGGER AS $$
BEGIN
    -- Logic xử lý ở đây
    -- NEW: bản ghi mới (dùng với INSERT/UPDATE)
    -- OLD: bản ghi cũ (dùng với UPDATE/DELETE)
    RETURN NEW;  -- hoặc RETURN NULL để hủy thao tác
END;
$$ LANGUAGE plpgsql;
```

### Bước 2: Đăng ký Trigger vào bảng

```sql
CREATE TRIGGER tên_trigger
    BEFORE | AFTER | INSTEAD OF   -- thời điểm chạy
    INSERT | UPDATE | DELETE        -- sự kiện kích hoạt
    ON tên_bảng
    FOR EACH ROW | FOR EACH STATEMENT
    EXECUTE FUNCTION tên_function();
```

---

## Các loại SQL Trigger

| Loại | Mô tả |
|------|-------|
| `BEFORE INSERT` | Chạy trước khi INSERT, có thể sửa/từ chối dữ liệu |
| `AFTER INSERT` | Chạy sau khi INSERT thành công |
| `BEFORE UPDATE` | Chạy trước khi UPDATE |
| `AFTER UPDATE` | Chạy sau khi UPDATE thành công |
| `BEFORE DELETE` | Chạy trước khi DELETE, có thể từ chối |
| `AFTER DELETE` | Chạy sau khi DELETE thành công |
| `FOR EACH ROW` | Chạy cho từng dòng bị ảnh hưởng |
| `FOR EACH STATEMENT` | Chạy 1 lần dù bao nhiêu dòng bị ảnh hưởng |

---

## Ví dụ thực tế cho project này

### Ví dụ 1: Tự động cập nhật `updated_at`

Tự động ghi lại thời điểm forecast được cập nhật:

```sql
-- Bước 1: Thêm cột updated_at vào bảng forecast
ALTER TABLE dw.fact_weather_forecast
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();

-- Bước 2: Tạo trigger function
CREATE OR REPLACE FUNCTION dw.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Bước 3: Đăng ký trigger
CREATE TRIGGER trg_forecast_updated_at
    BEFORE INSERT OR UPDATE
    ON dw.fact_weather_forecast
    FOR EACH ROW
    EXECUTE FUNCTION dw.set_updated_at();
```

**Kết quả:** Mỗi khi Python gọi `INSERT` hoặc `UPDATE` vào `fact_weather_forecast`, PostgreSQL tự động điền `updated_at = NOW()` mà không cần code Python phải xử lý.

---

### Ví dụ 2: Log lịch sử khi forecast bị ghi đè

Khi model chạy lại và ghi đè forecast cũ, lưu lại lịch sử để debug:

```sql
-- Bước 1: Tạo bảng log
CREATE TABLE IF NOT EXISTS dw.forecast_history (
    history_id      SERIAL PRIMARY KEY,
    forecast_id     INT,
    district_id     INT,
    date_id         INT,
    hour_id         SMALLINT,
    old_temperature_c    REAL,
    new_temperature_c    REAL,
    old_rain_mm          REAL,
    new_rain_mm          REAL,
    changed_at      TIMESTAMP DEFAULT NOW()
);

-- Bước 2: Tạo trigger function
CREATE OR REPLACE FUNCTION dw.log_forecast_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Chỉ log nếu nhiệt độ hoặc mưa thay đổi đáng kể
    IF ABS(NEW.temperature_c - OLD.temperature_c) > 0.5
    OR ABS(NEW.rain_mm - OLD.rain_mm) > 0.1 THEN
        INSERT INTO dw.forecast_history (
            forecast_id, district_id, date_id, hour_id,
            old_temperature_c, new_temperature_c,
            old_rain_mm, new_rain_mm
        ) VALUES (
            OLD.forecast_id, OLD.district_id, OLD.date_id, OLD.hour_id,
            OLD.temperature_c, NEW.temperature_c,
            OLD.rain_mm, NEW.rain_mm
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Bước 3: Đăng ký trigger
CREATE TRIGGER trg_log_forecast_change
    AFTER UPDATE
    ON dw.fact_weather_forecast
    FOR EACH ROW
    EXECUTE FUNCTION dw.log_forecast_change();
```

---

### Ví dụ 3: Validate dữ liệu trước khi INSERT

Ngăn chặn dữ liệu bất hợp lệ lọt vào fact table:

```sql
CREATE OR REPLACE FUNCTION dw.validate_weather_data()
RETURNS TRIGGER AS $$
BEGIN
    -- Nhiệt độ Da Nang hợp lệ: 5°C đến 45°C
    IF NEW.temperature_c < 5 OR NEW.temperature_c > 45 THEN
        RAISE EXCEPTION 'Nhiệt độ không hợp lệ: % °C', NEW.temperature_c;
    END IF;

    -- Độ ẩm: 0% đến 100%
    IF NEW.humidity_percent < 0 OR NEW.humidity_percent > 100 THEN
        RAISE EXCEPTION 'Độ ẩm không hợp lệ: % %%', NEW.humidity_percent;
    END IF;

    -- Lượng mưa không âm
    IF NEW.rain_mm < 0 THEN
        NEW.rain_mm = 0;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validate_forecast
    BEFORE INSERT OR UPDATE
    ON dw.fact_weather_forecast
    FOR EACH ROW
    EXECUTE FUNCTION dw.validate_weather_data();
```

---

## Quản lý Trigger

```sql
-- Xem tất cả trigger trong database
SELECT trigger_name, event_manipulation, event_object_table, action_timing
FROM information_schema.triggers
WHERE trigger_schema = 'dw'
ORDER BY event_object_table;

-- Tắt trigger tạm thời (dùng khi bulk load data)
ALTER TABLE dw.fact_weather_forecast DISABLE TRIGGER trg_validate_forecast;

-- Bật lại
ALTER TABLE dw.fact_weather_forecast ENABLE TRIGGER trg_validate_forecast;

-- Xóa trigger
DROP TRIGGER IF EXISTS trg_validate_forecast ON dw.fact_weather_forecast;

-- Xóa trigger function
DROP FUNCTION IF EXISTS dw.validate_weather_data();
```

---

## Khi nào nên và không nên dùng SQL Trigger?

| Nên dùng ✅ | Không nên dùng ❌ |
|-------------|-------------------|
| Tự động điền `created_at`, `updated_at` | Logic nghiệp vụ phức tạp |
| Validate dữ liệu đơn giản | Gọi API bên ngoài |
| Audit log / lịch sử thay đổi | Xử lý tốn nhiều thời gian |
| Đảm bảo toàn vẹn dữ liệu | Thay thế cho stored procedure |
| Sync dữ liệu giữa các bảng liên quan | Logic phụ thuộc vào trạng thái ứng dụng |
