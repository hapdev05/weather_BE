# Database Triggers — Weather Prediction System

> Tài liệu mô tả **3 SQL Triggers** đang hoạt động trong database `weather_dw`,
> trên bảng `dw.fact_weather_forecast`.

---

## Tổng quan 3 Triggers trong Database

| # | Tên Trigger trong DB | Trigger Function | Loại | Bảng |
|---|----------------------|------------------|------|------|
| 1 | `trg_forecast_updated_at` | `dw.set_updated_at()` | BEFORE INSERT OR UPDATE | `dw.fact_weather_forecast` |
| 2 | `trg_log_forecast_change` | `dw.log_forecast_change()` | AFTER UPDATE | `dw.fact_weather_forecast` |
| 3 | `trg_validate_forecast` | `dw.validate_weather_data()` | BEFORE INSERT OR UPDATE | `dw.fact_weather_forecast` |

---

## Luồng thực thi khi INSERT/UPDATE

```
Python predict → INSERT/UPDATE vào dw.fact_weather_forecast
                        │
                   ┌────▼──────────────────────┐
                   │ trg_validate_forecast      │ → BEFORE INSERT OR UPDATE
                   │ Function: validate_weather │   Validate: dữ liệu hợp lệ không?
                   │ _data()                    │   ❌ Sai → REJECT, không lưu
                   └────┬──────────────────────┘   ✅ Đúng → tiếp tục
                        │
                   ┌────▼──────────────────────┐
                   │ trg_forecast_updated_at    │ → BEFORE INSERT OR UPDATE
                   │ Function: set_updated_at() │   Ghi updated_at = NOW()
                   └────┬──────────────────────┘
                        │
                   ┌────▼──────────────────────┐
                   │        SAVE TO DB          │ → PostgreSQL lưu dữ liệu
                   └────┬──────────────────────┘
                        │
                   ┌────▼──────────────────────┐
                   │ trg_log_forecast_change    │ → AFTER UPDATE (chỉ khi UPDATE)
                   │ Function: log_forecast     │   Nếu chênh lệch lớn
                   │ _change()                  │   → Log old/new vào forecast_history
                   └───────────────────────────┘
```

---

## Chi tiết từng Trigger

### 1. `trg_forecast_updated_at`

| Thuộc tính | Giá trị |
|------------|---------|
| **Tên trigger** | `trg_forecast_updated_at` |
| **Trigger function** | `dw.set_updated_at()` |
| **Thời điểm** | `BEFORE INSERT OR UPDATE` |
| **Bảng** | `dw.fact_weather_forecast` |
| **Scope** | `FOR EACH ROW` |

**Tác dụng:** Mỗi khi có INSERT hoặc UPDATE, tự động gán `updated_at = NOW()`.

**Code trigger function:**

```sql
CREATE OR REPLACE FUNCTION dw.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

**Cột liên quan:**

```sql
ALTER TABLE dw.fact_weather_forecast
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
```

**Ví dụ thực tế:**
- Model chạy batch forecast lúc 00:05 → `updated_at = '2026-04-29 00:05:12'`
- Admin chạy lại forecast lúc 15:00 → `updated_at = '2026-04-29 15:00:03'`
- Frontend hiển thị: "Cập nhật lúc 15:00"

---

### 2. `trg_log_forecast_change`

| Thuộc tính | Giá trị |
|------------|---------|
| **Tên trigger** | `trg_log_forecast_change` |
| **Trigger function** | `dw.log_forecast_change()` |
| **Thời điểm** | `AFTER UPDATE` |
| **Bảng** | `dw.fact_weather_forecast` |
| **Scope** | `FOR EACH ROW` |

**Tác dụng:** Khi forecast bị ghi đè (UPDATE) và giá trị thay đổi đáng kể, lưu lại lịch sử vào bảng `dw.forecast_history`.

**Điều kiện log:** Chỉ log khi:
- Nhiệt độ chênh > 0.5°C: `ABS(NEW.temperature_c - OLD.temperature_c) > 0.5`
- HOẶC mưa chênh > 0.1mm: `ABS(NEW.rain_mm - OLD.rain_mm) > 0.1`

**Code trigger function:**

```sql
CREATE OR REPLACE FUNCTION dw.log_forecast_change()
RETURNS TRIGGER AS $$
BEGIN
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
```

**Bảng log liên quan — `dw.forecast_history`:**

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `history_id` | SERIAL PK | ID tự tăng |
| `forecast_id` | INT | ID forecast bị thay đổi |
| `district_id` | INT | Quận nào |
| `date_id` | INT | Ngày nào |
| `hour_id` | SMALLINT | Giờ nào |
| `old_temperature_c` | REAL | Nhiệt độ cũ |
| `new_temperature_c` | REAL | Nhiệt độ mới |
| `old_rain_mm` | REAL | Mưa cũ |
| `new_rain_mm` | REAL | Mưa mới |
| `changed_at` | TIMESTAMP | Thời điểm thay đổi |

**Ví dụ thực tế:**
```
Ngày 28/4: model dự báo ngày 30/4 lúc 14h → 32°C, mưa 0mm
Ngày 29/4: model chạy lại → 28°C, mưa 2mm

→ forecast_history ghi:
  old_temperature_c = 32.0, new_temperature_c = 28.0
  old_rain_mm = 0.0, new_rain_mm = 2.0
  changed_at = '2026-04-29 00:05:12'
```

---

### 3. `trg_validate_forecast`

| Thuộc tính | Giá trị |
|------------|---------|
| **Tên trigger** | `trg_validate_forecast` |
| **Trigger function** | `dw.validate_weather_data()` |
| **Thời điểm** | `BEFORE INSERT OR UPDATE` |
| **Bảng** | `dw.fact_weather_forecast` |
| **Scope** | `FOR EACH ROW` |

**Tác dụng:** Kiểm tra dữ liệu trước khi lưu. Từ chối dữ liệu bất hợp lệ, tự sửa giá trị âm.

**3 quy tắc validate:**

| Quy tắc | Điều kiện | Hành động |
|---------|-----------|-----------|
| Nhiệt độ | `< 5°C` hoặc `> 45°C` | ❌ `RAISE EXCEPTION` — từ chối INSERT/UPDATE |
| Độ ẩm | `< 0%` hoặc `> 100%` | ❌ `RAISE EXCEPTION` — từ chối INSERT/UPDATE |
| Lượng mưa | `< 0 mm` | ✅ Tự sửa `rain_mm = 0` — không lỗi |

**Code trigger function:**

```sql
CREATE OR REPLACE FUNCTION dw.validate_weather_data()
RETURNS TRIGGER AS $$
BEGIN
    -- Nhiệt độ Đà Nẵng hợp lệ: 5°C đến 45°C
    IF NEW.temperature_c < 5 OR NEW.temperature_c > 45 THEN
        RAISE EXCEPTION 'Nhiệt độ không hợp lệ: % °C', NEW.temperature_c;
    END IF;

    -- Độ ẩm: 0% đến 100%
    IF NEW.humidity_percent < 0 OR NEW.humidity_percent > 100 THEN
        RAISE EXCEPTION 'Độ ẩm không hợp lệ: % %%', NEW.humidity_percent;
    END IF;

    -- Lượng mưa không âm → tự sửa
    IF NEW.rain_mm < 0 THEN
        NEW.rain_mm = 0;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

**Ví dụ thực tế:**
- Model predict ra -10°C → trigger chặn → dữ liệu sai KHÔNG lọt vào DB
- Model predict ra rain = -0.5mm → trigger tự sửa thành 0mm → lưu bình thường

---

## Quản lý Triggers

```sql
-- Xem tất cả trigger đang hoạt động
SELECT trigger_name, event_manipulation, action_timing
FROM information_schema.triggers
WHERE event_object_schema = 'dw'
  AND event_object_table = 'fact_weather_forecast'
ORDER BY trigger_name, event_manipulation;

-- Tắt trigger tạm thời (khi bulk load data)
ALTER TABLE dw.fact_weather_forecast DISABLE TRIGGER trg_validate_forecast;

-- Bật lại trigger
ALTER TABLE dw.fact_weather_forecast ENABLE TRIGGER trg_validate_forecast;

-- Xóa trigger
DROP TRIGGER IF EXISTS trg_validate_forecast ON dw.fact_weather_forecast;

-- Xóa trigger function
DROP FUNCTION IF EXISTS dw.validate_weather_data();
```

---

## File liên quan

| File | Mô tả |
|------|-------|
| `triggers.sql` | SQL tạo triggers (chạy trong pgAdmin) |
| `test_triggers.sql` | SQL test từng trigger (chạy trong pgAdmin) |
| `setup_database.py` | Tạo bảng `fact_weather_forecast` (Python) |
