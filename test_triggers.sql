-- ============================================================
-- TEST TRIGGERS cho bảng dw.fact_weather_forecast
-- Database: weather_dw
-- Mở pgAdmin → Query Tool → chạy TỪNG BLOCK một
-- (Bôi đen block → F5 hoặc nút ▶ Execute)
-- ============================================================


-- ============================================================
-- BƯỚC 0: Kiểm tra trigger đã được cài chưa
-- ============================================================

SELECT trigger_name, event_manipulation, action_timing
FROM information_schema.triggers
WHERE event_object_schema = 'dw'
  AND event_object_table = 'fact_weather_forecast'
ORDER BY trigger_name, event_manipulation;

-- Kết quả mong đợi: 3 triggers
-- trg_forecast_updated_at  | INSERT | BEFORE
-- trg_forecast_updated_at  | UPDATE | BEFORE
-- trg_log_forecast_change  | UPDATE | AFTER
-- trg_validate_forecast    | INSERT | BEFORE
-- trg_validate_forecast    | UPDATE | BEFORE


-- ============================================================
-- BƯỚC 1: Lấy district_id và date_id hợp lệ để test
-- (cần tồn tại trong dim_district và dim_date)
-- ============================================================

SELECT district_id, district_name
FROM dw.dim_district
LIMIT 3;

SELECT date_id, full_date
FROM dw.dim_date
ORDER BY full_date DESC
LIMIT 3;

-- Ghi nhớ 1 district_id và 1 date_id từ kết quả trên
-- Ví dụ: district_id = 1, date_id = 1100


-- ============================================================
-- TEST 1: trg_forecast_updated_at
-- Mục đích: Kiểm tra updated_at tự động được điền khi INSERT/UPDATE
-- ============================================================

-- 1a. INSERT một forecast mới (thay district_id, date_id nếu cần)
INSERT INTO dw.fact_weather_forecast
    (district_id, date_id, hour_id, temperature_c, humidity_percent, wind_speed_m_s, rain_mm, rain_probability)
VALUES
    (1, 1100, 10, 30.0, 75.0, 3.5, 0.0, 5.0)
ON CONFLICT (district_id, date_id, hour_id) DO UPDATE
    SET temperature_c = EXCLUDED.temperature_c;

-- 1b. Kiểm tra updated_at đã được tự động điền
SELECT forecast_id, district_id, date_id, hour_id,
       temperature_c, predicted_at, updated_at
FROM dw.fact_weather_forecast
WHERE district_id = 1 AND date_id = 1100 AND hour_id = 10;

-- ✅ Kết quả mong đợi: updated_at có giá trị (NOT NULL), gần bằng NOW()

-- 1c. Đợi vài giây rồi UPDATE để thấy updated_at thay đổi
UPDATE dw.fact_weather_forecast
SET temperature_c = 31.5
WHERE district_id = 1 AND date_id = 1100 AND hour_id = 10;

-- 1d. Kiểm tra lại — updated_at phải thay đổi so với lần trước
SELECT forecast_id, temperature_c, predicted_at, updated_at
FROM dw.fact_weather_forecast
WHERE district_id = 1 AND date_id = 1100 AND hour_id = 10;

-- ✅ Kết quả mong đợi: updated_at mới hơn lần trước


-- ============================================================
-- TEST 2: trg_log_forecast_change
-- Mục đích: Khi UPDATE temperature hoặc rain thay đổi > ngưỡng,
--           tự động log vào bảng dw.forecast_history
-- ============================================================

-- 2a. Xem bảng history trước khi test
SELECT * FROM dw.forecast_history
ORDER BY history_id DESC
LIMIT 5;

-- 2b. UPDATE temperature thay đổi > 0.5°C (sẽ trigger log)
UPDATE dw.fact_weather_forecast
SET temperature_c = 35.0    -- thay đổi từ 31.5 → 35.0 (chênh 3.5 > 0.5)
WHERE district_id = 1 AND date_id = 1100 AND hour_id = 10;

-- 2c. Kiểm tra forecast_history — phải có 1 dòng mới
SELECT * FROM dw.forecast_history
ORDER BY history_id DESC
LIMIT 5;

-- ✅ Kết quả mong đợi:
-- old_temperature_c = 31.5
-- new_temperature_c = 35.0
-- changed_at ≈ NOW()

-- 2d. UPDATE nhỏ (chênh < 0.5°C) — KHÔNG nên tạo log
UPDATE dw.fact_weather_forecast
SET temperature_c = 35.2    -- chênh 0.2 < 0.5 → không log
WHERE district_id = 1 AND date_id = 1100 AND hour_id = 10;

-- 2e. Kiểm tra — số dòng trong history KHÔNG tăng
SELECT COUNT(*) AS total_history FROM dw.forecast_history;

-- ✅ Kết quả mong đợi: count không thay đổi so với 2c

-- 2f. UPDATE rain thay đổi > 0.1mm — sẽ trigger log
UPDATE dw.fact_weather_forecast
SET rain_mm = 5.0           -- từ 0.0 → 5.0 (chênh 5.0 > 0.1)
WHERE district_id = 1 AND date_id = 1100 AND hour_id = 10;

-- 2g. Kiểm tra lại — phải có thêm 1 dòng log mới
SELECT * FROM dw.forecast_history
ORDER BY history_id DESC
LIMIT 5;

-- ✅ Kết quả mong đợi:
-- old_rain_mm = 0.0
-- new_rain_mm = 5.0


-- ============================================================
-- TEST 3: trg_validate_forecast
-- Mục đích: Từ chối dữ liệu không hợp lệ (nhiệt độ, độ ẩm, mưa)
-- ============================================================

-- 3a. INSERT nhiệt độ quá cao (> 45°C) → phải BỊ TỪ CHỐI
INSERT INTO dw.fact_weather_forecast
    (district_id, date_id, hour_id, temperature_c, humidity_percent, wind_speed_m_s, rain_mm, rain_probability)
VALUES
    (1, 1100, 11, 50.0, 75.0, 3.5, 0.0, 5.0);

-- ❌ Kết quả mong đợi: ERROR — "Nhiệt độ không hợp lệ: 50 °C"

-- 3b. INSERT nhiệt độ quá thấp (< 5°C) → phải BỊ TỪ CHỐI
INSERT INTO dw.fact_weather_forecast
    (district_id, date_id, hour_id, temperature_c, humidity_percent, wind_speed_m_s, rain_mm, rain_probability)
VALUES
    (1, 1100, 12, 2.0, 75.0, 3.5, 0.0, 5.0);

-- ❌ Kết quả mong đợi: ERROR — "Nhiệt độ không hợp lệ: 2 °C"

-- 3c. INSERT độ ẩm > 100% → phải BỊ TỪ CHỐI
INSERT INTO dw.fact_weather_forecast
    (district_id, date_id, hour_id, temperature_c, humidity_percent, wind_speed_m_s, rain_mm, rain_probability)
VALUES
    (1, 1100, 13, 30.0, 120.0, 3.5, 0.0, 5.0);

-- ❌ Kết quả mong đợi: ERROR — "Độ ẩm không hợp lệ: 120 %"

-- 3d. INSERT mưa âm → phải tự sửa thành 0 (không lỗi)
INSERT INTO dw.fact_weather_forecast
    (district_id, date_id, hour_id, temperature_c, humidity_percent, wind_speed_m_s, rain_mm, rain_probability)
VALUES
    (1, 1100, 14, 30.0, 75.0, 3.5, -5.0, 5.0)
ON CONFLICT (district_id, date_id, hour_id) DO UPDATE
    SET rain_mm = EXCLUDED.rain_mm;

-- Kiểm tra — rain_mm phải = 0 (không phải -5)
SELECT forecast_id, rain_mm
FROM dw.fact_weather_forecast
WHERE district_id = 1 AND date_id = 1100 AND hour_id = 14;

-- ✅ Kết quả mong đợi: rain_mm = 0.0 (trigger tự sửa từ -5 → 0)

-- 3e. INSERT dữ liệu hợp lệ → phải THÀNH CÔNG
INSERT INTO dw.fact_weather_forecast
    (district_id, date_id, hour_id, temperature_c, humidity_percent, wind_speed_m_s, rain_mm, rain_probability)
VALUES
    (1, 1100, 15, 32.0, 80.0, 4.0, 1.2, 45.0)
ON CONFLICT (district_id, date_id, hour_id) DO UPDATE
    SET temperature_c = EXCLUDED.temperature_c;

-- ✅ Kết quả mong đợi: INSERT 1 row thành công


-- ============================================================
-- DỌN DẸP: Xóa dữ liệu test (TÙY CHỌN)
-- Chỉ chạy nếu muốn xóa dữ liệu test vừa tạo
-- ============================================================

-- DELETE FROM dw.fact_weather_forecast
-- WHERE district_id = 1 AND date_id = 1100 AND hour_id IN (10, 14, 15);

-- DELETE FROM dw.forecast_history
-- WHERE district_id = 1 AND date_id = 1100;
