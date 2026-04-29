-- ============================================================
-- TRIGGERS cho bảng dw.fact_weather_forecast
-- Database: weather_dw
-- Chạy toàn bộ file này trong pgAdmin Query Tool
-- (3 Trigger Functions đã tồn tại trong DB)
-- ============================================================


-- ============================================================
-- TRIGGER 1: Tự động cập nhật updated_at
-- ============================================================

ALTER TABLE dw.fact_weather_forecast
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();

DROP TRIGGER IF EXISTS trg_forecast_updated_at ON dw.fact_weather_forecast;

CREATE TRIGGER trg_forecast_updated_at
    BEFORE INSERT OR UPDATE
    ON dw.fact_weather_forecast
    FOR EACH ROW
    EXECUTE FUNCTION dw.set_updated_at();


-- ============================================================
-- TRIGGER 2: Log lịch sử khi forecast bị ghi đè
-- ============================================================

CREATE TABLE IF NOT EXISTS dw.forecast_history (
    history_id        SERIAL PRIMARY KEY,
    forecast_id       INT,
    district_id       INT,
    date_id           INT,
    hour_id           SMALLINT,
    old_temperature_c REAL,
    new_temperature_c REAL,
    old_rain_mm       REAL,
    new_rain_mm       REAL,
    changed_at        TIMESTAMP DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_log_forecast_change ON dw.fact_weather_forecast;

CREATE TRIGGER trg_log_forecast_change
    AFTER UPDATE
    ON dw.fact_weather_forecast
    FOR EACH ROW
    EXECUTE FUNCTION dw.log_forecast_change();


-- ============================================================
-- TRIGGER 3: Validate dữ liệu trước khi INSERT/UPDATE
-- ============================================================

DROP TRIGGER IF EXISTS trg_validate_forecast ON dw.fact_weather_forecast;

CREATE TRIGGER trg_validate_forecast
    BEFORE INSERT OR UPDATE
    ON dw.fact_weather_forecast
    FOR EACH ROW
    EXECUTE FUNCTION dw.validate_weather_data();


-- ============================================================
-- KIỂM TRA sau khi chạy
-- ============================================================

SELECT trigger_name, event_manipulation, action_timing
FROM information_schema.triggers
WHERE event_object_schema = 'dw'
  AND event_object_table = 'fact_weather_forecast'
ORDER BY trigger_name, event_manipulation;

