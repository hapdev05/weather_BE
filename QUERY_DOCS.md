# Giải thích các câu query SQL trong project

Tài liệu mô tả **ý nghĩa** và **cách hoạt động** của các câu SQL dùng trong ETL, feature engineering, predict, scheduler và API.

---

## 1. ETL (`etl_pipeline.py`)

### 1.1 `TRUNCATE staging.raw_weather`

Xóa toàn bộ dữ liệu cũ trong bảng staging trước khi nạp CSV mới, tránh trùng lặp.

### 1.2 `INSERT INTO dim_district ... SELECT DISTINCT district FROM staging`

- Lấy **danh sách không trùng** tên địa điểm từ `raw_weather`.
- Mỗi tên → một dòng trong `dim_district` (PostgreSQL tự tăng `district_id`).
- `ORDER BY district` để thứ tự ổn định.

### 1.3 `INSERT INTO dim_date ... SELECT DISTINCT ... FROM staging`

- Từ mỗi `datetime` trong staging, lấy **phần ngày** (`datetime::date`).
- Với mỗi ngày duy nhất, tính sẵn các cột phục vụ phân tích:
  - `EXTRACT(YEAR/MONTH/DAY/DOY FROM d)` — năm, tháng, ngày, ngày trong năm.
  - `EXTRACT(ISODOW FROM d)` — thứ trong tuần (1 = Thứ Hai).
  - `EXTRACT(WEEK FROM d)` — tuần trong năm.
  - `EXTRACT(QUARTER FROM d)` — quý.
  - `TO_CHAR(d, 'Month')` — tên tháng.
  - `EXTRACT(ISODOW FROM d) IN (6, 7)` — cuối tuần hay không.
  - `month * 100 + day` → `month_day` (ví dụ 20/3 → `320`) để so khớp “cùng ngày tháng” giữa các năm.

### 1.4 `INSERT INTO fact_weather_hourly ... JOIN dim_district JOIN dim_date`

- Nguồn: `staging.raw_weather`.
- `JOIN dim_district ON district_name` — map tên chuỗi → `district_id`.
- `JOIN dim_date ON full_date = datetime::date` — map ngày → `date_id`.
- `EXTRACT(HOUR FROM datetime)` — giờ 0–23 → `hour_id` (trùng với `dim_hour`).
- Các cột đo: `temperature_c`, `humidity_percent`, `wind_speed_m_s`, `rain_mm`.

**Ý nghĩa:** chuyển dữ liệu phẳng (CSV) sang **star schema** (fact + dimension keys).

---

## 2. Feature engineering (`feature_engineering.py`)

### 2.1 `INSERT INTO agg_daily_weather ... GROUP BY district_id, date_id`

- Nguồn: `fact_weather_hourly` + join `dim_date` lấy `year`, `month_day`.
- Với **mỗi district + mỗi ngày**, gom 24 giờ:
  - `AVG`, `STDDEV`, `MIN`, `MAX` cho nhiệt độ, độ ẩm, gió, mưa.
  - `SUM(rain_mm)` → tổng mưa trong ngày.

**Ý nghĩa:** một dòng = thống kê cả ngày, dùng cho bước “cùng ngày các năm trước”.

### 2.2 `INSERT INTO agg_historical_same_day` (self-join `agg_daily_weather`)

- `cur` = bản ghi ngày hiện tại (theo `district_id`, `year`, `month_day`).
- `prev` = các ngày **cùng `month_day`** nhưng `prev.year < cur.year`.
- `GROUP BY cur.district_id, cur.year, cur.month_day`:
  - Trung bình / min / max các chỉ số ngày của **các năm trước** cho đúng “ngày–tháng” đó.
- `years_count` = số năm có dữ liệu lịch sử.

**Ý nghĩa:** đặc trưng “20/3 năm trước thường như thế nào” cho từng district.

### 2.3 `INSERT INTO agg_historical_same_day_hour`

- So khớp: **cùng district**, **cùng `month_day`**, **cùng `hour_id`**, **năm trước < năm hiện tại**.
- Tính `AVG`, `STDDEV` theo giờ; `hist_h_rain_prob` = tỷ lệ giờ đó có mưa (`rain_mm > 0.1`) trong quá khứ.

**Ý nghĩa:** pattern theo giờ (ví dụ 14h thường nóng hơn 6h).

### 2.4 Materialized view `mv_training_features`

Một `SELECT` lớn join:

- `fact_weather_hourly` + `dim_district`, `dim_date`, `dim_hour` — mỗi dòng = một giờ thực tế.
- `LEFT JOIN agg_historical_same_day` — theo `district_id`, `year`, `month_day`.
- `LEFT JOIN agg_historical_same_day_hour` — thêm điều kiện `hour`.
- **Lag:** 7 lần `LEFT JOIN fact_weather_hourly AS lagN` với `date_id` tương ứng ngày `full_date - N days` (cùng district, cùng giờ).
- `SIN/COS` cho giờ, ngày trong năm, tháng — mã hóa chu kỳ.
- Cột mục tiêu: `temperature_c`, `humidity_percent`, `wind_speed_m_s`, `rain_mm`, `is_rainy`.

**Ý nghĩa:** một bảng “sẵn feature” để Python chỉ việc đọc và train; lag **chỉ** từ dữ liệu thực (training), không dùng forecast.

---

## 3. Predict — lấy lag (`predict_weather.py`)

### 3.1 CTE `actual`

- Từ `fact_weather_hourly`, lọc:
  - đúng `district_name`, đúng `hour`;
  - `full_date` từ `target_date - 7 ngày` đến **trước** `target_date` (không lấy ngày đang dự báo).
- `days_back = target_date - full_date` — số ngày lùi (1 = hôm qua, 2 = hôm kia, …).
- `source = 'actual'`.

### 3.2 CTE `forecast`

- Giống logic thời gian nhưng đọc từ `fact_weather_forecast` (kết quả dự báo trước đó).
- `source = 'forecast'`.

### 3.3 CTE `combined` + `ROW_NUMBER()`

- `UNION ALL` actual và forecast → có thể **hai dòng cùng một `full_date`** (nếu vừa có actual vừa có forecast — hiếm).
- `PARTITION BY full_date ORDER BY CASE source WHEN 'actual' THEN 0 ELSE 1 END`:
  - Ưu tiên **actual** (0) trước **forecast** (1).
- `WHERE rn = 1` — mỗi ngày chỉ giữ **một** bản ghi (ưu tiên thực đo).

### 3.4 `ORDER BY full_date DESC`

Ứng dụng Python map `days_back` → lag 1d, 2d, …

**Ý nghĩa:** khi dự báo xa trong tương lai, vẫn có “quá khứ gần” từ forecast đã lưu; khi đã có actual thì luôn dùng actual.

### 3.5 Các query nhỏ khác (predict)

- **`SELECT date_id FROM dim_date WHERE full_date = :d`** — kiểm tra / lấy khóa ngày.
- **`INSERT dim_date ... ON CONFLICT DO NOTHING`** — thêm ngày mới (ngày chỉ có forecast) vào dimension.
- **`DELETE + INSERT fact_weather_forecast`** — ghi đè forecast cùng district + date trước khi insert 24 giờ mới.

---

## 4. Scheduler (`api/scheduler.py`)

### 4.1 `SELECT MAX(full_date) FROM fact_weather_hourly JOIN dim_date`

Ngày **mới nhất có dữ liệu thực** trong DW — dùng làm “mốc” để suy ra ngày cần dự báo (ngày +1, +2).

### 4.2 `SELECT DISTINCT full_date FROM fact_weather_forecast ... ORDER BY`

Liệt kê các ngày đã có ít nhất một forecast (thường dùng để debug / kiểm tra).

### 4.3 `SELECT COUNT(DISTINCT fc.district_id) ... WHERE full_date = :d`

Đếm số district đã có forecast cho **một ngày cụ thể**. So với 53:
- Nếu < 53 → scheduler coi là **thiếu** → chạy batch bổ sung.

---

## 5. API (`api/main.py`)

### 5.1 `GET /api/weather` — summary toàn hệ

```sql
SELECT district_name, full_date,
       MIN/MAX/AVG(temperature), AVG(humidity), SUM(rain), MAX(wind), AVG(rain_probability)
FROM fact_weather_forecast
JOIN dim_district, dim_date
GROUP BY district_name, full_date
ORDER BY full_date, district_name
```

**Ý nghĩa:** mỗi dòng = một district × một ngày dự báo, đủ số liệu tóm tắt cho danh sách FE.

### 5.2 `GET /api/weather/{district}`

- Giống nguồn bảng forecast, thêm `dim_hour` để lấy `hour`.
- `WHERE district_name = :district`
- `ORDER BY full_date, hour` — 24h × nhiều ngày.

### 5.3 `GET /api/weather/{district}/{date}`

- Thêm `AND full_date = CAST(:date AS date)` — chỉ một ngày, 24 dòng.

### 5.4 `GET /api/districts`

`SELECT district_id, district_name ... ORDER BY district_name` — danh mục địa điểm.

### 5.5 `GET /api/actual/{district}/{date}`

- Nguồn `fact_weather_hourly` (dữ liệu thực).
- Join `dim_hour` để có `period_of_day` (Night/Morning/...).
- Lọc district + ngày, sắp xếp theo giờ.

### 5.6 Dashboard (`/`)

- **Thống kê nhanh:** `COUNT(*)` trên `fact_weather_hourly`, `fact_weather_forecast`, `dim_district`, `dim_date`.
- **Bảng forecast:** `GROUP BY district_name, full_date` với MIN/MAX temp, AVG humidity, SUM rain, MAX wind (giống tinh thần `/api/weather` nhưng cho HTML).
- **Actual gần đây:** subquery lấy `MAX(full_date)` trong các ngày có hourly actual, trừ 2 ngày → hiển thị vài ngày gần nhất (giới hạn 120 nhóm district×date).

### 5.7 `GET /api/health`

- `SELECT 1` — kiểm tra kết nối DB.
- `COUNT(DISTINCT district_id || '-' || date_id)` trên forecast — số “cặp district–ngày” đã dự báo (ước lượng mức độ đầy dữ liệu).

---

## 6. Khái niệm chung

| Khái niệm | Giải thích ngắn |
|-----------|-----------------|
| **JOIN** | Nối fact với dimension bằng khóa ngoại (`district_id`, `date_id`, `hour_id`). |
| **GROUP BY** | Gom nhiều giờ → một dòng thống kê ngày; hoặc gom nhiều district×date cho API summary. |
| **CTE (WITH)** | Đặt tên cho subquery (`actual`, `forecast`, `combined`) cho dễ đọc và tái dùng. |
| **ROW_NUMBER()** | Đánh số trong nhóm; ở đây dùng để **chọn một dòng** mỗi ngày khi có 2 nguồn (actual vs forecast). |
| **CAST(... AS date)** | Đảm bảo so sánh ngày đúng kiểu, tránh lỗi với tham số chuỗi. |
| **Materialized view** | Lưu kết quả query nặng ra đĩa; train đọc nhanh; cần `REFRESH` / tạo lại khi fact thay đổi. |

---

## 7. Khi nào cần chạy lại query / view?

| Thay đổi | Việc cần làm |
|----------|----------------|
| CSV / ETL mới | `etl_pipeline` → `feature_engineering` (full) → `train_weather_model` |
| Chỉ thêm forecast (API/predict) | Không cần refresh `mv_training_features` cho đến khi train lại |
| Đổi logic feature | Sửa SQL trong `feature_engineering.py`, chạy lại bước features + train |

---

Tài liệu này bổ sung cho `README.md` và `TRAIN_MODEL_DOCS.md`; chi tiết cột bảng xem README phần Data Warehouse.
