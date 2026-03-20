from pydantic import BaseModel, Field
from typing import Optional


class HourlyForecast(BaseModel):
    hour: int
    temperature_c: float
    humidity_percent: float
    wind_speed_m_s: float
    rain_mm: float
    rain_probability: float
    description: str


class DailySummary(BaseModel):
    temp_min: float
    temp_max: float
    temp_avg: float
    humidity_avg: float
    rain_total: float
    wind_max: float


class ForecastResponse(BaseModel):
    district: str
    date: str
    hourly: list[HourlyForecast]
    summary: DailySummary
    saved_to_dw: bool


class PredictRequest(BaseModel):
    district: str = Field(..., example="My_Khe_Beach")
    date: str = Field(..., example="2026-03-20", pattern=r"^\d{4}-\d{2}-\d{2}$")


class PredictRangeRequest(BaseModel):
    district: str = Field(..., example="My_Khe_Beach")
    start_date: str = Field(..., example="2026-03-20", pattern=r"^\d{4}-\d{2}-\d{2}$")
    num_days: int = Field(default=3, ge=1, le=30)


class RangeForecastResponse(BaseModel):
    district: str
    start_date: str
    num_days: int
    forecasts: list[ForecastResponse]


class DistrictInfo(BaseModel):
    district_id: int
    district_name: str


class ActualWeather(BaseModel):
    hour: int
    temperature_c: float
    humidity_percent: float
    wind_speed_m_s: float
    rain_mm: float
    period_of_day: str


class ActualResponse(BaseModel):
    district: str
    date: str
    hourly: list[ActualWeather]
    summary: Optional[DailySummary] = None


class SavedForecastEntry(BaseModel):
    district: str
    date: str
    hours_count: int
    temp_min: float
    temp_max: float
    rain_total: float
    predicted_at: str
