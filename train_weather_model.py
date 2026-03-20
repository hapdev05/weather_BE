"""
Train weather prediction models using features from PostgreSQL Data Warehouse.

V2 improvements:
  - district_id as feature (location-aware predictions)
  - Historical same-day-hour features (precise hourly patterns from past years)
  - Two-stage rain model: classifier (rain/no-rain) + regressor (amount if rainy)
  - Lag rain flags for rain classification
  - Weighted training: give more weight to recent years
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.metrics import accuracy_score, f1_score, classification_report
from xgboost import XGBRegressor, XGBClassifier
import joblib
import os
import time

from config import DATABASE_URL, MODEL_DIR, TARGETS, SCHEMA_FEATURES


FEATURE_COLS = [
    "district_id",
    "month", "day", "day_of_year", "day_of_week", "hour",
    "hour_sin", "hour_cos", "doy_sin", "doy_cos", "month_sin", "month_cos",
    # Historical same-day (daily)
    "hist_temp_mean", "hist_temp_std", "hist_temp_min", "hist_temp_max",
    "hist_hum_mean", "hist_hum_std", "hist_hum_min", "hist_hum_max",
    "hist_wind_mean", "hist_wind_std", "hist_wind_min", "hist_wind_max",
    "hist_rain_mean", "hist_rain_std", "hist_rain_min", "hist_rain_max",
    "years_count",
    # Historical same-day-hour (precise)
    "hist_h_temp_mean", "hist_h_temp_std",
    "hist_h_hum_mean", "hist_h_hum_std",
    "hist_h_wind_mean", "hist_h_wind_std",
    "hist_h_rain_mean", "hist_h_rain_max", "hist_h_rain_prob",
    # Lag 1-7 days
    "lag_1d_temp", "lag_1d_hum", "lag_1d_wind", "lag_1d_rain",
    "lag_2d_temp", "lag_2d_hum", "lag_2d_wind", "lag_2d_rain",
    "lag_3d_temp", "lag_3d_hum", "lag_3d_wind", "lag_3d_rain",
    "lag_4d_temp", "lag_4d_hum", "lag_4d_wind", "lag_4d_rain",
    "lag_5d_temp", "lag_5d_hum", "lag_5d_wind", "lag_5d_rain",
    "lag_6d_temp", "lag_6d_hum", "lag_6d_wind", "lag_6d_rain",
    "lag_7d_temp", "lag_7d_hum", "lag_7d_wind", "lag_7d_rain",
    # Rain flags
    "lag_1d_is_rainy", "lag_2d_is_rainy", "lag_3d_is_rainy",
]

ROLLING_COLS = [
    "rolling_3d_temp_mean", "rolling_7d_temp_mean", "rolling_3d_temp_std", "trend_3d_temp",
    "rolling_3d_hum_mean", "rolling_7d_hum_mean", "rolling_3d_hum_std", "trend_3d_hum",
    "rolling_3d_wind_mean", "rolling_7d_wind_mean", "rolling_3d_wind_std", "trend_3d_wind",
    "rolling_3d_rain_mean", "rolling_7d_rain_mean", "rolling_3d_rain_std", "trend_3d_rain",
    "recent_rain_count",
]


def load_training_data() -> pd.DataFrame:
    print("Loading training data from PostgreSQL...")
    t0 = time.time()
    engine = create_engine(DATABASE_URL)
    df = pd.read_sql(
        f"SELECT * FROM {SCHEMA_FEATURES}.mv_training_features",
        engine,
    )
    engine.dispose()
    print(f"  Loaded {len(df):,} rows in {time.time() - t0:.1f}s")
    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    print("Computing rolling/trend features...")

    for short in ["temp", "hum", "wind", "rain"]:
        lag_3 = [f"lag_{d}d_{short}" for d in range(1, 4)]
        lag_7 = [f"lag_{d}d_{short}" for d in range(1, 8)]

        df[f"rolling_3d_{short}_mean"] = df[lag_3].mean(axis=1)
        df[f"rolling_7d_{short}_mean"] = df[lag_7].mean(axis=1)
        df[f"rolling_3d_{short}_std"] = df[lag_3].std(axis=1)
        df[f"trend_3d_{short}"] = df[f"lag_1d_{short}"] - df[f"lag_3d_{short}"]

    rain_flag_cols = ["lag_1d_is_rainy", "lag_2d_is_rainy", "lag_3d_is_rainy"]
    df["recent_rain_count"] = df[rain_flag_cols].sum(axis=1)

    return df


def compute_sample_weights(df: pd.DataFrame) -> np.ndarray:
    """Give more weight to recent data so model adapts to current climate."""
    years = df["year"].values
    min_year = years.min()
    max_year = years.max()
    if max_year == min_year:
        return np.ones(len(df))
    normalized = (years - min_year) / (max_year - min_year)
    return 1.0 + normalized * 2.0  # range [1.0, 3.0]


def train():
    df = load_training_data()
    df = add_rolling_features(df)

    all_features = FEATURE_COLS + ROLLING_COLS
    print(f"\nTotal features: {len(all_features)}")

    non_rain_targets = ["temperature_c", "humidity_percent", "wind_speed_m_s"]
    df_clean = df.dropna(subset=all_features + TARGETS).copy()
    print(f"Training samples after dropping NaN: {len(df_clean):,} / {len(df):,}")

    df_clean.sort_values("full_date", inplace=True)
    split_date = df_clean["full_date"].quantile(0.85)
    train_mask = df_clean["full_date"] <= split_date

    X_train = df_clean.loc[train_mask, all_features]
    X_test = df_clean.loc[~train_mask, all_features]
    w_train = compute_sample_weights(df_clean.loc[train_mask])

    os.makedirs(MODEL_DIR, exist_ok=True)
    models = {}
    metrics = {}

    print("\n" + "=" * 65)
    print("  MODEL TRAINING")
    print("=" * 65)

    # --- Train temperature, humidity, wind ---
    for target in non_rain_targets:
        print(f"\n  >> {target}")
        y_train = df_clean.loc[train_mask, target]
        y_test = df_clean.loc[~train_mask, target]

        model = XGBRegressor(
            n_estimators=600,
            max_depth=9,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=5,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=42,
            n_jobs=-1,
            tree_method="hist",
        )
        model.fit(X_train, y_train, sample_weight=w_train,
                  eval_set=[(X_test, y_test)], verbose=False)

        y_pred = model.predict(X_test)
        if target == "humidity_percent":
            y_pred = np.clip(y_pred, 0, 100)

        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)
        metrics[target] = {"MAE": mae, "RMSE": rmse, "R2": r2}
        print(f"     MAE={mae:.4f}  RMSE={rmse:.4f}  R2={r2:.4f}")

        joblib.dump(model, os.path.join(MODEL_DIR, f"xgb_{target}.joblib"))
        models[target] = model

    # --- Two-stage rain model ---
    print(f"\n  >> rain_mm (TWO-STAGE)")

    # Stage 1: classify rain / no-rain
    y_cls_train = (df_clean.loc[train_mask, "rain_mm"] > 0.1).astype(int)
    y_cls_test = (df_clean.loc[~train_mask, "rain_mm"] > 0.1).astype(int)

    n_pos = y_cls_train.sum()
    n_neg = len(y_cls_train) - n_pos
    scale_pos = n_neg / max(n_pos, 1)

    rain_clf = XGBClassifier(
        n_estimators=400,
        max_depth=7,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos,
        random_state=42,
        n_jobs=-1,
        tree_method="hist",
        eval_metric="logloss",
    )
    rain_clf.fit(X_train, y_cls_train, sample_weight=w_train,
                 eval_set=[(X_test, y_cls_test)], verbose=False)

    y_cls_pred = rain_clf.predict(X_test)
    y_cls_prob = rain_clf.predict_proba(X_test)[:, 1]

    acc = accuracy_score(y_cls_test, y_cls_pred)
    f1 = f1_score(y_cls_test, y_cls_pred)
    print(f"     Stage 1 (classifier): Acc={acc:.4f}  F1={f1:.4f}")
    print(f"     Rain ratio: train={n_pos/len(y_cls_train):.3f}  test={y_cls_test.mean():.3f}")

    # Stage 2: regressor only on rainy samples
    rain_train_mask = y_cls_train == 1
    rain_test_mask = y_cls_test == 1

    if rain_train_mask.sum() > 100:
        X_rain_train = X_train[rain_train_mask.values]
        y_rain_train = df_clean.loc[train_mask, "rain_mm"][rain_train_mask.values]
        X_rain_test = X_test[rain_test_mask.values] if rain_test_mask.sum() > 0 else X_test[:1]
        y_rain_test = df_clean.loc[~train_mask, "rain_mm"][rain_test_mask.values] if rain_test_mask.sum() > 0 else pd.Series([0.0])

        rain_reg = XGBRegressor(
            n_estimators=400,
            max_depth=7,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=3,
            random_state=42,
            n_jobs=-1,
            tree_method="hist",
        )
        rain_reg.fit(X_rain_train, y_rain_train,
                     eval_set=[(X_rain_test, y_rain_test)], verbose=False)
    else:
        rain_reg = None
        print("     WARNING: Too few rainy samples for Stage 2")

    # Combined prediction
    y_rain_pred = np.zeros(len(X_test))
    pred_rainy = y_cls_prob > 0.5
    if rain_reg is not None and pred_rainy.sum() > 0:
        y_rain_pred[pred_rainy] = np.maximum(rain_reg.predict(X_test[pred_rainy]), 0)

    y_rain_test_full = df_clean.loc[~train_mask, "rain_mm"]
    mae = mean_absolute_error(y_rain_test_full, y_rain_pred)
    rmse = np.sqrt(mean_squared_error(y_rain_test_full, y_rain_pred))
    r2 = r2_score(y_rain_test_full, y_rain_pred)
    metrics["rain_mm"] = {"MAE": mae, "RMSE": rmse, "R2": r2}
    print(f"     Stage 2 (combined): MAE={mae:.4f}  RMSE={rmse:.4f}  R2={r2:.4f}")

    joblib.dump(rain_clf, os.path.join(MODEL_DIR, "xgb_rain_classifier.joblib"))
    if rain_reg is not None:
        joblib.dump(rain_reg, os.path.join(MODEL_DIR, "xgb_rain_regressor.joblib"))
    models["rain_clf"] = rain_clf
    models["rain_reg"] = rain_reg

    # --- Save artifacts ---
    joblib.dump(all_features, os.path.join(MODEL_DIR, "feature_columns.joblib"))

    print("\n" + "=" * 65)
    print("  EVALUATION SUMMARY")
    print("=" * 65)
    summary = pd.DataFrame(metrics).T
    print(summary.to_string())
    print(f"\n  Models saved to: {MODEL_DIR}/")

    return models, all_features, metrics


if __name__ == "__main__":
    train()
