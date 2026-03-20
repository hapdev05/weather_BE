"""
Weather Prediction Workflow Orchestrator.

Full pipeline:
  1. SETUP      - Create PostgreSQL database & Data Warehouse schema (star schema)
  2. ETL        - Extract CSV -> Staging -> Transform dims -> Load fact table
  3. FEATURES   - Compute aggregates, historical same-day stats, materialized view
  4. TRAIN      - Train XGBoost models using features from PostgreSQL
  5. PREDICT    - Forecast weather, save results back to DW for chained prediction

Usage:
  python workflow.py                                        # Full pipeline (steps 1-4)
  python workflow.py --step setup|etl|features|train        # Single step
  python workflow.py --predict My_Khe_Beach 2026-03-20      # Predict 1 day
  python workflow.py --predict My_Khe_Beach 2026-03-20 7    # Predict 7 days chain
"""

import time
import argparse

from setup_database import setup as db_setup
from etl_pipeline import run_etl
from feature_engineering import run_feature_engineering
from train_weather_model import train
from predict_weather import predict, predict_range, display


BANNER = """
╔══════════════════════════════════════════════════════════════╗
║          WEATHER PREDICTION PIPELINE - DA NANG              ║
║                                                              ║
║  Data Warehouse: PostgreSQL (Star Schema)                    ║
║  ML Model: XGBoost                                           ║
║  Features: Historical same-day + Nearby days lag             ║
║  Forecast: Saved to DW for chained multi-day prediction      ║
╚══════════════════════════════════════════════════════════════╝
"""


def run_full_pipeline():
    print(BANNER)
    total_start = time.time()

    steps = [
        ("1/4 - DATABASE SETUP", db_setup),
        ("2/4 - ETL PIPELINE", run_etl),
        ("3/4 - FEATURE ENGINEERING", run_feature_engineering),
        ("4/4 - MODEL TRAINING", train),
    ]

    for name, func in steps:
        print(f"\n{'='*65}")
        print(f"  STEP {name}")
        print(f"{'='*65}")
        t0 = time.time()
        func()
        print(f"  Step completed in {time.time() - t0:.1f}s")

    total = time.time() - total_start
    print(f"\n{'='*65}")
    print(f"  PIPELINE COMPLETE - Total time: {total:.1f}s")
    print(f"{'='*65}")
    print(f"\nDu bao thoi tiet:")
    print(f"  python predict_weather.py <district> <date>")
    print(f"  python predict_weather.py <district> <date> <so_ngay>")
    print(f"\nVi du:")
    print(f"  python predict_weather.py My_Khe_Beach 2026-03-20")
    print(f"  python predict_weather.py My_Khe_Beach 2026-03-20 7")


def run_prediction(district: str, date: str, num_days: int = 1):
    print(BANNER)
    if num_days == 1:
        print(f"Predicting: {district} | {date}")
        result = predict(district, date, save_to_dw=True)
        if result is not None:
            display(district, date, result)
    else:
        results = predict_range(district, date, num_days)
        if results:
            for d, df in results.items():
                display(district, d, df)


def main():
    parser = argparse.ArgumentParser(description="Weather Prediction Pipeline")
    parser.add_argument("--step", choices=["setup", "etl", "features", "train"],
                        help="Run a single pipeline step")
    parser.add_argument("--predict", nargs="+", metavar="ARG",
                        help="Predict: --predict DISTRICT DATE [NUM_DAYS]")

    args = parser.parse_args()

    if args.predict:
        district = args.predict[0]
        date = args.predict[1] if len(args.predict) > 1 else None
        num_days = int(args.predict[2]) if len(args.predict) > 2 else 1

        if not date:
            print("Error: date required. --predict DISTRICT DATE [NUM_DAYS]")
            return
        run_prediction(district, date, num_days)

    elif args.step:
        step_map = {
            "setup": db_setup,
            "etl": run_etl,
            "features": run_feature_engineering,
            "train": train,
        }
        print(f"\nRunning step: {args.step}")
        step_map[args.step]()
    else:
        run_full_pipeline()


if __name__ == "__main__":
    main()
