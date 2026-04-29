"""
Batch forecast all 53 districts from 2026-03-20 to 2026-04-24.
Runs day-by-day (chained) so each day's forecast feeds into the next as lag data.
"""
import sys, os
sys.path.insert(0, "/Users/hapdev/Downloads/train model")

from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from config import DATABASE_URL, SCHEMA_DW
from predict_weather import predict as run_predict

def main():
    start_date = datetime(2026, 3, 20)
    end_date = datetime(2026, 4, 24)
    
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT district_name FROM {SCHEMA_DW}.dim_district ORDER BY district_name"
        ))
        districts = [r[0] for r in rows]
    engine.dispose()
    
    num_days = (end_date - start_date).days + 1  # 36 days
    total = num_days * len(districts)
    done = 0
    errors = []
    
    print(f"=== BATCH FORECAST ===")
    print(f"Period: {start_date.date()} -> {end_date.date()} ({num_days} days)")
    print(f"Districts: {len(districts)}")
    print(f"Total forecasts: {total}")
    print(f"{'='*60}")
    
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        print(f"\n--- {date_str} ---")
        
        for i, district in enumerate(districts):
            try:
                df = run_predict(district, date_str, save_to_dw=True)
                done += 1
            except Exception as e:
                print(f"  ERROR: {district} {date_str}: {e}")
                errors.append(f"{district} {date_str}: {e}")
                done += 1
        
        pct = round(done / total * 100)
        print(f"  [{done}/{total}] ({pct}%) - {date_str} done for {len(districts)} districts")
        
        current += timedelta(days=1)
    
    print(f"\n{'='*60}")
    print(f"COMPLETED: {done - len(errors)}/{total} OK, {len(errors)} errors")
    if errors:
        print("Errors:")
        for e in errors:
            print(f"  - {e}")

if __name__ == "__main__":
    main()
