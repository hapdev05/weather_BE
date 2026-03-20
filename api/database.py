import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy import create_engine, text
from config import DATABASE_URL, SCHEMA_DW, SCHEMA_FEATURES

engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20, pool_pre_ping=True)


def get_conn():
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()
