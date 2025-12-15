from contextlib import contextmanager
from typing import Optional

import psycopg
from psycopg.rows import dict_row

from .settings import settings


@contextmanager
def get_conn():
    conn = psycopg.connect(settings.DATABASE_URL, row_factory=dict_row)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def execute(sql: str, params: Optional[tuple] = None) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())


def fetch_one(sql: str, params: Optional[tuple] = None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()


def fetch_all(sql: str, params: Optional[tuple] = None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()
