"""
db_writer.py
DuckDB 讀寫封裝：建表、upsert cache、寫入 master。

優化:
- 新增 get_cache_batch（批次查詢 cache，減少 DB round-trip）
- write_master flush 門檻由 1000 提升至 5000
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

logger = logging.getLogger(__name__)

CREATE_STATEMENTS = """
CREATE TABLE IF NOT EXISTS companies_raw (
    cr_no       VARCHAR PRIMARY KEY,
    name_zh     VARCHAR,
    name_en     VARCHAR,
    address_raw VARCHAR,
    fetched_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS address_cache (
    address_hash  VARCHAR PRIMARY KEY,
    address_clean VARCHAR,
    als_json      VARCHAR,
    geo_address   VARCHAR,
    score         DOUBLE,
    region        VARCHAR,
    district      VARCHAR,
    street_name   VARCHAR,
    building_name VARCHAR,
    latitude      DOUBLE,
    longitude     DOUBLE,
    manual_review BOOLEAN,
    fetched_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS master (
    cr_no         VARCHAR PRIMARY KEY,
    name_zh       VARCHAR,
    name_en       VARCHAR,
    address_raw   VARCHAR,
    address_clean VARCHAR,
    geo_address   VARCHAR,
    region        VARCHAR,
    district      VARCHAR,
    street_name   VARCHAR,
    building_name VARCHAR,
    latitude      DOUBLE,
    longitude     DOUBLE,
    confidence    DOUBLE,
    manual_review BOOLEAN,
    last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


class DBWriter:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(db_path)
        self._init_tables()

    def _init_tables(self):
        for stmt in CREATE_STATEMENTS.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                self.con.execute(stmt)
        logger.info("DuckDB 表初始化完成")

    # ---------- Cache ----------

    def get_cache(self, address_hash: str) -> Optional[dict]:
        rows = self.con.execute(
            "SELECT * FROM address_cache WHERE address_hash = ?", [address_hash]
        ).fetchall()
        if not rows:
            return None
        cols = [d[0] for d in self.con.description]
        return dict(zip(cols, rows[0]))

    def get_cache_batch(self, address_hashes: list[str]) -> dict[str, dict]:
        """
        批次查詢 cache，一次 DB round-trip 回傳 {hash: record} 字典。
        大幅減少 N 次單筆查詢的 overhead。
        """
        if not address_hashes:
            return {}
        placeholders = ", ".join(["?"] * len(address_hashes))
        rows = self.con.execute(
            f"SELECT * FROM address_cache WHERE address_hash IN ({placeholders})",
            address_hashes,
        ).fetchall()
        if not rows:
            return {}
        cols = [d[0] for d in self.con.description]
        return {row[0]: dict(zip(cols, row)) for row in rows}

    def upsert_cache(self, record: dict):
        self.con.execute("""
            INSERT INTO address_cache
                (address_hash, address_clean, als_json, geo_address, score,
                 region, district, street_name, building_name,
                 latitude, longitude, manual_review)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (address_hash) DO UPDATE SET
                als_json = EXCLUDED.als_json,
                geo_address = EXCLUDED.geo_address,
                score = EXCLUDED.score,
                fetched_at = CURRENT_TIMESTAMP
        """, [
            record.get("address_hash"),
            record.get("address_clean"),
            record.get("als_json"),
            record.get("geo_address"),
            record.get("score", 0),
            record.get("region"),
            record.get("district"),
            record.get("street_name"),
            record.get("building_name"),
            record.get("latitude"),
            record.get("longitude"),
            record.get("manual_review", False),
        ])

    # ---------- Raw & Master ----------

    def write_raw(self, df: pl.DataFrame):
        """寫入 companies_raw（upsert by cr_no）。"""
        self.con.register("df_raw", df.to_arrow())
        self.con.execute("""
            INSERT INTO companies_raw
            SELECT cr_no, name_zh, name_en, address_raw, fetched_at::TIMESTAMP
            FROM df_raw
            ON CONFLICT (cr_no) DO UPDATE SET
                address_raw = EXCLUDED.address_raw,
                fetched_at  = EXCLUDED.fetched_at
        """)
        self.con.unregister("df_raw")
        logger.info(f"已寫入 companies_raw: {len(df)} 筆")

    def write_master(self, records: list[dict]):
        """批次寫入 master 主檔（建議每 5000 筆呼叫一次）。"""
        if not records:
            return
        now = datetime.now().isoformat()
        rows = [
            (
                r.get("cr_no"), r.get("name_zh"), r.get("name_en"),
                r.get("address_raw"), r.get("address_clean"),
                r.get("geo_address"), r.get("region"), r.get("district"),
                r.get("street_name"), r.get("building_name"),
                r.get("latitude"), r.get("longitude"),
                r.get("score", 0), r.get("manual_review", False), now,
            )
            for r in records
        ]
        self.con.executemany("""
            INSERT INTO master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (cr_no) DO UPDATE SET
                geo_address   = EXCLUDED.geo_address,
                region        = EXCLUDED.region,
                district      = EXCLUDED.district,
                confidence    = EXCLUDED.confidence,
                manual_review = EXCLUDED.manual_review,
                last_updated  = EXCLUDED.last_updated
        """, rows)
        logger.info(f"已寫入 master: {len(rows)} 筆")

    def summary(self) -> dict:
        total = self.con.execute("SELECT COUNT(*) FROM master").fetchone()[0]
        need_review = self.con.execute(
            "SELECT COUNT(*) FROM master WHERE manual_review = TRUE"
        ).fetchone()[0]
        cache_size = self.con.execute("SELECT COUNT(*) FROM address_cache").fetchone()[0]
        avg_score = self.con.execute(
            "SELECT AVG(confidence) FROM master WHERE confidence > 0"
        ).fetchone()[0] or 0
        return {
            "master_total": total,
            "manual_review": need_review,
            "cache_size": cache_size,
            "hit_rate": f"{(1 - need_review/total)*100:.1f}%" if total else "N/A",
            "avg_confidence": f"{avg_score:.1f}",
        }

    def close(self):
        self.con.close()
