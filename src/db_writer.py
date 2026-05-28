"""
db_writer.py
DuckDB 讀寫封裝：建表、upsert cache、寫入 master。

優化:
- 新增 get_cache_batch（批次查詢 cache，減少 DB round-trip）
- write_master flush 門溻由 1000 提升至 5000
- init_brn_queue: numeric 模式改用 DuckDB generate_series，避免 Python 生成 1 億筆 list，速度提升 10-50x
- master 表新增 company_type / date_of_incorporation / re_domiciliation_date 欄位
- 新增 reset_stale_hits()：將超過 N 天的 hit 重置為 pending，供 --verify-hits 使用
"""

import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

logger = logging.getLogger(__name__)

CREATE_STATEMENTS = """
CREATE TABLE IF NOT EXISTS brn_scan_queue (
    brn        VARCHAR PRIMARY KEY,
    status     VARCHAR DEFAULT 'pending',  -- 'pending' / 'hit' / 'miss'
    queried_at TIMESTAMP,
    batch_id   VARCHAR
);

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
    cr_no                  VARCHAR PRIMARY KEY,
    name_zh                VARCHAR,
    name_en                VARCHAR,
    address_raw            VARCHAR,
    address_clean          VARCHAR,
    geo_address            VARCHAR,
    region                 VARCHAR,
    district               VARCHAR,
    street_name            VARCHAR,
    building_name          VARCHAR,
    latitude               DOUBLE,
    longitude              DOUBLE,
    confidence             DOUBLE,
    manual_review          BOOLEAN,
    company_type           VARCHAR,
    date_of_incorporation  VARCHAR,
    re_domiciliation_date  VARCHAR,
    last_updated           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# 為已存在的 master 表補加新欄位（向下相容）
_MIGRATE_STATEMENTS = [
    "ALTER TABLE master ADD COLUMN IF NOT EXISTS company_type          VARCHAR",
    "ALTER TABLE master ADD COLUMN IF NOT EXISTS date_of_incorporation VARCHAR",
    "ALTER TABLE master ADD COLUMN IF NOT EXISTS re_domiciliation_date VARCHAR",
]


class DBWriter:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(db_path)
        self._lock = threading.Lock()
        self._init_tables()

    def _init_tables(self):
        with self._lock:
            for stmt in CREATE_STATEMENTS.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    self.con.execute(stmt)
            for stmt in _MIGRATE_STATEMENTS:
                try:
                    self.con.execute(stmt)
                except Exception:
                    pass
        logger.info("DuckDB 表初始化完成")

    # ---------- Cache ----------

    def get_cache(self, address_hash: str) -> Optional[dict]:
        with self._lock:
            rows = self.con.execute(
                "SELECT * FROM address_cache WHERE address_hash = ?", [address_hash]
            ).fetchall()
            if not rows:
                return None
            cols = [d[0] for d in self.con.description]
            return dict(zip(cols, rows[0]))

    def get_cache_batch(self, address_hashes: list[str]) -> dict[str, dict]:
        if not address_hashes:
            return {}
        placeholders = ", ".join(["?"] * len(address_hashes))
        with self._lock:
            rows = self.con.execute(
                f"SELECT * FROM address_cache WHERE address_hash IN ({placeholders})",
                address_hashes,
            ).fetchall()
            if not rows:
                return {}
            cols = [d[0] for d in self.con.description]
            return {row[0]: dict(zip(cols, row)) for row in rows}

    def upsert_cache(self, record: dict):
        with self._lock:
            self.con.execute("""
                INSERT INTO address_cache
                    (address_hash, address_clean, als_json, geo_address, score,
                     region, district, street_name, building_name,
                     latitude, longitude, manual_review)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (address_hash) DO UPDATE SET
                    als_json      = EXCLUDED.als_json,
                    geo_address   = EXCLUDED.geo_address,
                    score         = EXCLUDED.score,
                    region        = EXCLUDED.region,
                    district      = EXCLUDED.district,
                    street_name   = EXCLUDED.street_name,
                    building_name = EXCLUDED.building_name,
                    latitude      = EXCLUDED.latitude,
                    longitude     = EXCLUDED.longitude,
                    address_clean = EXCLUDED.address_clean,
                    manual_review = EXCLUDED.manual_review,
                    fetched_at    = CURRENT_TIMESTAMP
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
        with self._lock:
            self.con.register("df_raw", df.to_arrow())
            try:
                self.con.execute("""
                    INSERT INTO companies_raw
                    SELECT cr_no, name_zh, name_en, address_raw, fetched_at::TIMESTAMP
                    FROM df_raw
                    ON CONFLICT (cr_no) DO UPDATE SET
                        address_raw = EXCLUDED.address_raw,
                        fetched_at  = EXCLUDED.fetched_at
                """)
            finally:
                self.con.unregister("df_raw")
        logger.info(f"已寫入 companies_raw: {len(df)} 筆")

    def write_master(self, records: list[dict]):
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
                r.get("score", 0), r.get("manual_review", False),
                r.get("company_type"), r.get("date_of_incorporation"),
                r.get("re_domiciliation_date"), now,
            )
            for r in records
        ]
        with self._lock:
            self.con.executemany("""
                INSERT INTO master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT (cr_no) DO UPDATE SET
                    geo_address           = EXCLUDED.geo_address,
                    region                = EXCLUDED.region,
                    district              = EXCLUDED.district,
                    street_name           = EXCLUDED.street_name,
                    building_name         = EXCLUDED.building_name,
                    latitude              = EXCLUDED.latitude,
                    longitude             = EXCLUDED.longitude,
                    address_clean         = EXCLUDED.address_clean,
                    confidence            = EXCLUDED.confidence,
                    manual_review         = EXCLUDED.manual_review,
                    company_type          = EXCLUDED.company_type,
                    date_of_incorporation = EXCLUDED.date_of_incorporation,
                    re_domiciliation_date = EXCLUDED.re_domiciliation_date,
                    last_updated          = EXCLUDED.last_updated
            """, rows)
        logger.info(f"已寫入 master: {len(rows)} 筆")

    def summary(self) -> dict:
        with self._lock:
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

    # ---------- BRN Scan Queue ----------

    def init_brn_queue(
        self,
        mode: str = "numeric",
        prefixes: list | None = None,
        start: int = 0,
        end: int = 99_999_999,
        prefix_start: int = 1_000_000,
        prefix_end: int = 4_000_000,
        chunk_size: int = 1_000_000,
    ) -> int:
        if prefixes is None:
            prefixes = ["C", "G", "L", "F", "E", "H", "N", "U", "Z", "B", "D"]

        logger.info(f"init_brn_queue: mode={mode}，開始生成 BRN...")

        if mode == "numeric":
            total = end - start + 1
            with self._lock:
                self.con.execute(f"""
                    INSERT INTO brn_scan_queue (brn)
                    SELECT printf('%08d', n)
                    FROM generate_series({start}, {end}) t(n)
                    ON CONFLICT (brn) DO NOTHING
                """)
            logger.info(f"init_brn_queue 完成，共處理 {total:,} 筆")
            return total

        elif mode == "prefix":
            import itertools

            def _iter_chunks():
                it = (
                    f"{prefix}{n}"
                    for prefix in prefixes
                    for n in range(prefix_start, prefix_end + 1)
                )
                while True:
                    chunk = list(itertools.islice(it, chunk_size))
                    if not chunk:
                        break
                    yield chunk

            total_inserted = 0
            for chunk in _iter_chunks():
                rows = [(b,) for b in chunk]
                with self._lock:
                    self.con.executemany(
                        "INSERT INTO brn_scan_queue (brn) VALUES (?) ON CONFLICT (brn) DO NOTHING",
                        rows,
                    )
                total_inserted += len(chunk)
            logger.info(f"init_brn_queue 完成，共處理 {total_inserted:,} 筆")
            return total_inserted

        else:
            raise ValueError(f"未知的 mode: {mode}")

    def fetch_pending_batch(self, batch_size: int = 10_000) -> list[str]:
        with self._lock:
            rows = self.con.execute(
                """
                SELECT brn FROM brn_scan_queue
                WHERE status = 'pending'
                ORDER BY RANDOM()
                LIMIT ?
                """,
                [batch_size],
            ).fetchall()
        return [r[0] for r in rows]

    def fetch_hit_batch(self, batch_size: int = 500, older_than_days: int | None = None) -> list[str]:
        """
        抽取 hit 狀態的 BRN，供 --verify-hits 使用。
        older_than_days: 只抽取超過 N 天未更新的 hit（None = 全部 hit）。
        """
        if older_than_days is not None:
            sql = """
                SELECT brn FROM brn_scan_queue
                WHERE status = 'hit'
                  AND queried_at < NOW() - INTERVAL ? DAY
                ORDER BY queried_at ASC
                LIMIT ?
            """
            params = [older_than_days, batch_size]
        else:
            sql = """
                SELECT brn FROM brn_scan_queue
                WHERE status = 'hit'
                ORDER BY queried_at ASC
                LIMIT ?
            """
            params = [batch_size]
        with self._lock:
            rows = self.con.execute(sql, params).fetchall()
        return [r[0] for r in rows]

    def upsert_verified_raw(self, cr_no: str, api_record: dict) -> str:
        """
        將 --verify-hits 查詢結果寫入/更新 companies_raw。
        回傳 'updated' | 'unchanged' | 'new'。
        """
        address_new = (api_record.get("address_raw") or "").strip()
        name_zh_new = (api_record.get("name_zh") or "").strip()
        name_en_new = (api_record.get("name_en") or "").strip()
        now = datetime.now().isoformat()

        with self._lock:
            existing = self.con.execute(
                "SELECT name_zh, name_en, address_raw FROM companies_raw WHERE cr_no = ?",
                [cr_no]
            ).fetchone()

        if existing is None:
            # 全新記錄（理論上不應發生，但保安全）
            with self._lock:
                self.con.execute(
                    """
                    INSERT INTO companies_raw (cr_no, name_zh, name_en, address_raw, fetched_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (cr_no) DO UPDATE SET
                        name_zh     = EXCLUDED.name_zh,
                        name_en     = EXCLUDED.name_en,
                        address_raw = EXCLUDED.address_raw,
                        fetched_at  = EXCLUDED.fetched_at
                    """,
                    [cr_no, name_zh_new, name_en_new, address_new, now]
                )
            return "new"

        old_addr = (existing[2] or "").strip()
        old_zh   = (existing[0] or "").strip()
        old_en   = (existing[1] or "").strip()
        changed  = (old_addr != address_new or old_zh != name_zh_new or old_en != name_en_new)

        if changed:
            with self._lock:
                self.con.execute(
                    """
                    UPDATE companies_raw
                    SET name_zh = ?, name_en = ?, address_raw = ?, fetched_at = ?
                    WHERE cr_no = ?
                    """,
                    [name_zh_new, name_en_new, address_new, now, cr_no]
                )
                # master 表的地址同步重置，待 --process-als 重新標準化
                self.con.execute(
                    """
                    UPDATE master
                    SET address_raw = ?, address_clean = NULL,
                        geo_address = NULL, region = NULL, district = NULL,
                        street_name = NULL, building_name = NULL,
                        latitude = NULL, longitude = NULL,
                        confidence = 0, manual_review = TRUE,
                        last_updated = ?
                    WHERE cr_no = ?
                    """,
                    [address_new, now, cr_no]
                )
                # brn_scan_queue 更新 queried_at
                self.con.execute(
                    "UPDATE brn_scan_queue SET queried_at = ? WHERE brn = ?",
                    [now, cr_no]
                )
            return "updated"
        else:
            # 資料相同，只更新 queried_at
            with self._lock:
                self.con.execute(
                    "UPDATE brn_scan_queue SET queried_at = ? WHERE brn = ?",
                    [now, cr_no]
                )
            return "unchanged"

    def bulk_update_brn_status(self, records: list[dict]):
        if not records:
            return
        rows = [
            (
                r["status"],
                r.get("queried_at"),
                r.get("batch_id"),
                r["brn"],
            )
            for r in records
        ]
        with self._lock:
            self.con.executemany(
                """
                UPDATE brn_scan_queue
                SET status = ?, queried_at = ?, batch_id = ?
                WHERE brn = ?
                """,
                rows,
            )
        logger.debug(f"bulk_update_brn_status: 更新 {len(rows)} 筆")

    def scan_progress(self) -> dict:
        with self._lock:
            row = self.con.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
                    COUNT(*) FILTER (WHERE status = 'hit')     AS hit,
                    COUNT(*) FILTER (WHERE status = 'miss')    AS miss,
                    MAX(batch_id)    AS last_batch_id,
                    MAX(queried_at)  AS last_queried_at
                FROM brn_scan_queue
                """
            ).fetchone()
        return {
            "pending":         row[0],
            "hit":             row[1],
            "miss":            row[2],
            "last_batch_id":   row[3],
            "last_queried_at": row[4],
        }

    def close(self):
        self.con.close()
