"""
cr_downloader_brn.py
BRN 盲查下載器：透過 BRN（Business Registration Number）逐筆查詢公司資料。

策略說明：
  - mode=numeric：掃描 00000000–99999999（8 位補零數字）
  - mode=prefix ：掃描字母前綴 + 數字（如 C1000000–C3999999）
  - 不使用順序掃描；每批從 brn_scan_queue 隨機抽取 pending BRN
  - 查詢後批次更新 hit/miss 狀態
  - 中斷安全：失敗保持 pending，下次自動重試

反結機制：
  - User-Agent 輪換（4 個真實瀏覽器 UA）
  - Jitter 隨機延遲
  - 429 精確退避（讀取 Retry-After）
  - Circuit Breaker（連續失敗熱斷）
  - Tenacity 指數重試（最多 5 次）
  - Referer 偽裝

API：
  GET https://data.cr.gov.hk/cr/api/api/v1/api_builder/json/local/search
  query[0][key1]=Brn&query[0][key2]=equal&query[0][key3]=<BRN>&format=json

  注意：
  1. API 要求方括號不被 URL encode，使用手動拼接 URL 而非 httpx params。
  2. 不存在的 BRN API 回傳 400（而非空陣列），程式將 400 視為 miss。
  3. download_batch() 完成後可透過 on_batch_hit callback 即時交給 pipeline 處理。
"""

import asyncio
import logging
import random
import time
import uuid
from collections import deque
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Awaitable, Callable

import httpx
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# httpx 過於詳細，只顯示 WARNING 以上
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_USER_AGENTS = deque([
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
])


class CircuitBreakerState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"


class CircuitBreaker:
    def __init__(self, threshold: int = 10, cooldown: float = 60.0):
        self.threshold = threshold
        self.cooldown = cooldown
        self._failures = 0
        self._state = CircuitBreakerState.CLOSED
        self._opened_at: float | None = None

    @property
    def state(self) -> CircuitBreakerState:
        if self._state == CircuitBreakerState.OPEN:
            if time.monotonic() - self._opened_at >= self.cooldown:
                logger.info("Circuit Breaker 冷卻完畢，回到 CLOSED")
                self._state = CircuitBreakerState.CLOSED
                self._failures = 0
                self._opened_at = None
        return self._state

    def record_failure(self):
        self._failures += 1
        if self._failures >= self.threshold:
            self._state = CircuitBreakerState.OPEN
            self._opened_at = time.monotonic()
            logger.warning(
                f"Circuit Breaker OPEN（連續失敗 {self._failures} 次），冷卻 {self.cooldown}s"
            )

    def record_success(self):
        self._failures = 0

    def is_open(self) -> bool:
        return self.state == CircuitBreakerState.OPEN


class CRDownloaderBrn:
    def __init__(self, config: dict, db=None):
        cr_cfg = config.get("cr", {})
        brn_cfg = config.get("cr_brn", {})

        self.base_url = cr_cfg.get(
            "base_url",
            "https://data.cr.gov.hk/cr/api/api/v1/api_builder/json/local/search",
        )
        self.raw_dir = Path(cr_cfg.get("raw_dir", "data/raw"))
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        self.mode: str = brn_cfg.get("mode", "numeric")
        self.prefixes: list[str] = brn_cfg.get(
            "prefixes", ["C", "G", "L", "F", "E", "H", "N", "U", "Z", "B", "D"]
        )
        self.prefix_start: int = brn_cfg.get("prefix_start", 1_000_000)
        self.prefix_end: int = brn_cfg.get("prefix_end", 4_000_000)
        self.start: int = brn_cfg.get("start", 0)
        self.end: int = brn_cfg.get("end", 99_999_999)

        self.fetch_batch_size: int = brn_cfg.get("fetch_batch_size", 10_000)
        self.concurrency: int = brn_cfg.get("concurrency", 20)
        self.miss_limit: int = brn_cfg.get("miss_limit", 2_000)
        self.batch_write: int = brn_cfg.get("batch_write", 2_000)
        self.jitter_min: float = brn_cfg.get("jitter_min", 0.05)
        self.jitter_max: float = brn_cfg.get("jitter_max", 0.30)
        self.cb_threshold: int = brn_cfg.get("cb_threshold", 10)
        self.cb_cooldown: float = brn_cfg.get("cb_cooldown", 60.0)
        self.request_timeout: float = cr_cfg.get("request_timeout", 30)
        self.progress_every: int = brn_cfg.get("progress_every", 500)

        self.db = db
        self.cb = CircuitBreaker(self.cb_threshold, self.cb_cooldown)

        self._writer: pq.ParquetWriter | None = None
        self._schema: pa.Schema | None = None
        self._today_parquet: Path | None = None

        logger.info(
            f"CRDownloaderBrn 初始化：mode={self.mode}, "
            f"concurrency={self.concurrency}, batch={self.fetch_batch_size}"
        )

    def _next_headers(self) -> dict:
        _USER_AGENTS.rotate(1)
        return {
            "User-Agent": _USER_AGENTS[0],
            "Accept": "application/json",
            "Referer": "https://www.cr.gov.hk/",
        }

    def _build_url(self, brn: str) -> str:
        return (
            f"{self.base_url}"
            f"?query[0][key1]=Brn"
            f"&query[0][key2]=equal"
            f"&query[0][key3]={brn}"
            f"&format=json"
        )

    async def _fetch_brn(self, client: httpx.AsyncClient, brn: str) -> list[dict] | None:
        url = self._build_url(brn)

        @retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=3, max=60),
            retry=retry_if_exception_type(
                (httpx.TransportError, httpx.TimeoutException)
            ),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        async def _do_fetch() -> list[dict] | None:
            resp = await client.get(url, timeout=self.request_timeout, headers=self._next_headers())
            if resp.status_code == 400:
                return None  # BRN 不存在，視為 miss，不重試
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", self.cb_cooldown))
                logger.warning(f"BRN={brn} → 429，等待 {retry_after}s")
                self.cb.record_failure()
                await asyncio.sleep(retry_after)
                resp.raise_for_status()
            if resp.status_code == 503:
                logger.warning(f"BRN={brn} → 503")
                self.cb.record_failure()
                resp.raise_for_status()
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []

        return await _do_fetch()

    async def _query_one(
        self, client: httpx.AsyncClient, sem: asyncio.Semaphore, brn: str, batch_id: str
    ) -> dict:
        if self.cb.is_open():
            return {"brn": brn, "status": "pending", "queried_at": None, "batch_id": batch_id, "records": []}

        await asyncio.sleep(random.uniform(self.jitter_min, self.jitter_max))

        async with sem:
            try:
                result = await self._fetch_brn(client, brn)
                self.cb.record_success()
                if result is None or len(result) == 0:
                    return {"brn": brn, "status": "miss", "queried_at": datetime.now().isoformat(), "batch_id": batch_id, "records": []}
                return {"brn": brn, "status": "hit", "queried_at": datetime.now().isoformat(), "batch_id": batch_id, "records": result}
            except Exception as exc:
                self.cb.record_failure()
                logger.error(f"BRN={brn} 最終失敗：{exc}")
                return {"brn": brn, "status": "pending", "queried_at": None, "batch_id": batch_id, "records": []}

    def _normalize_hit_records(self, hit_records: list[dict]) -> pl.DataFrame:
        """將 API 原始 hit records 轉為標準化 DataFrame（欄位重命名）。"""
        fetched_at = datetime.now().isoformat()
        for r in hit_records:
            r["fetched_at"] = fetched_at
        df = pl.DataFrame(hit_records)
        rename_map = {
            "Brn": "cr_no",
            "Chinese_Company_Name": "name_zh",
            "English_Company_Name": "name_en",
            "Address_of_Registered_Office": "address_raw",
            "Company_Type": "company_type",
            "Date_of_Incorporation": "date_of_incorporation",
            "Re-domiciliation_Date": "re_domiciliation_date",
        }
        existing = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(existing)
        df = df.rename(
            {c: c.lower().replace(" ", "_").replace("-", "_") for c in df.columns if c not in existing.values()}
        )
        return df

    def _flush_to_parquet(self, hit_records: list[dict], batch_id: str):
        if not hit_records:
            return
        today = datetime.now().strftime("%Y%m%d")
        output_parquet = self.raw_dir / f"cr_brn_{today}.parquet"
        self._today_parquet = output_parquet

        df = self._normalize_hit_records(hit_records)
        arrow_batch = df.to_arrow()
        if self._writer is None:
            self._schema = arrow_batch.schema
            self._writer = pq.ParquetWriter(output_parquet, self._schema, compression="snappy")
        else:
            try:
                arrow_batch = arrow_batch.cast(self._schema)
            except Exception:
                for field in self._schema:
                    if field.name not in df.columns:
                        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(field.name))
                arrow_batch = df.select([f.name for f in self._schema]).to_arrow().cast(self._schema)
        self._writer.write_table(arrow_batch)
        logger.info(f"Parquet flush: {len(hit_records)} 筆 hit（batch={batch_id}）")

    async def download_batch(
        self,
        on_hit: Callable[[pl.DataFrame], Awaitable[None]] | None = None,
    ) -> dict:
        """
        從 brn_scan_queue 抽取 pending BRN → 並發查詢 → 更新狀態。

        on_hit: 可選 async callback，batch 完成後以標準化 DataFrame 呼叫。
                pipeline 傳入此 callback 做 ALS + 寫 master。
        """
        if self.db is None:
            raise RuntimeError("download_batch 需要 db（DBWriter）實例")

        batch_id = str(uuid.uuid4())[:8]
        pending_brns = self.db.fetch_pending_batch(self.fetch_batch_size)

        if not pending_brns:
            logger.info("brn_scan_queue 中無 pending BRN，批次結束")
            return {"total": 0, "hit": 0, "miss": 0, "skipped": 0}

        total_in_batch = len(pending_brns)
        logger.info(f"=== BRN 批次開始 batch={batch_id}，取得 {total_in_batch:,} 筆 pending ===")

        sem = asyncio.Semaphore(self.concurrency)
        tasks_results: list[dict] = []
        all_hit_records: list[dict] = []
        consecutive_miss = 0
        batch_stopped = False
        done_count = hit_count = miss_count = 0
        batch_start = time.monotonic()

        async with httpx.AsyncClient(http2=True) as client:
            tasks = [self._query_one(client, sem, brn, batch_id) for brn in pending_brns]
            for coro in asyncio.as_completed(tasks):
                result = await coro
                tasks_results.append(result)
                done_count += 1

                if result["status"] == "hit":
                    hit_count += 1
                    consecutive_miss = 0
                    all_hit_records.extend(result["records"])
                elif result["status"] == "miss":
                    miss_count += 1
                    consecutive_miss += 1
                    if consecutive_miss >= self.miss_limit:
                        logger.info(f"本批連續 miss {consecutive_miss} 次達到門檻 ({self.miss_limit})，停止本批")
                        batch_stopped = True
                        break

                if done_count % self.progress_every == 0 or done_count == total_in_batch:
                    elapsed = time.monotonic() - batch_start
                    rate = done_count / elapsed if elapsed > 0 else 0
                    pct = done_count / total_in_batch * 100
                    print(
                        f"\r[batch={batch_id}] {done_count:,}/{total_in_batch:,} "
                        f"({pct:.1f}%) | hit={hit_count:,} miss={miss_count:,} "
                        f"| {rate:.0f} BRN/s",
                        end="", flush=True,
                    )
        print()

        # 先 flush Parquet（原始備份）
        if all_hit_records:
            self._flush_to_parquet(all_hit_records, batch_id)

        # 更新 brn_scan_queue 狀態
        update_records = [r for r in tasks_results if r["status"] in ("hit", "miss")]
        if update_records:
            self.db.bulk_update_brn_status(update_records)

        # 立即呼叫 pipeline callback → ALS + 寫 master
        if on_hit and all_hit_records:
            hit_df = self._normalize_hit_records(all_hit_records)
            logger.info(f"batch={batch_id} 觸發 on_hit callback，{len(hit_df)} 筆交給 pipeline")
            await on_hit(hit_df)

        stats = {
            "total": len(tasks_results),
            "hit": hit_count,
            "miss": miss_count,
            "skipped": sum(1 for r in tasks_results if r["status"] == "pending"),
            "batch_stopped_early": batch_stopped,
        }
        logger.info(f"=== 批次完成 batch={batch_id} === {stats}")
        return stats

    async def download_all(
        self,
        resume: bool = True,
        on_hit: Callable[[pl.DataFrame], Awaitable[None]] | None = None,
    ) -> Path:
        """
        循環執行 download_batch 直到佇列空。
        on_hit callback 每批有 hit 時觸發，供 pipeline 即時做 ALS + 寫 master。
        """
        total_hit = total_miss = batch_num = 0
        logger.info("=== download_all 開始，循環執行到佇列空 —— Ctrl+C 可安全中斷 ===")

        while True:
            stats = await self.download_batch(on_hit=on_hit)
            if stats["total"] == 0:
                logger.info("=== 所有 BRN 已掃描完畢 ===")
                break
            batch_num += 1
            total_hit += stats["hit"]
            total_miss += stats["miss"]
            print(
                f"[download_all] batch #{batch_num} 完成 | "
                f"累計 hit={total_hit:,}  miss={total_miss:,}"
            )

        self.close()
        if self._today_parquet is None:
            today = datetime.now().strftime("%Y%m%d")
            self._today_parquet = self.raw_dir / f"cr_brn_{today}.parquet"
        return self._today_parquet

    def get_delta(self, yesterday_parquet: Path, today_parquet: Path) -> pl.DataFrame:
        keep_cols = ["cr_no", "name_zh", "name_en", "address_raw"]

        def safe_select(path: Path) -> pl.DataFrame:
            df = pl.read_parquet(path)
            cols = [c for c in keep_cols if c in df.columns]
            return df.select(cols)

        old = safe_select(yesterday_parquet)
        new = safe_select(today_parquet)
        join_on = [c for c in keep_cols if c in old.columns and c in new.columns]
        delta = new.join(old, on=join_on, how="anti")
        logger.info(f"增量: {len(delta)} 筆新增/變動")
        return delta

    def close(self):
        if self._writer:
            self._writer.close()
            self._writer = None
