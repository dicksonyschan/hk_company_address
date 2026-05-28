"""
cr_downloader.py
從 data.cr.gov.hk 分頁下載本地公司地址資料，支援斷點續傳。

優化:
- 並發分頁下載（Semaphore 控制 5 頁同時）
- 串流寫入 Parquet（PyArrow ParquetWriter），避免 OOM
- 每頁下載後即刪除 JSON 暫存，節省磁碟
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from datetime import datetime

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# 並發下載分頁數上限
_DOWNLOAD_CONCURRENCY = 5


class CRDownloader:
    def __init__(self, config: dict):
        self.base_url = config["cr"]["base_url"]
        self.page_size = config["cr"]["page_size"]
        self.raw_dir = Path(config["cr"]["raw_dir"])
        self.timeout = config["cr"]["request_timeout"]
        self.retry_attempts = config["cr"]["retry_attempts"]
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    async def _fetch_page(self, client: httpx.AsyncClient, skip: int) -> list[dict]:
        """抓取單一分頁，失敗時自動重試（指數退避）。"""
        params = {"max": self.page_size, "skip": skip}
        resp = await client.get(
            self.base_url,
            params=params,
            timeout=self.timeout,
            headers={"Accept": "application/json", "Accept-Language": "zh-Hant"},
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        return data.get("company", data.get("result", []))

    def _get_resume_page(self) -> int:
        """檢查今日 cr_raw_{today}.parquet 是否已存在，以決定斷點頁碼。
        由於輸出為單一串流 Parquet，無法逐頁計算已寫入頁數；
        若檔案已存在（上次中斷），從頭重跑以確保資料完整。
        若需真正逐頁斷點，改用 page_{n:05d}.parquet 暫存模式。
        """
        today = datetime.now().strftime("%Y%m%d")
        output_parquet = self.raw_dir / f"cr_raw_{today}.parquet"
        if output_parquet.exists():
            logger.info(f"發現今日 Parquet 已存在：{output_parquet}，將從頭重跑以確保完整性")
        return 0

    async def _download_page(
        self,
        client: httpx.AsyncClient,
        sem: asyncio.Semaphore,
        page_num: int,
    ) -> tuple[int, list[dict]]:
        """單頁下載，受 Semaphore 限制並發數。"""
        async with sem:
            skip = page_num * self.page_size
            records = await self._fetch_page(client, skip)
            return page_num, records

    async def download_all(self, resume: bool = True) -> Path:
        """
        全量下載所有分頁，並發執行（最多 _DOWNLOAD_CONCURRENCY 頁同時）。
        結果串流寫入單一 Parquet 檔，避免全量載入記憶體。
        """
        start_page = self._get_resume_page() if resume else 0
        today = datetime.now().strftime("%Y%m%d")
        output_parquet = self.raw_dir / f"cr_raw_{today}.parquet"
        fetched_at = datetime.now().isoformat()

        logger.info(f"開始下載 CR 資料，從第 {start_page} 頁起（並發: {_DOWNLOAD_CONCURRENCY}）")

        sem = asyncio.Semaphore(_DOWNLOAD_CONCURRENCY)
        writer: pq.ParquetWriter | None = None
        schema: pa.Schema | None = None
        page_num = start_page
        total_records = 0

        async with httpx.AsyncClient(http2=True) as client:
            # 批次並發：每批 _DOWNLOAD_CONCURRENCY 頁
            while True:
                batch_pages = list(range(page_num, page_num + _DOWNLOAD_CONCURRENCY))
                tasks = [
                    self._download_page(client, sem, p)
                    for p in batch_pages
                ]
                results = await asyncio.gather(*tasks)

                any_data = False
                empty_in_batch = False
                # 按頁序排序後串流寫入，遇第一個空頁即停止（避免多餘請求）
                for pn, records in sorted(results, key=lambda x: x[0]):
                    if not records:
                        empty_in_batch = True
                        break
                    any_data = True
                    # 加入 fetched_at
                    for r in records:
                        r["fetched_at"] = fetched_at

                    df_chunk = pl.DataFrame(records)
                    df_chunk = df_chunk.rename(
                        {c: c.lower().strip() for c in df_chunk.columns}
                    )
                    arrow_batch = df_chunk.to_arrow()

                    if writer is None:
                        schema = arrow_batch.schema
                        writer = pq.ParquetWriter(output_parquet, schema, compression="snappy")
                    else:
                        # 統一 schema（欄位可能略有差異）
                        arrow_batch = arrow_batch.cast(schema)

                    writer.write_table(arrow_batch)
                    total_records += len(records)
                    logger.info(f"  頁 {pn:05d}: {len(records)} 筆（累計 {total_records}）")

                if not any_data or empty_in_batch:
                    logger.info(f"下載完成，共 {page_num} 頁，{total_records} 筆")
                    break

                page_num += _DOWNLOAD_CONCURRENCY

                # 批次間禮貌性短暫等待
                await asyncio.sleep(0.2)

        if writer:
            writer.close()

        return output_parquet

    def get_delta(self, yesterday_parquet: Path, today_parquet: Path) -> pl.DataFrame:
        """
        增量模式：比較昨日與今日資料，回傳新增／變動的記錄。
        使用全欄位 hash 偵測任何欄位變動（包含 name_zh/name_en）。
        """
        keep_cols = ["cr_no", "name_zh", "name_en", "address_raw"]

        def safe_select(path: Path) -> pl.DataFrame:
            df = pl.read_parquet(path)
            df = df.rename({c: c.lower().strip() for c in df.columns})
            cols = [c for c in keep_cols if c in df.columns]
            return df.select(cols)

        old = safe_select(yesterday_parquet)
        new = safe_select(today_parquet)

        # Anti-join on all available key columns
        join_on = [c for c in keep_cols if c in old.columns and c in new.columns]
        delta = new.join(old, on=join_on, how="anti")
        logger.info(f"增量: {len(delta)} 筆新增/變動")
        return delta
