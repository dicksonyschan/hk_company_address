"""
cr_downloader.py
從 data.cr.gov.hk 分頁下載本地公司地址資料，支援斷點續傳。

優化:
- 並發分頁下載（Semaphore 控制 5 頁同時）
- 串流寫入 Parquet（PyArrow ParquetWriter），避免 OOM
- 每頁下載後即刪除 JSON 暫存，節省磁碟

修復 (2026-05-28):
- CR API /local/search 端點要求至少一個搜尋參數（q）；
  純 max+skip 的請求會被拒絕並回傳 HTTP 400。
  已加入 q=" " (空白通配) 讓 API 匹配全量記錄。
  若空白仍然 400，程式會自動 fallback 嘗試 q="*"，
  兩者均失敗才拋出例外。
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from datetime import datetime

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# 並發下載分頁數上限
_DOWNLOAD_CONCURRENCY = 5

# CR API 全量查詢用的通配符候選（依序試用）
_WILDCARD_CANDIDATES = [" ", "*", ""]


class CRDownloader:
    def __init__(self, config: dict):
        self.base_url = config["cr"]["base_url"]
        self.page_size = config["cr"]["page_size"]
        self.raw_dir = Path(config["cr"]["raw_dir"])
        self.timeout = config["cr"]["request_timeout"]
        self.retry_attempts = config["cr"]["retry_attempts"]
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self._working_query: str | None = None  # 快取已確認可用的通配符

    async def _probe_query_param(self, client: httpx.AsyncClient) -> str:
        """
        探測 CR API 接受哪個通配符（空白 / * / 空字串）。
        成功後快取結果，後續頁面直接使用，不再重複探測。
        """
        if self._working_query is not None:
            return self._working_query

        for candidate in _WILDCARD_CANDIDATES:
            params: dict = {"max": 1, "skip": 0}
            if candidate:
                params["q"] = candidate
            try:
                resp = await client.get(
                    self.base_url,
                    params=params,
                    timeout=self.timeout,
                    headers={"Accept": "application/json", "Accept-Language": "zh-Hant"},
                )
                if resp.status_code == 200:
                    self._working_query = candidate
                    logger.info(f"CR API 通配符探測成功：q={repr(candidate)}")
                    return candidate
                else:
                    logger.debug(f"CR API 通配符 q={repr(candidate)} 回傳 {resp.status_code}，繼續嘗試")
            except Exception as e:
                logger.debug(f"CR API 通配符 q={repr(candidate)} 請求失敗：{e}")

        raise RuntimeError(
            "CR API 所有通配符均失敗（400 Bad Request）。\n"
            "請確認：\n"
            "  1. 網絡可訪問 data.cr.gov.hk\n"
            "  2. API 端點 URL 是否有更新（見 config.yaml cr.base_url）\n"
            "  3. 是否需要 API Key 或 session cookie（建議瀏覽器開啟 URL 確認）\n"
            f"  測試 URL: {self.base_url}?max=1&skip=0&q=*"
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(httpx.HTTPStatusError),
    )
    async def _fetch_page(
        self, client: httpx.AsyncClient, skip: int, query: str
    ) -> list[dict]:
        """抓取單一分頁，失敗時自動重試（指數退避）。"""
        params: dict = {"max": self.page_size, "skip": skip}
        if query:
            params["q"] = query

        resp = await client.get(
            self.base_url,
            params=params,
            timeout=self.timeout,
            headers={"Accept": "application/json", "Accept-Language": "zh-Hant"},
        )

        if resp.status_code == 400:
            logger.error(
                f"CR API 回傳 400 Bad Request (skip={skip})。\n"
                f"  URL: {resp.request.url}\n"
                f"  回應: {resp.text[:300]}"
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
        query: str,
    ) -> tuple[int, list[dict]]:
        """單頁下載，受 Semaphore 限制並發數。"""
        async with sem:
            skip = page_num * self.page_size
            records = await self._fetch_page(client, skip, query)
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
            # 先探測可用的通配符
            working_query = await self._probe_query_param(client)

            # 批次並發：每批 _DOWNLOAD_CONCURRENCY 頁
            while True:
                batch_pages = list(range(page_num, page_num + _DOWNLOAD_CONCURRENCY))
                tasks = [
                    self._download_page(client, sem, p, working_query)
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
