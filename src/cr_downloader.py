"""
cr_downloader.py
從 data.cr.gov.hk 分頁下載本地公司地址資料，支援斷點續傳。
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

logger = logging.getLogger(__name__)


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
        params = {
            "max": self.page_size,
            "skip": skip,
        }
        resp = await client.get(
            self.base_url,
            params=params,
            timeout=self.timeout,
            headers={"Accept": "application/json", "Accept-Language": "zh-Hant"},
        )
        resp.raise_for_status()
        data = resp.json()
        # CR API 回傳結構: {"company": [...]} 或直接 list
        if isinstance(data, list):
            return data
        return data.get("company", data.get("result", []))

    def _get_last_page(self) -> int:
        """從已下載檔案推算斷點，支援續傳。"""
        pages = sorted(self.raw_dir.glob("page_*.json"))
        if not pages:
            return 0
        last = pages[-1].stem  # e.g. 'page_00042'
        return int(last.split("_")[1])

    async def download_all(self, resume: bool = True) -> Path:
        """
        全量下載所有分頁，存成 page_XXXXX.json。
        resume=True 時從上次斷點續傳。
        最後合併成單一 Parquet 檔回傳路徑。
        """
        start_page = self._get_last_page() if resume else 0
        today = datetime.now().strftime("%Y%m%d")
        output_parquet = self.raw_dir / f"cr_raw_{today}.parquet"

        logger.info(f"開始下載 CR 資料，從第 {start_page} 頁起")

        async with httpx.AsyncClient(http2=True) as client:
            skip = start_page * self.page_size
            page_num = start_page

            while True:
                records = await self._fetch_page(client, skip)
                if not records:
                    logger.info(f"下載完成，共 {page_num} 頁")
                    break

                page_file = self.raw_dir / f"page_{page_num:05d}.json"
                page_file.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
                logger.info(f"  頁 {page_num:05d}: {len(records)} 筆")

                skip += self.page_size
                page_num += 1

                # 禮貌性延遲，避免過快請求政府伺服器
                await asyncio.sleep(0.3)

        # 合併所有 JSON 頁為 Parquet
        self._merge_to_parquet(output_parquet)
        return output_parquet

    def _merge_to_parquet(self, output_path: Path):
        """把所有 page_*.json 合併成一個 Parquet 檔。"""
        all_records = []
        for f in sorted(self.raw_dir.glob("page_*.json")):
            all_records.extend(json.loads(f.read_text(encoding="utf-8")))

        if not all_records:
            logger.warning("無資料可合併")
            return

        df = pl.DataFrame(all_records)
        # 統一欄位名稱（CR API 欄位名可能因語言設定略有不同）
        df = df.rename({c: c.lower().strip() for c in df.columns})
        df = df.with_columns(pl.lit(datetime.now().isoformat()).alias("fetched_at"))
        df.write_parquet(output_path)
        logger.info(f"已合併 {len(all_records)} 筆到 {output_path}")

    def get_delta(self, yesterday_parquet: Path, today_parquet: Path) -> pl.DataFrame:
        """
        增量模式：比較昨日與今日資料，回傳新增／變動的記錄。
        """
        old = pl.read_parquet(yesterday_parquet).select(["cr_no", "address"])
        new = pl.read_parquet(today_parquet).select(["cr_no", "address"])
        # Anti-join: 今日有、昨日無 或 地址有變動
        delta = new.join(old, on=["cr_no", "address"], how="anti")
        logger.info(f"增量: {len(delta)} 筆新增/變動")
        return delta
