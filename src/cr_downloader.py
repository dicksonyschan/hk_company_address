"""
cr_downloader.py
透過 data.gov.hk / data.cr.gov.hk API 全量下載本地公司地址資料。

API 說明（來源：data.gov.hk resource 2bbe945a）：
  GET https://data.cr.gov.hk/cr/api/api/v1/api_builder/json/local/search
  必要參數：
    query[0][key1] = Comp_name | Brn
    query[0][key2] = begins_with | equal
    query[0][key3] = <查詢值>
    format        = json | csv | xml

  限制：
    - 每次最多回傳 100 筆（max/skip 參數無效）
    - Brn 只支援 equal，不支援 begins_with
    - 無需認證

全量下載策略（遞迴前綴展開）：
  1. 以單字母前綴（A–Z、0–9、中文常用首字）發起請求
  2. 若回傳 == 100 筆（可能截斷），自動展開為 prefix+A–Z+0–9 子前綴遞迴查詢
  3. 若回傳 < 100 筆，代表該前綴已完整，直接收錄
  4. 並發執行（Semaphore 控制），串流寫入 Parquet 避免 OOM
"""

import asyncio
import logging
import string
from pathlib import Path
from datetime import datetime

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# 並發請求上限
_CONCURRENCY = 10

# 前綴展開用字元集（英文大小寫視同，以大寫處理）
_EXPAND_CHARS = list(string.ascii_uppercase) + list(string.digits) + [
    # 中文常見公司名首字（覆蓋絕大多數本地公司）
    "中", "香", "大", "新", "東", "南", "北", "西", "美", "國",
    "華", "亞", "金", "星", "龍", "興", "德", "成", "富", "永",
    "信", "安", "合", "聯", "萬", "恒", "利", "康", "明", "海",
]

# 單次回傳上限（API 硬性限制）
_API_PAGE_LIMIT = 100


class CRDownloader:
    def __init__(self, config: dict):
        self.base_url = config["cr"]["base_url"]
        self.raw_dir = Path(config["cr"]["raw_dir"])
        self.timeout = config["cr"]["request_timeout"]
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ #
    # 核心 API 請求                                                         #
    # ------------------------------------------------------------------ #

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
    )
    async def _fetch_prefix(
        self, client: httpx.AsyncClient, prefix: str
    ) -> list[dict]:
        """
        查詢以 prefix 開頭的公司名稱，回傳最多 100 筆。
        若回傳恰好 100 筆，呼叫方應繼續展開子前綴。
        """
        params = {
            "query[0][key1]": "Comp_name",
            "query[0][key2]": "begins_with",
            "query[0][key3]": prefix,
            "format": "json",
        }
        resp = await client.get(
            self.base_url,
            params=params,
            timeout=self.timeout,
            headers={"Accept": "application/json"},
        )
        if resp.status_code != 200:
            logger.warning(
                f"prefix={repr(prefix)} → HTTP {resp.status_code}: {resp.text[:200]}"
            )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    # ------------------------------------------------------------------ #
    # 遞迴前綴展開                                                          #
    # ------------------------------------------------------------------ #

    async def _collect_prefix(
        self,
        client: httpx.AsyncClient,
        sem: asyncio.Semaphore,
        prefix: str,
        depth: int = 0,
    ) -> list[dict]:
        """
        遞迴收集 prefix 下的所有記錄：
        - 回傳 < 100 → 直接返回（已完整）
        - 回傳 == 100 → 繼續展開 prefix+char 子前綴
        depth 限制最深遞迴層數（防止極端 case 無限展開）。
        """
        async with sem:
            records = await self._fetch_prefix(client, prefix)

        if len(records) < _API_PAGE_LIMIT or depth >= 6:
            if depth >= 6 and len(records) == _API_PAGE_LIMIT:
                logger.warning(
                    f"prefix={repr(prefix)} 已達最大深度 {depth}，"
                    f"可能仍有截斷（{len(records)} 筆）"
                )
            return records

        # 需要展開：並發查詢所有子前綴
        logger.debug(f"prefix={repr(prefix)} 回傳 {len(records)} 筆，展開子前綴（depth={depth}）")
        sub_tasks = [
            self._collect_prefix(client, sem, prefix + ch, depth + 1)
            for ch in _EXPAND_CHARS
        ]
        sub_results = await asyncio.gather(*sub_tasks, return_exceptions=True)

        all_records: list[dict] = []
        for ch, result in zip(_EXPAND_CHARS, sub_results):
            if isinstance(result, Exception):
                logger.error(f"子前綴 {repr(prefix + ch)} 失敗：{result}")
            else:
                all_records.extend(result)
        return all_records

    # ------------------------------------------------------------------ #
    # 公開介面                                                              #
    # ------------------------------------------------------------------ #

    async def download_all(self, resume: bool = True) -> Path:
        """
        全量下載所有本地公司地址資料。
        結果串流寫入 Parquet 檔（data/raw/cr_raw_YYYYMMDD.parquet）。
        """
        today = datetime.now().strftime("%Y%m%d")
        output_parquet = self.raw_dir / f"cr_raw_{today}.parquet"
        fetched_at = datetime.now().isoformat()

        if resume and output_parquet.exists():
            logger.info(f"發現今日 Parquet 已存在：{output_parquet}，直接使用")
            return output_parquet

        logger.info(f"=== 開始全量下載 CR 資料（並發: {_CONCURRENCY}）===")

        sem = asyncio.Semaphore(_CONCURRENCY)
        writer: pq.ParquetWriter | None = None
        schema: pa.Schema | None = None
        total_records = 0
        seen_brns: set[str] = set()  # 去重（跨前綴可能重複）

        async with httpx.AsyncClient(http2=True) as client:
            # 第一層：並發所有頂層前綴
            top_tasks = [
                self._collect_prefix(client, sem, ch)
                for ch in _EXPAND_CHARS
            ]
            logger.info(f"發起 {len(_EXPAND_CHARS)} 個頂層前綴查詢...")
            top_results = await asyncio.gather(*top_tasks, return_exceptions=True)

        # 寫入 Parquet
        for ch, result in zip(_EXPAND_CHARS, top_results):
            if isinstance(result, Exception):
                logger.error(f"頂層前綴 {repr(ch)} 失敗：{result}")
                continue

            # 去重 + 加 fetched_at
            new_records = []
            for r in result:
                brn = r.get("Brn", "")
                if brn and brn in seen_brns:
                    continue
                if brn:
                    seen_brns.add(brn)
                r["fetched_at"] = fetched_at
                new_records.append(r)

            if not new_records:
                continue

            df_chunk = pl.DataFrame(new_records)
            # 統一欄位名稱為 snake_case
            rename_map = {
                "Brn": "cr_no",
                "Chinese_Company_Name": "name_zh",
                "English_Company_Name": "name_en",
                "Address_of_Registered_Office": "address_raw",
                "Company_Type": "company_type",
                "Date_of_Incorporation": "date_of_incorporation",
                "Re-domiciliation_Date": "re_domiciliation_date",
            }
            existing_renames = {k: v for k, v in rename_map.items() if k in df_chunk.columns}
            df_chunk = df_chunk.rename(existing_renames)
            # 其餘欄位轉 lower snake_case
            df_chunk = df_chunk.rename(
                {c: c.lower().replace(" ", "_").replace("-", "_")
                 for c in df_chunk.columns if c not in existing_renames.values()}
            )

            arrow_batch = df_chunk.to_arrow()
            if writer is None:
                schema = arrow_batch.schema
                writer = pq.ParquetWriter(output_parquet, schema, compression="snappy")
            else:
                try:
                    arrow_batch = arrow_batch.cast(schema)
                except Exception:
                    # schema 略有差異時做 best-effort 對齊
                    for field in schema:
                        if field.name not in df_chunk.columns:
                            df_chunk = df_chunk.with_columns(
                                pl.lit(None).cast(pl.Utf8).alias(field.name)
                            )
                    arrow_batch = df_chunk.select(
                        [f.name for f in schema]
                    ).to_arrow().cast(schema)

            writer.write_table(arrow_batch)
            total_records += len(new_records)
            logger.info(f"前綴 {repr(ch):>6}: {len(new_records):>5} 筆（累計 {total_records:>7}）")

        if writer:
            writer.close()

        logger.info(f"=== 下載完成，共 {total_records} 筆（去重後），輸出：{output_parquet} ===")
        return output_parquet

    def get_delta(self, yesterday_parquet: Path, today_parquet: Path) -> pl.DataFrame:
        """
        增量模式：比較昨日與今日資料，回傳新增／變動的記錄。
        """
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
