"""
pipeline.py
串接所有步驟：下載 CR → 清洗 → ALS 標準化 → 寫入 DuckDB。
支援 full（全量）與 delta（增量）兩種模式。

優化:
- master records 改用 Polars 向量化組裝（取代逐行 dict append）
- flush 門檻由 1000 提升至 5000，減少 IO 次數
- pipeline 結束時呼叫 als.aclose() 關閉持久化 HTTP client
"""

import asyncio
import logging
from pathlib import Path

import polars as pl

from .cr_downloader import CRDownloader
from .address_cleaner import AddressCleaner
from .als_client import ALSClient
from .db_writer import DBWriter

logger = logging.getLogger(__name__)

CR_FIELD_MAP = {
    "companyno": "cr_no",
    "company_no": "cr_no",
    "namechinese": "name_zh",
    "nameenglish": "name_en",
    "address": "address_raw",
    "registeredofficeaddress": "address_raw",
}

_FLUSH_SIZE = 5000  # 每批寫入 master 的筆數


class Pipeline:
    def __init__(self, config: dict):
        self.config = config
        self.db = DBWriter(config["db"]["path"])
        self.cleaner = AddressCleaner(config["alias_map_path"])
        self.als = ALSClient(config, self.db)
        self.downloader = CRDownloader(config)

    def _normalize_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        col_map = {}
        for col in df.columns:
            mapped = CR_FIELD_MAP.get(col.lower().strip())
            if mapped:
                col_map[col] = mapped
        if col_map:
            df = df.rename(col_map)
        for required in ["cr_no", "name_zh", "name_en", "address_raw"]:
            if required not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(required))
        return df

    def _build_master_df(
        self,
        df: pl.DataFrame,
        addresses_clean: list[str],
        als_results: list[dict | None],
    ) -> pl.DataFrame:
        """
        用 Polars 向量化組裝 master DataFrame，
        取代逐行 dict append（大資料量快 10-30x）。
        """
        als_cols = ["geo_address", "region", "district", "street_name",
                    "building_name", "latitude", "longitude", "score", "manual_review"]

        # 把 als_results（含 None）轉為 dict of lists
        als_data: dict[str, list] = {c: [] for c in als_cols}
        for res in als_results:
            r = res or {}
            for c in als_cols:
                als_data[c].append(r.get(c))

        als_df = pl.DataFrame(als_data)

        base = df.select(["cr_no", "name_zh", "name_en", "address_raw"]).with_columns(
            pl.Series("address_clean", addresses_clean)
        )
        return pl.concat([base, als_df], how="horizontal")

    async def run_full(self):
        logger.info("=== 全量模式開始 ===")

        parquet_path = await self.downloader.download_all(resume=True)
        df = pl.read_parquet(parquet_path)
        df = self._normalize_columns(df)
        self.db.write_raw(df)

        addresses_raw = df["address_raw"].to_list()
        addresses_clean = self.cleaner.clean_batch(addresses_raw)
        als_results = await self.als.process_batch(addresses_clean)

        # Polars 向量化組裝
        master_df = self._build_master_df(df, addresses_clean, als_results)

        # 分批 flush（每 _FLUSH_SIZE 筆）
        records = master_df.to_dicts()
        for i in range(0, len(records), _FLUSH_SIZE):
            self.db.write_master(records[i: i + _FLUSH_SIZE])

        summary = self.db.summary()
        logger.info(f"=== 完成 === {summary}")
        print("\n=== 執行摘要 ===")
        for k, v in summary.items():
            print(f"  {k}: {v}")

    async def run_delta(self, yesterday_parquet: str):
        logger.info("=== 增量模式開始 ===")

        today_parquet = await self.downloader.download_all(resume=False)
        delta_df = self.downloader.get_delta(Path(yesterday_parquet), today_parquet)

        if len(delta_df) == 0:
            logger.info("無新增/變動記錄，跳過 ALS 呼叫")
            return

        delta_df = self._normalize_columns(delta_df)
        addresses_clean = self.cleaner.clean_batch(delta_df["address_raw"].to_list())
        als_results = await self.als.process_batch(addresses_clean)

        master_df = self._build_master_df(delta_df, addresses_clean, als_results)
        records = master_df.to_dicts()
        for i in range(0, len(records), _FLUSH_SIZE):
            self.db.write_master(records[i: i + _FLUSH_SIZE])

        summary = self.db.summary()
        logger.info(f"=== 增量完成 === {summary}")

    def close(self):
        asyncio.get_event_loop().run_until_complete(self.als.aclose())
        self.db.close()
