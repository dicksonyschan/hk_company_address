"""
pipeline.py
串接所有步驟：下載 CR → 清洗 → ALS 標準化 → 寫入 DuckDB。
支援 full（全量）與 delta（增量）兩種模式。
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

# CR API 欄位名稱映射（API 可能因版本略有差異，統一在此處理）
CR_FIELD_MAP = {
    # 可能的 CR JSON 欄位名 -> 標準欄位名
    "companyno": "cr_no",
    "company_no": "cr_no",
    "namechinese": "name_zh",
    "nameenglish": "name_en",
    "address": "address_raw",
    "registeredofficeaddress": "address_raw",
}


class Pipeline:
    def __init__(self, config: dict):
        self.config = config
        self.db = DBWriter(config["db"]["path"])
        self.cleaner = AddressCleaner(config["alias_map_path"])
        self.als = ALSClient(config, self.db)
        self.downloader = CRDownloader(config)

    def _normalize_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """統一 CR 欄位名稱。"""
        col_map = {}
        for col in df.columns:
            mapped = CR_FIELD_MAP.get(col.lower().strip())
            if mapped:
                col_map[col] = mapped
        if col_map:
            df = df.rename(col_map)
        # 確保必要欄位存在
        for required in ["cr_no", "name_zh", "name_en", "address_raw"]:
            if required not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(required))
        return df

    async def run_full(self):
        """全量模式：下載全部 CR 資料並處理。"""
        logger.info("=== 全量模式開始 ===")

        # 1. 下載
        parquet_path = await self.downloader.download_all(resume=True)

        # 2. 讀取 & 正規化欄位
        df = pl.read_parquet(parquet_path)
        df = self._normalize_columns(df)
        self.db.write_raw(df)

        # 3. 清洗地址
        addresses_raw = df["address_raw"].to_list()
        addresses_clean = self.cleaner.clean_batch(addresses_raw)

        # 4. ALS 標準化
        als_results = await self.als.process_batch(addresses_clean)

        # 5. 建構 master 記錄
        master_records = []
        for i, row in enumerate(df.iter_rows(named=True)):
            als = als_results[i] or {}
            master_records.append({
                "cr_no": row.get("cr_no"),
                "name_zh": row.get("name_zh"),
                "name_en": row.get("name_en"),
                "address_raw": row.get("address_raw"),
                "address_clean": addresses_clean[i],
                **{k: als.get(k) for k in [
                    "geo_address", "region", "district",
                    "street_name", "building_name",
                    "latitude", "longitude", "score", "manual_review"
                ]},
            })

            # 每 1000 筆 flush 一次
            if len(master_records) >= 1000:
                self.db.write_master(master_records)
                master_records = []

        if master_records:
            self.db.write_master(master_records)

        # 6. 摘要
        summary = self.db.summary()
        logger.info(f"=== 完成 === {summary}")
        print("\n=== 執行摘要 ===")
        for k, v in summary.items():
            print(f"  {k}: {v}")

    async def run_delta(self, yesterday_parquet: str):
        """增量模式：只處理 CR 新增/變動的記錄。"""
        logger.info("=== 增量模式開始 ===")

        today_parquet = await self.downloader.download_all(resume=False)
        delta_df = self.downloader.get_delta(
            Path(yesterday_parquet), today_parquet
        )

        if len(delta_df) == 0:
            logger.info("無新增/變動記錄，跳過 ALS 呼叫")
            return

        delta_df = self._normalize_columns(delta_df)
        addresses_clean = self.cleaner.clean_batch(delta_df["address_raw"].to_list())
        als_results = await self.als.process_batch(addresses_clean)

        master_records = []
        for i, row in enumerate(delta_df.iter_rows(named=True)):
            als = als_results[i] or {}
            master_records.append({
                "cr_no": row.get("cr_no"),
                "name_zh": row.get("name_zh"),
                "name_en": row.get("name_en"),
                "address_raw": row.get("address_raw"),
                "address_clean": addresses_clean[i],
                **{k: als.get(k) for k in [
                    "geo_address", "region", "district",
                    "street_name", "building_name",
                    "latitude", "longitude", "score", "manual_review"
                ]},
            })

        self.db.write_master(master_records)
        summary = self.db.summary()
        logger.info(f"=== 增量完成 === {summary}")

    def close(self):
        self.db.close()
