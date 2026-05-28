"""
pipeline.py
串接所有步驟：下載 CR → 清洗 → ALS 標準化 → 寫入 DuckDB。
支援 full（全量）與 delta（增量）兩種模式。

BRN 模式特別說明：
  - run_full() 偵測到 CRDownloaderBrn 時，改為「每 batch 即時處理」模式。
  - download_all(on_hit=...) 每批有 hit 就觸發 callback → ALS + 寫 master。
  - 不需等全量掃完，Ctrl+C 後已寫入的 master 資料不會丟失。
"""

import asyncio
import logging
from pathlib import Path

import polars as pl

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

_FLUSH_SIZE = 5000
_EXTRA_COLS = ["company_type", "date_of_incorporation", "re_domiciliation_date"]


class Pipeline:
    def __init__(self, config: dict):
        self.config = config
        self.db = DBWriter(config["db"]["path"])
        self.cleaner = AddressCleaner(config["alias_map_path"])
        self.als = ALSClient(config, self.db)

        downloader_type = config.get("cr", {}).get("downloader", "prefix")
        if downloader_type == "brn":
            from .cr_downloader_brn import CRDownloaderBrn
            self.downloader = CRDownloaderBrn(config, db=self.db)
            logger.info("使用 CRDownloaderBrn（BRN 盲查模式）")
        else:
            from .cr_downloader import CRDownloader
            self.downloader = CRDownloader(config)
            logger.info(f"使用 CRDownloader（前綴掃描模式，downloader={downloader_type!r})")

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
        for col in _EXTRA_COLS:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
        return df

    def _build_master_df(
        self,
        df: pl.DataFrame,
        addresses_clean: list[str],
        als_results: list[dict | None],
    ) -> pl.DataFrame:
        als_cols = ["geo_address", "region", "district", "street_name",
                    "building_name", "latitude", "longitude", "score", "manual_review"]
        als_data: dict[str, list] = {c: [] for c in als_cols}
        for res in als_results:
            r = res or {}
            for c in als_cols:
                als_data[c].append(r.get(c))
        als_df = pl.DataFrame(als_data)
        base_cols = ["cr_no", "name_zh", "name_en", "address_raw"] + _EXTRA_COLS
        base = df.select(base_cols).with_columns(
            pl.Series("address_clean", addresses_clean)
        )
        return pl.concat([base, als_df], how="horizontal")

    async def _process_hit_df(self, df: pl.DataFrame):
        """
        BRN 模式專用：接收單批 hit DataFrame，做 ALS + 寫入 master。
        由 download_all(on_hit=...) 每批觸發。
        """
        df = self._normalize_columns(df)

        # 跳過 master 已有的 cr_no
        try:
            existing = {r[0] for r in self.db.con.execute("SELECT cr_no FROM master").fetchall()}
            if existing:
                df = df.filter(~pl.col("cr_no").is_in(existing))
        except Exception:
            pass

        if len(df) == 0:
            return

        self.db.write_raw(df)

        addresses_clean = self.cleaner.clean_batch(df["address_raw"].to_list())
        als_results = await self.als.process_batch(addresses_clean)
        master_df = self._build_master_df(df, addresses_clean, als_results)

        records = master_df.to_dicts()
        for i in range(0, len(records), _FLUSH_SIZE):
            self.db.write_master(records[i: i + _FLUSH_SIZE])

        logger.info(f"master 已寫入 {len(records)} 筆（本批）")

    async def run_full(self):
        logger.info("=== 全量模式開始 ===")

        from .cr_downloader_brn import CRDownloaderBrn
        if isinstance(self.downloader, CRDownloaderBrn):
            # BRN 模式：每 batch 即時 ALS + 寫 master
            logger.info("BRN 模式：每批完成後立即寫入 master")
            await self.downloader.download_all(
                resume=True,
                on_hit=self._process_hit_df,
            )
        else:
            # 原有前綴掃描模式：全量下載完再處理
            parquet_path = await self.downloader.download_all(resume=True)
            df = pl.scan_parquet(parquet_path).collect()
            df = self._normalize_columns(df)
            self.db.write_raw(df)

            existing_cr_nos: set[str] = set()
            try:
                rows = self.db.con.execute("SELECT cr_no FROM master").fetchall()
                existing_cr_nos = {r[0] for r in rows}
                logger.info(f"checkpoint: master 已有 {len(existing_cr_nos)} 筆，將跳過")
            except Exception:
                pass

            if existing_cr_nos:
                df = df.filter(~pl.col("cr_no").is_in(existing_cr_nos))

            if len(df) > 0:
                addresses_clean = self.cleaner.clean_batch(df["address_raw"].to_list())
                als_results = await self.als.process_batch(addresses_clean)
                master_df = self._build_master_df(df, addresses_clean, als_results)
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
        self.db.write_raw(delta_df)
        addresses_clean = self.cleaner.clean_batch(delta_df["address_raw"].to_list())
        als_results = await self.als.process_batch(addresses_clean)
        master_df = self._build_master_df(delta_df, addresses_clean, als_results)
        records = master_df.to_dicts()
        for i in range(0, len(records), _FLUSH_SIZE):
            self.db.write_master(records[i: i + _FLUSH_SIZE])

        summary = self.db.summary()
        logger.info(f"=== 增量完成 === {summary}")

    def close(self):
        asyncio.run(self.als.aclose())
        self.db.close()
