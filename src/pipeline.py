"""
pipeline.py
串接所有步驟：下載 CR → 清洗 → ALS 標準化 → 寫入 DuckDB。
支援 full（全量）與 delta（增量）兩種模式。

BRN 模式說明：
  - run_full() 對 BRN 模式：只做 CR 下載 + 寫入 companies_raw，不呼叫 ALS。
  - ALS 地址處理改用獨立指令：
      python main.py --process-als
  - 好處：大量下載時 ALS 失敗不影響已存的 CR 資料。
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

    async def _save_raw_only(self, df: pl.DataFrame):
        """
        BRN 模式用：只將 hit 資料寫入 companies_raw，不呼叫 ALS。
        ALS 處理實復改用 --process-als 獨立執行。
        """
        df = self._normalize_columns(df)
        if len(df) == 0:
            return
        self.db.write_raw(df)
        logger.info(f"[raw] 寫入 companies_raw {len(df)} 筆（未呼 ALS）")

    async def process_als_from_raw(self, batch_size: int = 500):
        """
        獨立 ALS 處理步驟：從 companies_raw 讀取尚未寫入 master 的記錄，
        批次呼叫 ALS 後寫入 master。
        從 --mode full --downloader brn 完成後執行：
            python main.py --process-als
        支援斷點繼續（已在 master 的 cr_no 會被跳過）。
        """
        logger.info("=== ALS 地址處理開始 ===")

        # 已在 master 的 cr_no
        try:
            done = {r[0] for r in self.db.con.execute(
                "SELECT cr_no FROM master WHERE geo_address IS NOT NULL"
            ).fetchall()}
        except Exception:
            done = set()
        logger.info(f"master 已有地址結果: {len(done):,} 筆，將跳過")

        # 從 companies_raw 讀取尚未處理的
        try:
            all_raw = self.db.con.execute(
                "SELECT cr_no, name_zh, name_en, address_raw FROM companies_raw"
            ).fetchall()
        except Exception as e:
            logger.error(f"companies_raw 讀取失敗: {e}")
            return

        pending = [r for r in all_raw if r[0] not in done]
        logger.info(f"companies_raw 共 {len(all_raw):,} 筆，尚未處理: {len(pending):,} 筆")

        if not pending:
            logger.info("所有 raw 記錄已處理完成")
            return

        total_written = 0
        for i in range(0, len(pending), batch_size):
            chunk = pending[i: i + batch_size]
            cr_nos    = [r[0] for r in chunk]
            names_zh  = [r[1] for r in chunk]
            names_en  = [r[2] for r in chunk]
            addr_raws = [r[3] for r in chunk]

            addresses_clean = self.cleaner.clean_batch(addr_raws)
            als_results = await self.als.process_batch(addresses_clean)

            records = []
            for j, (cr_no, name_zh, name_en, addr_raw) in enumerate(zip(cr_nos, names_zh, names_en, addr_raws)):
                als = als_results[j] or {}
                records.append({
                    "cr_no":         cr_no,
                    "name_zh":       name_zh,
                    "name_en":       name_en,
                    "address_raw":   addr_raw,
                    "address_clean": addresses_clean[j],
                    "geo_address":   als.get("geo_address"),
                    "region":        als.get("region"),
                    "district":      als.get("district"),
                    "street_name":   als.get("street_name"),
                    "building_name": als.get("building_name"),
                    "latitude":      als.get("latitude"),
                    "longitude":     als.get("longitude"),
                    "score":         als.get("score", 0),
                    "manual_review": als.get("manual_review", True),
                    "company_type":  None,
                    "date_of_incorporation": None,
                    "re_domiciliation_date": None,
                })
            self.db.write_master(records)
            total_written += len(records)
            pct = min(i + batch_size, len(pending))
            logger.info(f"ALS 進度: {pct:,}/{len(pending):,} 筆（已寫 master {total_written:,}）")
            print(f"\r[process-als] {pct:,}/{len(pending):,}", end="", flush=True)

        print()
        summary = self.db.summary()
        logger.info(f"=== ALS 處理完成 === {summary}")
        return summary

    async def run_full(self):
        logger.info("=== 全量模式開始 ===")

        from .cr_downloader_brn import CRDownloaderBrn
        if isinstance(self.downloader, CRDownloaderBrn):
            # BRN 模式：只存 raw，不呼 ALS
            logger.info("BRN 模式：只存储 companies_raw，ALS 請後續執行 --process-als")
            await self.downloader.download_all(
                resume=True,
                on_hit=self._save_raw_only,
            )
        else:
            # 前綴掃描模式（不變）
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
