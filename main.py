"""
main.py
主程式入口，支援 CLI 參數。

BRN 模式建議兩階段實復：
  階段 1：下載 CR 資料（只存 raw，不呼 ALS）
    python main.py --mode full --downloader brn

  階段 2：獨立地址處理（可多次執行，支援斷點繼續）
    python main.py --process-als

其他用法：
  python main.py --mode full              # 全量（前綴掃描）
  python main.py --mode delta --yesterday data/raw/cr_raw_20260527.parquet
  python main.py --lookup "290 UN CHAU STREET CHEUNG SHA WAN"  # 單筆查詢
  python main.py --init-brn-queue         # 初始化 BRN 佇列
  python main.py --scan-status            # 查看 BRN 掃描進度
  python main.py --test-brn               # 渫渫：指定 BRN 範圍一鍵渫渫
"""

import asyncio
import logging
import sys
from pathlib import Path

import click
import yaml


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(config: dict):
    log_cfg = config.get("logging", {})
    log_file = log_cfg.get("file", "data/pipeline.log")
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, log_cfg.get("level", "INFO")),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


@click.command()
@click.option("--mode", type=click.Choice(["full", "delta"]), default="full",
              help="full=全量, delta=增量")
@click.option("--yesterday", default=None,
              help="增量模式: 昨日 Parquet 路徑")
@click.option("--lookup", default=None,
              help="單筆地址查詢（渫渫用）")
@click.option("--init-hsic", is_flag=True, default=False,
              help="載入 HSIC 行業代碼表")
@click.option("--tag-industry", is_flag=True, default=False,
              help="對 master 表執行行業標籤")
@click.option("--downloader", type=click.Choice(["prefix", "brn"]), default="prefix",
              help="下載器類型")
@click.option("--init-brn-queue", is_flag=True, default=False,
              help="初始化 brn_scan_queue 佇列")
@click.option("--scan-status", is_flag=True, default=False,
              help="印出 brn_scan_queue 進度統計")
@click.option("--process-als", "process_als", is_flag=True, default=False,
              help="獨立 ALS 地址處理：從 companies_raw 讀取尚未處理的記錄寫入 master")
@click.option("--als-batch-size", default=500, show_default=True,
              help="--process-als 每批處理筆數")
@click.option("--test-brn", is_flag=True, default=False,
              help="渫渫模式：對指定 BRN 範圍執行完整流程")
@click.option("--brn-start", default="71807826",
              help="--test-brn 起始 BRN（預設: 71807826）")
@click.option("--brn-end", default="71807828",
              help="--test-brn 結束 BRN（預設: 71807828）")
@click.option("--config", "config_path", default="config.yaml",
              help="設定檔路徑")
def main(mode, yesterday, lookup,
         init_hsic, tag_industry,
         downloader, init_brn_queue, scan_status,
         process_als, als_batch_size,
         test_brn, brn_start, brn_end,
         config_path):
    config = load_config(config_path)
    setup_logging(config)
    logger = logging.getLogger("main")

    config.setdefault("cr", {})["downloader"] = downloader

    # --test-brn 渫渫模式
    if test_brn:
        asyncio.run(_run_test_brn(config, brn_start, brn_end, logger))
        return

    # --process-als 獨立 ALS 處理
    if process_als:
        from src.pipeline import Pipeline
        pipeline = Pipeline(config)
        try:
            summary = asyncio.run(pipeline.process_als_from_raw(batch_size=als_batch_size))
            if summary:
                print("\n=== ALS 處理摘要 ===")
                for k, v in summary.items():
                    print(f"  {k}: {v}")
        except KeyboardInterrupt:
            logger.info("使用者中斷，已處理的記錄已儲存")
        finally:
            pipeline.close()
        return

    # --lookup 單筆查詢
    if lookup:
        from src.address_cleaner import AddressCleaner
        from src.db_writer import DBWriter
        from src.als_client import ALSClient

        cleaner = AddressCleaner(config["alias_map_path"])
        db = DBWriter(config["db"]["path"])
        als = ALSClient(config, db)

        cleaned = cleaner.clean(lookup)
        print(f"清洗後: {cleaned}")

        result = asyncio.run(als.process_batch([cleaned]))
        if result and result[0]:
            r = result[0]
            print(f"地區: {r.get('region')}")
            print(f"分區: {r.get('district')}")
            print(f"街道: {r.get('street_name')}")
            print(f"大廈: {r.get('building_name')}")
            print(f"GeoAddress: {r.get('geo_address')}")
            print(f"信心分數: {r.get('score')}")
            print(f"需人工覆核: {r.get('manual_review')}")
        else:
            print("無法標準化此地址")
        asyncio.run(als.aclose())
        db.close()
        return

    # --init-brn-queue
    if init_brn_queue:
        from src.db_writer import DBWriter
        brn_cfg = config.get("cr_brn", {})
        db = DBWriter(config["db"]["path"])
        logger.info("=== 初始化 BRN 佇列 ===")
        total = db.init_brn_queue(
            mode=brn_cfg.get("mode", "numeric"),
            prefixes=brn_cfg.get("prefixes", None),
            start=brn_cfg.get("start", 0),
            end=brn_cfg.get("end", 99_999_999),
            prefix_start=brn_cfg.get("prefix_start", 1_000_000),
            prefix_end=brn_cfg.get("prefix_end", 4_000_000),
        )
        logger.info(f"=== 初始化完成，共處理 {total:,} 筆 BRN ===")
        print(f"\n初始化完成，共處理 {total:,} 筆 BRN")
        db.close()
        return

    # --scan-status
    if scan_status:
        from src.db_writer import DBWriter
        db = DBWriter(config["db"]["path"])
        progress = db.scan_progress()
        print("\n=== BRN 佇列進度 ===")
        total_q = (progress["pending"] or 0) + (progress["hit"] or 0) + (progress["miss"] or 0)
        for k, v in progress.items():
            print(f"  {k}: {v}")
        if total_q > 0:
            scanned = (progress["hit"] or 0) + (progress["miss"] or 0)
            hit_count = progress["hit"] or 0
            print(f"  scanned_pct: {scanned / total_q * 100:.2f}%")
            print(f"  hit_rate_of_scanned: {hit_count / scanned * 100:.4f}%" if scanned else "  hit_rate_of_scanned: N/A")
        db.close()
        return

    # --init-hsic
    if init_hsic:
        from src.cr_industry_codes import CRIndustryCodes
        logger.info("=== 載入 HSIC 行業代碼表 ===")
        hsic = CRIndustryCodes(config["db"]["path"])
        hsic.load()
        logger.info("=== HSIC 載入完成 ===")
        return

    # --tag-industry
    if tag_industry:
        from src.industry_tagger import IndustryTagger
        industry_cfg = config.get("industry", {})
        llm_fallback = industry_cfg.get("enable_llm_fallback", True)
        logger.info("=== 行業標籤開始 ===")
        tagger = IndustryTagger(config, config["db"]["path"])
        result = tagger.run(llm_fallback=llm_fallback)
        logger.info(f"=== 行業標籤完成 === {result}")
        for k, v in result.items():
            print(f"  {k}: {v}")
        return

    # 全量 / 增量模式
    from src.pipeline import Pipeline
    pipeline = Pipeline(config)
    try:
        if mode == "full":
            asyncio.run(pipeline.run_full())
        elif mode == "delta":
            if not yesterday:
                raise click.UsageError("delta 模式需提供 --yesterday 參數")
            asyncio.run(pipeline.run_delta(yesterday))
    except KeyboardInterrupt:
        logger.info("使用者中斷，目前進度已儲存")
    finally:
        pipeline.close()


async def _run_test_brn(config: dict, brn_start: str, brn_end: str, logger):
    """
    渫渫模式：對指定 BRN 範圍執行完整流程。
    步驟：
      1. 初始化 DBWriter
      2. 將指定 BRN 寫入 brn_scan_queue
      3. CRDownloaderBrn 查詢 CR API
      4. AddressCleaner + ALSClient
      5. 寫入 master 表
      6. 印出摘要
    """
    import re as _re
    from src.db_writer import DBWriter
    from src.address_cleaner import AddressCleaner
    from src.als_client import ALSClient

    logger.info(f"=== 渫渫模式: BRN {brn_start} – {brn_end} ===")

    def _parse_brn(s: str):
        s = s.strip().upper()
        if _re.match(r'^\d+$', s):
            return int(s), "numeric"
        return s, "alpha"

    start_val, start_type = _parse_brn(brn_start)
    end_val, end_type = _parse_brn(brn_end)

    db = DBWriter(config["db"]["path"])
    logger.info("[1/5] DuckDB 建表完成")

    if start_type == "numeric" and end_type == "numeric":
        total_q = db.init_brn_queue(mode="numeric", start=int(start_val), end=int(end_val))
    else:
        brn_list = list({brn_start.upper(), brn_end.upper()})
        rows = [(b,) for b in brn_list]
        db.con.executemany(
            "INSERT INTO brn_scan_queue (brn) VALUES (?) ON CONFLICT (brn) DO NOTHING",
            rows
        )
        total_q = len(brn_list)

    logger.info(f"[2/5] brn_scan_queue 寫入 {total_q} 筆 BRN pending")

    from src.cr_downloader_brn import CRDownloaderBrn

    hit_records = []

    async def on_hit(df):
        for row in df.to_dicts():
            hit_records.append(row)

    cr_config = dict(config)
    cr_config.setdefault("cr", {})["downloader"] = "brn"
    brn_cfg_override = dict(config.get("cr_brn", {}))
    brn_cfg_override["fetch_batch_size"] = total_q
    brn_cfg_override["concurrency"] = min(5, total_q)
    brn_cfg_override["miss_limit"] = total_q + 1
    cr_config["cr_brn"] = brn_cfg_override

    downloader = CRDownloaderBrn(cr_config, db=db)
    await downloader.download_all(resume=True, on_hit=on_hit)
    logger.info(f"[3/5] CR 查詢完成，共 hit {len(hit_records)} 筆")

    if not hit_records:
        print("\n渫渫結果: CR API 查詢無任何記錄（所有 BRN 均為 miss）")
        db.close()
        return

    import polars as pl
    df_hits = pl.DataFrame(hit_records)

    CR_FIELD_MAP = {
        "companyno": "cr_no", "company_no": "cr_no",
        "namechinese": "name_zh", "nameenglish": "name_en",
        "address": "address_raw", "registeredofficeaddress": "address_raw",
    }
    col_map = {c: CR_FIELD_MAP[c.lower().strip()]
               for c in df_hits.columns if c.lower().strip() in CR_FIELD_MAP}
    if col_map:
        df_hits = df_hits.rename(col_map)
    for req in ["cr_no", "name_zh", "name_en", "address_raw"]:
        if req not in df_hits.columns:
            df_hits = df_hits.with_columns(pl.lit(None).cast(pl.Utf8).alias(req))

    db.write_raw(df_hits)

    cleaner = AddressCleaner(config["alias_map_path"])
    als_client = ALSClient(config, db)

    addresses_clean = cleaner.clean_batch(df_hits["address_raw"].to_list())
    logger.info(f"[4/5] 地址清洗完成，送交 ALS 標準化...")
    als_results = await als_client.process_batch(addresses_clean)
    await als_client.aclose()

    als_cols = ["geo_address", "region", "district", "street_name",
                "building_name", "latitude", "longitude", "score", "manual_review"]
    extra_cols = ["company_type", "date_of_incorporation", "re_domiciliation_date"]

    master_records = []
    for i, row in enumerate(df_hits.to_dicts()):
        r = als_results[i] or {}
        master_records.append({
            "cr_no":         row.get("cr_no"),
            "name_zh":       row.get("name_zh"),
            "name_en":       row.get("name_en"),
            "address_raw":   row.get("address_raw"),
            "address_clean": addresses_clean[i],
            **{k: r.get(k) for k in als_cols},
            **{k: row.get(k) for k in extra_cols},
        })

    db.write_master(master_records)
    logger.info(f"[5/5] master 寫入完成，{len(master_records)} 筆")

    summary = db.summary()
    db.close()

    print("\n" + "=" * 50)
    print("渫渫完成——執行摘要")
    print("=" * 50)
    print(f"  BRN 範圍    : {brn_start} – {brn_end}")
    print(f"  CR hit 筆數  : {len(hit_records)}")
    print(f"  master 總筆數 : {summary['master_total']}")
    print(f"  平均信心分    : {summary['avg_confidence']}")
    print(f"  需人工覆核   : {summary['manual_review']}")
    print(f"  ALS 命中率   : {summary['hit_rate']}")
    print("=" * 50)
    print()
    print("詳細記錄：")
    for rec in master_records:
        print(f"  cr_no={rec['cr_no']} | name={rec.get('name_en') or rec.get('name_zh')}")
        print(f"    address_raw   : {rec['address_raw']}")
        print(f"    address_clean : {rec['address_clean']}")
        print(f"    geo_address   : {rec.get('geo_address')}")
        print(f"    region/district: {rec.get('region')} / {rec.get('district')}")
        print(f"    score         : {rec.get('score')}  manual_review={rec.get('manual_review')}")
        print()


if __name__ == "__main__":
    main()
