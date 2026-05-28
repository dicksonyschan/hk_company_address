"""
main.py
主程式入口，支援 CLI 參數。

BRN 模式建議流程：
  階段 1：下載 CR 資料（只存 raw，不呼 ALS）
    python main.py --mode full --downloader brn

  階段 2：獨立地址處理（可多次執行，支援斷點繼續）
    python main.py --process-als

  階段 3（定期）：檢查公司資料是否已變更
    python main.py --verify-hits                    # 檢查全部 hit
    python main.py --verify-hits --older-than 30    # 只檢查 30 天未更新的

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
@click.option("--mode", type=click.Choice(["full", "delta"]), default="full")
@click.option("--yesterday", default=None)
@click.option("--lookup", default=None, help="單筆地址查詢")
@click.option("--init-hsic", is_flag=True, default=False)
@click.option("--tag-industry", is_flag=True, default=False)
@click.option("--downloader", type=click.Choice(["prefix", "brn"]), default="prefix")
@click.option("--init-brn-queue", is_flag=True, default=False)
@click.option("--scan-status", is_flag=True, default=False)
@click.option("--process-als", "process_als", is_flag=True, default=False,
              help="獨立 ALS 地址處理：從 companies_raw 寫入 master")
@click.option("--als-batch-size", default=500, show_default=True)
@click.option("--verify-hits", "verify_hits", is_flag=True, default=False,
              help="對已 hit 的 BRN 重新查 CR API，比對公司資料是否變更")
@click.option("--older-than", "older_than", default=None, type=int,
              help="--verify-hits: 只檢查超過 N 天未更新的 hit（剩省=全部）")
@click.option("--verify-batch-size", default=200, show_default=True,
              help="--verify-hits 每批带入筆數")
@click.option("--test-brn", is_flag=True, default=False)
@click.option("--brn-start", default="71807826")
@click.option("--brn-end", default="71807828")
@click.option("--config", "config_path", default="config.yaml")
def main(mode, yesterday, lookup,
         init_hsic, tag_industry,
         downloader, init_brn_queue, scan_status,
         process_als, als_batch_size,
         verify_hits, older_than, verify_batch_size,
         test_brn, brn_start, brn_end,
         config_path):
    config = load_config(config_path)
    setup_logging(config)
    logger = logging.getLogger("main")
    config.setdefault("cr", {})["downloader"] = downloader

    if test_brn:
        asyncio.run(_run_test_brn(config, brn_start, brn_end, logger))
        return

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

    # --verify-hits 檢查公司資料是否變更
    if verify_hits:
        asyncio.run(_run_verify_hits(config, older_than, verify_batch_size, logger))
        return

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
            for k in ["region", "district", "street_name", "building_name", "geo_address", "score", "manual_review"]:
                print(f"  {k}: {r.get(k)}")
        else:
            print("無法標準化此地址")
        asyncio.run(als.aclose())
        db.close()
        return

    if init_brn_queue:
        from src.db_writer import DBWriter
        brn_cfg = config.get("cr_brn", {})
        db = DBWriter(config["db"]["path"])
        total = db.init_brn_queue(
            mode=brn_cfg.get("mode", "numeric"),
            prefixes=brn_cfg.get("prefixes", None),
            start=brn_cfg.get("start", 0),
            end=brn_cfg.get("end", 99_999_999),
            prefix_start=brn_cfg.get("prefix_start", 1_000_000),
            prefix_end=brn_cfg.get("prefix_end", 4_000_000),
        )
        print(f"\n初始化完成，共處理 {total:,} 筆 BRN")
        db.close()
        return

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
            if scanned:
                print(f"  hit_rate_of_scanned: {hit_count / scanned * 100:.4f}%")
        db.close()
        return

    if init_hsic:
        from src.cr_industry_codes import CRIndustryCodes
        hsic = CRIndustryCodes(config["db"]["path"])
        hsic.load()
        return

    if tag_industry:
        from src.industry_tagger import IndustryTagger
        tagger = IndustryTagger(config, config["db"]["path"])
        result = tagger.run(llm_fallback=config.get("industry", {}).get("enable_llm_fallback", True))
        for k, v in result.items():
            print(f"  {k}: {v}")
        return

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


async def _run_verify_hits(
    config: dict,
    older_than: int | None,
    batch_size: int,
    logger,
):
    """
    --verify-hits 實作：
    1. 從 brn_scan_queue 抽取 hit 狀態的 BRN
    2. 重新查 CR API
    3. 比對 name_zh / name_en / address_raw 是否變更
    4. 變更 -> 更新 companies_raw + master 重置地址欄位
    5. 印出統計：樣本數 / updated / unchanged / gone（API 輸回 miss）
    """
    import random
    import httpx
    from src.db_writer import DBWriter

    db = DBWriter(config["db"]["path"])
    cr_cfg = config.get("cr", {})
    base_url = cr_cfg.get(
        "base_url",
        "https://data.cr.gov.hk/cr/api/api/v1/api_builder/json/local/search",
    )
    request_timeout = cr_cfg.get("request_timeout", 30)
    jitter_min = config.get("cr_brn", {}).get("jitter_min", 0.05)
    jitter_max = config.get("cr_brn", {}).get("jitter_max", 0.30)

    hit_brns = db.fetch_hit_batch(batch_size=batch_size, older_than_days=older_than)
    if not hit_brns:
        msg = f"\n無符合條件的 hit BRN"
        if older_than:
            msg += f"（指定: 超過 {older_than} 天未更新）"
        print(msg)
        db.close()
        return

    label = f"超過 {older_than} 天" if older_than else "全部"
    logger.info(f"=== verify-hits 開始：{label} hit，本批 {len(hit_brns)} 筆 BRN ===")
    print(f"\n檢查範圍: {label} hit，本批 {len(hit_brns)} 筆")

    stats = {"updated": 0, "unchanged": 0, "gone": 0, "error": 0}
    updated_brns = []

    _FIELD_MAP = {
        "Brn": "cr_no",
        "Chinese_Company_Name": "name_zh",
        "English_Company_Name": "name_en",
        "Address_of_Registered_Office": "address_raw",
    }

    async def _check_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, brn: str):
        await asyncio.sleep(random.uniform(jitter_min, jitter_max))
        url = (
            f"{base_url}"
            f"?query[0][key1]=Brn"
            f"&query[0][key2]=equal"
            f"&query[0][key3]={brn}"
            f"&format=json"
        )
        async with sem:
            try:
                resp = await client.get(url, timeout=request_timeout)
                if resp.status_code == 400:
                    # BRN 已不存在
                    stats["gone"] += 1
                    logger.info(f"BRN {brn} 已不存在（gone）")
                    return
                resp.raise_for_status()
                data = resp.json()
                records = data if isinstance(data, list) else []
                if not records:
                    stats["gone"] += 1
                    return
                raw = records[0]
                api_rec = {_FIELD_MAP.get(k, k.lower()): v for k, v in raw.items()}
                result = db.upsert_verified_raw(brn, api_rec)
                stats[result] += 1
                if result == "updated":
                    updated_brns.append(brn)
                    logger.info(f"BRN {brn} 資料已變更")
                print(
                    f"\r[verify] 已處理 {stats['updated']+stats['unchanged']+stats['gone']+stats['error']}/{len(hit_brns)}"
                    f"  updated={stats['updated']} unchanged={stats['unchanged']} gone={stats['gone']}",
                    end="", flush=True
                )
            except Exception as e:
                stats["error"] += 1
                logger.warning(f"BRN {brn} 查詢失敗: {e}")

    sem = asyncio.Semaphore(config.get("cr_brn", {}).get("concurrency", 10))
    async with httpx.AsyncClient(http2=True) as client:
        tasks = [_check_one(client, sem, brn) for brn in hit_brns]
        await asyncio.gather(*tasks)
    print()

    db.close()

    print("\n" + "=" * 50)
    print("verify-hits 完成")
    print("=" * 50)
    print(f"  檢查範圍 : {label}")
    print(f"  本批樣本 : {len(hit_brns)} 筆")
    print(f"  unchanged: {stats['unchanged']} （資料不變）")
    print(f"  updated  : {stats['updated']} （已更新 companies_raw + master 重置）")
    print(f"  gone     : {stats['gone']} （BRN 已在 CR 失效）")
    print(f"  error    : {stats['error']} （查詢失敗）")
    if updated_brns:
        print(f"\n  資料已變更的 BRN：")
        for b in updated_brns:
            print(f"    {b}")
        print(f"\n  建議執行 --process-als 重新標準化地址：")
        print(f"    python main.py --process-als")
    print("=" * 50)


async def _run_test_brn(config: dict, brn_start: str, brn_end: str, logger):
    import re as _re
    from src.db_writer import DBWriter
    from src.address_cleaner import AddressCleaner
    from src.als_client import ALSClient

    logger.info(f"=== 渫渫模式: BRN {brn_start} – {brn_end} ===")

    def _parse_brn(s):
        s = s.strip().upper()
        return (int(s), "numeric") if _re.match(r'^\d+$', s) else (s, "alpha")

    start_val, start_type = _parse_brn(brn_start)
    end_val, end_type   = _parse_brn(brn_end)

    db = DBWriter(config["db"]["path"])
    logger.info("[1/5] DuckDB 建表完成")

    if start_type == "numeric" and end_type == "numeric":
        total_q = db.init_brn_queue(mode="numeric", start=int(start_val), end=int(end_val))
    else:
        brn_list = list({brn_start.upper(), brn_end.upper()})
        db.con.executemany(
            "INSERT INTO brn_scan_queue (brn) VALUES (?) ON CONFLICT (brn) DO NOTHING",
            [(b,) for b in brn_list]
        )
        total_q = len(brn_list)
    logger.info(f"[2/5] brn_scan_queue 寫入 {total_q} 筆 BRN")

    from src.cr_downloader_brn import CRDownloaderBrn
    hit_records = []

    async def on_hit(df):
        hit_records.extend(df.to_dicts())

    cr_config = dict(config)
    cr_config.setdefault("cr", {})["downloader"] = "brn"
    bq = dict(config.get("cr_brn", {}))
    bq.update({"fetch_batch_size": total_q, "concurrency": min(5, total_q), "miss_limit": total_q + 1})
    cr_config["cr_brn"] = bq

    await CRDownloaderBrn(cr_config, db=db).download_all(resume=True, on_hit=on_hit)
    logger.info(f"[3/5] CR hit {len(hit_records)} 筆")

    if not hit_records:
        print("\n渫渫結果: 無任何 hit")
        db.close()
        return

    import polars as pl
    df_hits = pl.DataFrame(hit_records)
    CR_MAP = {"companyno": "cr_no", "company_no": "cr_no",
              "namechinese": "name_zh", "nameenglish": "name_en",
              "address": "address_raw", "registeredofficeaddress": "address_raw"}
    col_map = {c: CR_MAP[c.lower()] for c in df_hits.columns if c.lower() in CR_MAP}
    if col_map:
        df_hits = df_hits.rename(col_map)
    for req in ["cr_no", "name_zh", "name_en", "address_raw"]:
        if req not in df_hits.columns:
            df_hits = df_hits.with_columns(pl.lit(None).cast(pl.Utf8).alias(req))
    db.write_raw(df_hits)

    cleaner = AddressCleaner(config["alias_map_path"])
    als_client = ALSClient(config, db)
    addresses_clean = cleaner.clean_batch(df_hits["address_raw"].to_list())
    logger.info("[4/5] ALS 標準化...")
    als_results = await als_client.process_batch(addresses_clean)
    await als_client.aclose()

    als_cols = ["geo_address", "region", "district", "street_name",
                "building_name", "latitude", "longitude", "score", "manual_review"]
    extra_cols = ["company_type", "date_of_incorporation", "re_domiciliation_date"]
    master_records = []
    for i, row in enumerate(df_hits.to_dicts()):
        r = als_results[i] or {}
        master_records.append({
            "cr_no": row.get("cr_no"), "name_zh": row.get("name_zh"),
            "name_en": row.get("name_en"), "address_raw": row.get("address_raw"),
            "address_clean": addresses_clean[i],
            **{k: r.get(k) for k in als_cols},
            **{k: row.get(k) for k in extra_cols},
        })
    db.write_master(master_records)
    logger.info(f"[5/5] master 寫入 {len(master_records)} 筆")

    summary = db.summary()
    db.close()
    print("\n" + "=" * 50)
    print("渫渫完成")
    print("=" * 50)
    print(f"  BRN 範圍    : {brn_start} – {brn_end}")
    print(f"  CR hit 筆數  : {len(hit_records)}")
    print(f"  master 總筆數 : {summary['master_total']}")
    print(f"  平均信心分    : {summary['avg_confidence']}")
    print(f"  需人工覆核   : {summary['manual_review']}")
    print("=" * 50)
    print()
    for rec in master_records:
        print(f"  cr_no={rec['cr_no']} | {rec.get('name_en') or rec.get('name_zh')}")
        print(f"    address_raw   : {rec['address_raw']}")
        print(f"    address_clean : {rec['address_clean']}")
        print(f"    geo_address   : {rec.get('geo_address')}")
        print(f"    region/district: {rec.get('region')} / {rec.get('district')}")
        print(f"    score={rec.get('score')}  manual_review={rec.get('manual_review')}")
        print()


if __name__ == "__main__":
    main()
