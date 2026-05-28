"""
main.py
主程式入口，支援 CLI 參數。

用法:
  python main.py --mode full              # 全量
  python main.py --mode delta --yesterday data/raw/cr_raw_20260527.parquet
  python main.py --lookup "旺角彌敦道123號"  # 單筆查詢
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
              help="單筆地址查詢（測試用）")
@click.option("--init-hsic", is_flag=True, default=False,
              help="載入 HSIC 行業代碼表（從 data.gov.hk 下載）")
@click.option("--tag-industry", is_flag=True, default=False,
              help="對 master 表執行行業標籤（完成 --init-hsic 後使用）")
@click.option("--config", "config_path", default="config.yaml",
              help="設定檔路徑")
def main(mode: str, yesterday: str, lookup: str,
         init_hsic: bool, tag_industry: bool, config_path: str):
    config = load_config(config_path)
    setup_logging(config)
    logger = logging.getLogger("main")

    # 單筆查詢模式
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
        asyncio.run(als.aclose())  # P1 #3: 關閉持久化 httpx.AsyncClient
        db.close()
        return

    # --init-hsic 模式
    if init_hsic:
        from src.cr_industry_codes import CRIndustryCodes
        logger.info("=== 載入 HSIC 行業代碼表 ===")
        hsic = CRIndustryCodes(config["db"]["path"])
        hsic.load()
        logger.info("=== HSIC 載入完成 ===")
        return

    # --tag-industry 模式
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


if __name__ == "__main__":
    main()
