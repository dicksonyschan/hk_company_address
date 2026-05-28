"""
cr_industry_codes.py
從香港政府公開資料下載 HKICLS（香港標準行業分類）完整代碼表，
並存入 DuckDB 的 industry_codes 參考表。

資料來源：
  - data.gov.hk《香港標準行業分類（第 2.0 版）》JSON
  - 若政府 API 不可用，使用 src/industry_tagger.py 中的 HKICLS_MAP 作後備
"""

import json
import logging
from pathlib import Path

import duckdb
import httpx

from .industry_tagger import HKICLS_MAP

logger = logging.getLogger(__name__)

# 政府 data.gov.hk HKICLS 資料集（如有更新請修改 URL）
_HKICLS_GOV_URL = (
    "https://www.censtatd.gov.hk/en/data/stat_report/product/B1010062/att/"
    "B10100622023AN23B0100.json"
)

CREATE_INDUSTRY_CODES_TABLE = """
CREATE TABLE IF NOT EXISTS industry_codes (
    code        VARCHAR PRIMARY KEY,  -- HKICLS 代碼（單字母或兩位數字）
    level       INTEGER,              -- 層級：1=大類, 2=中類, 3=小類
    name_zh     VARCHAR,
    name_en     VARCHAR,
    parent_code VARCHAR               -- 父層代碼
);
"""


class CRIndustryCodes:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def init_table(self):
        """建立 industry_codes 表（幂等）。"""
        con = duckdb.connect(self.db_path)
        con.execute(CREATE_INDUSTRY_CODES_TABLE)
        con.close()
        logger.info("industry_codes 表初始化完成")

    def load_from_gov(self, timeout: int = 15) -> bool:
        """
        嘗試從政府 API 下載 HKICLS 完整代碼表。
        成功回傳 True，失敗回傳 False（呼叫方可 fallback 到 load_from_builtin）。
        """
        try:
            resp = httpx.get(_HKICLS_GOV_URL, timeout=timeout, follow_redirects=True)
            resp.raise_for_status()
            data = resp.json()
            records = self._parse_gov_json(data)
            if records:
                self._upsert(records)
                logger.info(f"已從政府 API 載入 {len(records)} 筆行業代碼")
                return True
        except Exception as e:
            logger.warning(f"政府 HKICLS API 下載失敗：{e}，改用內建資料")
        return False

    def load_from_builtin(self):
        """
        使用 HKICLS_MAP 內建精簡版（大類）填充 industry_codes 表。
        作為政府 API 不可用時的後備。
        """
        records = [
            {
                "code": code,
                "level": 1,
                "name_zh": info["name_zh"],
                "name_en": info["name_en"],
                "parent_code": None,
            }
            for code, info in HKICLS_MAP.items()
        ]
        self._upsert(records)
        logger.info(f"已從內建 HKICLS_MAP 載入 {len(records)} 筆行業大類代碼")

    def load(self):
        """嘗試政府 API，失敗則用內建資料。"""
        self.init_table()
        if not self.load_from_gov():
            self.load_from_builtin()

    def _parse_gov_json(self, data: dict | list) -> list[dict]:
        """
        解析政府 JSON 格式（格式因發布版本可能不同，以下為通用解析）。
        若格式不符，回傳空列表讓呼叫方 fallback。
        """
        records = []
        try:
            items = data if isinstance(data, list) else data.get("data", data.get("records", []))
            for item in items:
                code = str(item.get("code") or item.get("sic_code") or "").strip()
                name_zh = str(item.get("name_zh") or item.get("name_c") or "").strip()
                name_en = str(item.get("name_en") or item.get("name_e") or "").strip()
                level = int(item.get("level", 1))
                parent = str(item.get("parent_code") or "").strip() or None
                if code and (name_zh or name_en):
                    records.append({
                        "code": code, "level": level,
                        "name_zh": name_zh, "name_en": name_en,
                        "parent_code": parent,
                    })
        except Exception as e:
            logger.error(f"解析政府 HKICLS JSON 失敗：{e}")
        return records

    def _upsert(self, records: list[dict]):
        con = duckdb.connect(self.db_path)
        con.executemany(
            """
            INSERT INTO industry_codes (code, level, name_zh, name_en, parent_code)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (code) DO UPDATE SET
                name_zh     = EXCLUDED.name_zh,
                name_en     = EXCLUDED.name_en,
                level       = EXCLUDED.level,
                parent_code = EXCLUDED.parent_code
            """,
            [
                (r["code"], r["level"], r["name_zh"], r["name_en"], r["parent_code"])
                for r in records
            ],
        )
        con.close()
