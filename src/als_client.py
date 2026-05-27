"""
als_client.py
非同步 ALS API 客戶端。
- aiolimiter 限流（避免過快請求政府伺服器）
- tenacity 重試（指數退避）
- 本地 hash cache（相同地址不重複呼叫）
- 結果寫入 DuckDB address_cache 表
"""

import asyncio
import hashlib
import json
import logging
from typing import Optional

import httpx
from aiolimiter import AsyncLimiter
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm.asyncio import tqdm as atqdm

from .db_writer import DBWriter

logger = logging.getLogger(__name__)


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


class ALSClient:
    def __init__(self, config: dict, db_writer: DBWriter):
        self.base_url = config["als"]["base_url"]
        self.concurrency = config["als"]["concurrency"]
        self.rate_limit = config["als"]["rate_limit"]
        self.threshold = config["als"]["confidence_threshold"]
        self.timeout = config["als"]["request_timeout"]
        self.result_count = config["als"].get("result_count", 1)
        self.db = db_writer
        self._limiter = AsyncLimiter(self.rate_limit, 1.0)  # N req/sec

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def _call_als(self, client: httpx.AsyncClient, address_clean: str) -> dict:
        """呼叫 ALS API，回傳原始 JSON。"""
        async with self._limiter:
            resp = await client.get(
                self.base_url,
                params={"q": address_clean, "n": self.result_count},
                timeout=self.timeout,
                headers={
                    "Accept": "application/json",
                    "Accept-Language": "zh-Hant, en",
                    "Accept-Encoding": "gzip",
                },
            )
            resp.raise_for_status()
            return resp.json()

    def _parse_als_response(self, als_json: dict) -> dict:
        """從 ALS 回應取出最高分結果，萃取關鍵欄位。"""
        results = als_json.get("SuggestedAddress", [])
        if not results:
            return {"geo_address": None, "score": 0, "region": None,
                    "district": None, "street_name": None, "building_name": None,
                    "latitude": None, "longitude": None}

        top = results[0]  # 已按信心分數排序，取第一筆
        addr = top.get("Address", {})
        geo = top.get("ValidationInformation", {})

        # 嘗試取得中文欄位，若無則取英文
        def zh_or_en(key_zh, key_en, obj):
            return obj.get(key_zh) or obj.get(key_en)

        premises = addr.get("PremisesAddress", {})
        hk_addr = premises.get("ChiPremisesAddress", premises.get("EngPremisesAddress", {}))
        region_block = hk_addr.get("ChiDistrictBlock", hk_addr.get("EngDistrictBlock", {}))

        return {
            "geo_address": top.get("Address", {}).get("PremisesAddress", {}).get("GeoAddress"),
            "score": geo.get("Score", 0),
            "region": zh_or_en("ChiRegion", "EngRegion", hk_addr),
            "district": zh_or_en("ChiDistrict", "EngDistrict", region_block),
            "street_name": zh_or_en("ChiStreetName", "EngStreetName",
                                     hk_addr.get("StreetBlock", {})),
            "building_name": zh_or_en("ChiBuildingName", "EngBuildingName",
                                       hk_addr.get("BuildingBlock", {})),
            "latitude": premises.get("GeospatialInformation", {}).get("Latitude"),
            "longitude": premises.get("GeospatialInformation", {}).get("Longitude"),
        }

    async def _process_one(
        self,
        client: httpx.AsyncClient,
        address_clean: str,
        sem: asyncio.Semaphore,
    ) -> Optional[dict]:
        """處理單一地址：先查 cache，cache miss 才呼叫 API。"""
        h = _sha1(address_clean)

        # 查 cache
        cached = self.db.get_cache(h)
        if cached:
            return cached

        async with sem:
            try:
                als_json = await self._call_als(client, address_clean)
            except Exception as e:
                logger.warning(f"ALS 呼叫失敗: {address_clean[:50]} — {e}")
                return None

        parsed = self._parse_als_response(als_json)
        parsed["address_hash"] = h
        parsed["address_clean"] = address_clean
        parsed["als_json"] = json.dumps(als_json, ensure_ascii=False)
        parsed["manual_review"] = parsed["score"] < self.threshold

        # 寫入 cache
        self.db.upsert_cache(parsed)
        return parsed

    async def process_batch(self, addresses: list[str]) -> list[Optional[dict]]:
        """
        批次處理地址清單（已清洗）。
        只對 unique 地址呼叫 API，最大化 cache 效益。
        """
        unique_addresses = list(dict.fromkeys(a for a in addresses if a))  # 去重保序
        sem = asyncio.Semaphore(self.concurrency)

        logger.info(f"準備處理 {len(unique_addresses)} 個 unique 地址（共 {len(addresses)} 筆）")

        async with httpx.AsyncClient(http2=True) as client:
            tasks = [
                self._process_one(client, addr, sem)
                for addr in unique_addresses
            ]
            results = await atqdm.gather(*tasks, desc="ALS 標準化", total=len(tasks))

        # 建立 hash -> result 映射，再對應回原始清單（含重複）
        result_map = {}
        for addr, res in zip(unique_addresses, results):
            if res:
                result_map[_sha1(addr)] = res

        return [result_map.get(_sha1(a)) for a in addresses]
