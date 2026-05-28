"""
als_client.py
非同步 ALS API 客戶端。

優化:
- httpx.AsyncClient 持久化（class 成員），減少 TCP 連線開銷
- process_batch 開頭批次查詢 cache（get_cache_batch），減少 DB round-trip
- _parse_als_response 加入 try/except，防止單筆 JSON 結構異常中斷整批
- 保留 aiolimiter 限流 + tenacity 重試
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
        self._limiter = AsyncLimiter(self.rate_limit, 1.0)
        # 持久化 HTTP client（呼叫者需在完成後呼叫 aclose()）
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                http2=True,
                limits=httpx.Limits(
                    max_connections=self.concurrency + 5,
                    max_keepalive_connections=self.concurrency,
                ),
            )
        return self._client

    async def aclose(self):
        """關閉持久化 HTTP client，於 pipeline 結束時呼叫。"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def _call_als(self, address_clean: str) -> dict:
        """呼叫 ALS API，回傳原始 JSON。使用持久化 client。"""
        client = await self._get_client()
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
        """從 ALS 回應取出最高分結果，萃取關鍵欄位。加入 try/except 防止解析失敗。"""
        _empty = {
            "geo_address": None, "score": 0, "region": None,
            "district": None, "street_name": None, "building_name": None,
            "latitude": None, "longitude": None,
        }
        try:
            results = als_json.get("SuggestedAddress", [])
            if not results:
                return _empty

            top = results[0]
            addr = top.get("Address", {})
            geo = top.get("ValidationInformation", {})

            def zh_or_en(key_zh, key_en, obj):
                return obj.get(key_zh) or obj.get(key_en)

            premises = addr.get("PremisesAddress", {})
            hk_addr = premises.get("ChiPremisesAddress", premises.get("EngPremisesAddress", {}))
            region_block = hk_addr.get("ChiDistrictBlock", hk_addr.get("EngDistrictBlock", {}))
            street_block = hk_addr.get("StreetBlock", {})
            building_block = hk_addr.get("BuildingBlock", {})

            return {
                "geo_address": premises.get("GeoAddress"),
                "score": float(geo.get("Score", 0)),
                "region": zh_or_en("ChiRegion", "EngRegion", hk_addr),
                "district": zh_or_en("ChiDistrict", "EngDistrict", region_block),
                "street_name": zh_or_en("ChiStreetName", "EngStreetName", street_block),
                "building_name": zh_or_en("ChiBuildingName", "EngBuildingName", building_block),
                "latitude": premises.get("GeospatialInformation", {}).get("Latitude"),
                "longitude": premises.get("GeospatialInformation", {}).get("Longitude"),
            }
        except Exception as e:
            logger.warning(f"ALS 回應解析失敗: {e} | json={str(als_json)[:200]}")
            return _empty

    async def _process_one(
        self,
        address_clean: str,
        sem: asyncio.Semaphore,
    ) -> Optional[dict]:
        """處理單一地址：cache miss 才呼叫 API（cache 已在 process_batch 批次預載）。"""
        h = _sha1(address_clean)
        async with sem:
            try:
                als_json = await self._call_als(address_clean)
            except Exception as e:
                logger.warning(f"ALS 呼叫失敗: {address_clean[:50]} — {e}")
                return None

        parsed = self._parse_als_response(als_json)
        parsed["address_hash"] = h
        parsed["address_clean"] = address_clean
        parsed["als_json"] = json.dumps(als_json, ensure_ascii=False)
        parsed["manual_review"] = parsed["score"] < self.threshold

        self.db.upsert_cache(parsed)
        return parsed

    async def process_batch(self, addresses: list[str]) -> list[Optional[dict]]:
        """
        批次處理地址清單（已清洗）。
        1. 批次查詢 cache（一次 DB 查詢）
        2. 只對 cache miss 的 unique 地址呼叫 API
        """
        unique_addresses = list(dict.fromkeys(a for a in addresses if a))

        # --- 批次 cache 查詢（一次 DB round-trip）---
        hashes = [_sha1(a) for a in unique_addresses]
        cache_map: dict[str, dict] = self.db.get_cache_batch(hashes)

        miss_addresses = [a for a, h in zip(unique_addresses, hashes) if h not in cache_map]
        logger.info(
            f"共 {len(unique_addresses)} unique 地址，"
            f"cache 命中 {len(unique_addresses) - len(miss_addresses)}，"
            f"需 API 查詢 {len(miss_addresses)}"
        )

        # --- 只對 miss 的地址呼叫 ALS ---
        if miss_addresses:
            sem = asyncio.Semaphore(self.concurrency)
            tasks = [self._process_one(addr, sem) for addr in miss_addresses]
            miss_results = await atqdm.gather(*tasks, desc="ALS 標準化", total=len(tasks))
            for addr, res in zip(miss_addresses, miss_results):
                if res:
                    cache_map[_sha1(addr)] = res

        # 對應回原始清單（含重複）
        return [cache_map.get(_sha1(a)) for a in addresses]
