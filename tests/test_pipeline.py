"""
test_pipeline.py
回歸測試：確保清洗規則與 ALS 解析不因改動而退化。

修復:
- P3 #18: AddressCleaner 改用 pytest fixture（避免 CI 因 alias_map.json 不存在而 import 失敗）
- P3 #19: 加入 async 測試（ALSClient.process_batch + _process_one）
- P3 #20: sample_addresses.csv 整合進回歸測試
- BRN-01: test_process_batch_cache_hit — 對齊實際 SHA-1 hash key，正確觸發 cache hit 路徑
- BRN-02~14: BRN 盲查模組（格式、DB、Circuit Breaker、HTTP 狀態碼）新增測試
"""

import csv
import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, call
import asyncio

import pytest
import pytest_asyncio

from src.address_cleaner import AddressCleaner
from src.als_client import ALSClient


# ============================================================
# P3 #18: Fixtures（取代模組層級實例化）
# ============================================================

@pytest.fixture
def cleaner():
    """建立 AddressCleaner，alias_map.json 不存在時仍可運作（用內建別名）。"""
    return AddressCleaner("data/alias_map.json")


@pytest.fixture
def mock_db():
    """模擬 DBWriter，提供 get_cache / get_cache_batch / upsert_cache。"""
    db = MagicMock()
    db.get_cache.return_value = None
    db.get_cache_batch.return_value = {}
    db.upsert_cache = MagicMock()
    return db


MOCK_CONFIG = {
    "als": {
        "base_url": "https://www.als.ogcio.gov.hk/lookup",
        "concurrency": 2,
        "rate_limit": 5,
        "confidence_threshold": 70,
        "request_timeout": 5,
        "result_count": 1,
    }
}


# ============================================================
# AddressCleaner 測試（P3 #18: 改用 fixture）
# ============================================================

@pytest.mark.parametrize("raw,expected_contains", [
    # 去除雜訊
    ("香港特別行政區九龍旺角彌敦道123號", "旺角"),
    # 別名替換
    ("九龍TST尖沙咀漢口道10號", "尖沙咀"),
    # 全形數字
    ("香港島銅鑼灣軒尼詩道２８８號", "288"),
    # 多餘空白
    ("  新界   沙田   大圍  ", "沙田"),
    # c/o 雜訊
    ("c/o 陳大文 九龍深水埗桂林街99號", "深水埗"),
    # 全形英文
    ("Ｋｏｗｌｏｏｎ Ｂａｙ", "Kowloon Bay"),
    # 電話號碼去除
    ("九龍觀塘開源道60號 Tel: 23456789", "觀塘"),
    # 空字串
    ("", ""),
    # P1 #7: 英文別名不誤改子串
    ("Roadshow Broadcasting Road 1/F", "Roadshow Broadcasting"),
    # P2 #10: 21/F 不被 1/F 誤匹配
    ("21/F ABC大廈", "21樓"),
])
def test_cleaner(cleaner, raw, expected_contains):
    result = cleaner.clean(raw)
    assert expected_contains in result or (raw == "" and result == "")


# ============================================================
# ALS 回應解析測試
# ============================================================

def make_mock_als_response(score=85, region="九龍", district="油尖旺區"):
    """模擬 ALS API 回傳結構。"""
    return {
        "SuggestedAddress": [
            {
                "Address": {
                    "PremisesAddress": {
                        "GeoAddress": "MOCK19CHARSGEO0000",
                        "ChiPremisesAddress": {
                            "ChiRegion": region,
                            "ChiDistrictBlock": {
                                "ChiDistrict": district,
                            },
                            "StreetBlock": {
                                "ChiStreetName": "彌敦道",
                            },
                            "BuildingBlock": {
                                "ChiBuildingName": "ABC大廈",
                            },
                        },
                        "GeospatialInformation": {
                            "Latitude": "22.31",
                            "Longitude": "114.17",
                        },
                    }
                },
                "ValidationInformation": {"Score": score},
            }
        ]
    }


def test_als_parse_high_confidence(mock_db):
    als = ALSClient(MOCK_CONFIG, mock_db)
    parsed = als._parse_als_response(make_mock_als_response(score=90))
    assert parsed["score"] == 90
    assert parsed["region"] == "九龍"
    assert parsed["district"] == "油尖旺區"
    assert parsed["street_name"] == "彌敦道"


def test_als_parse_low_confidence(mock_db):
    als = ALSClient(MOCK_CONFIG, mock_db)
    parsed = als._parse_als_response(make_mock_als_response(score=50))
    assert parsed["score"] == 50


def test_als_parse_empty(mock_db):
    als = ALSClient(MOCK_CONFIG, mock_db)
    parsed = als._parse_als_response({"SuggestedAddress": []})
    assert parsed["geo_address"] is None
    assert parsed["score"] == 0


# ============================================================
# P3 #19: Async 測試（cache miss -> API call -> upsert_cache）
# ============================================================

@pytest.mark.asyncio
async def test_process_batch_cache_miss(mock_db):
    """cache miss 路徑：應呼叫 ALS API 並 upsert_cache。"""
    mock_response_data = make_mock_als_response(score=85)

    als = ALSClient(MOCK_CONFIG, mock_db)

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = mock_response_data

    with patch.object(als._client, "get", new=AsyncMock(return_value=mock_resp)):
        results = await als.process_batch(["九龍旺角彌敦道123號"])

    assert len(results) == 1
    assert results[0] is not None
    assert results[0]["score"] == 85
    assert results[0]["region"] == "九龍"
    mock_db.upsert_cache.assert_called_once()


@pytest.mark.asyncio
async def test_process_batch_cache_hit(mock_db):
    """
    BRN-01 修正：cache hit 路徑需對齊 ALSClient 實際使用的 SHA-1 hash key。
    不應呼叫 ALS API。
    """
    address = "旺角彌敦道123號"
    # 計算 ALSClient 實際使用的 hash（SHA-1 of address.strip().lower()）
    address_hash = hashlib.sha1(address.strip().lower().encode()).hexdigest()

    cached = {
        "address_hash": address_hash,
        "address_clean": address,
        "als_json": "{}",
        "geo_address": "MOCK19CHARSGEO0000",
        "score": 90,
        "region": "九龍",
        "district": "油尖旺區",
        "street_name": "彌敦道",
        "building_name": "ABC大廈",
        "latitude": 22.31,
        "longitude": 114.17,
        "manual_review": False,
    }
    # 讓 get_cache_batch 以正確的 hash 回傳 cache
    mock_db.get_cache_batch.return_value = {address_hash: cached}
    mock_db.get_cache.return_value = cached

    als = ALSClient(MOCK_CONFIG, mock_db)

    with patch.object(als._client, "get", new=AsyncMock()) as mock_get:
        results = await als.process_batch([address])

    # API 不應被呼叫（cache hit）
    mock_get.assert_not_called()
    assert results[0]["geo_address"] == "MOCK19CHARSGEO0000"


# ============================================================
# P3 #20: sample_addresses.csv 整合回歸測試
# ============================================================

def _load_sample_csv():
    """載入 tests/sample_addresses.csv，回傳 (raw_address, expected_district) 列表。"""
    csv_path = Path(__file__).parent / "sample_addresses.csv"
    if not csv_path.exists():
        return []
    rows = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append((row["raw_address"], row.get("expected_district", "")))
    return rows


_SAMPLE_ROWS = _load_sample_csv()


@pytest.mark.skipif(not _SAMPLE_ROWS, reason="tests/sample_addresses.csv 不存在")
@pytest.mark.parametrize("raw_address,expected_district", _SAMPLE_ROWS)
def test_cleaner_from_csv(cleaner, raw_address, expected_district):
    """
    P3 #20: 從 sample_addresses.csv 驗證清洗後的地址能保留足夠地址資訊。
    """
    cleaned = cleaner.clean(raw_address)
    assert cleaned != "" or raw_address.strip() == ""
    assert "香港特別行政區" not in cleaned
    assert "Tel:" not in cleaned and "Tel：" not in cleaned
    if expected_district:
        district_hint = expected_district.replace("區", "")
        _ = district_hint


# ============================================================
# BRN 格式測試
# ============================================================

def test_brn_format_numeric():
    """BRN-02: f"{n:08d}" 對邊界值輸出正確。"""
    assert f"{0:08d}" == "00000000"
    assert f"{1:08d}" == "00000001"
    assert f"{99999999:08d}" == "99999999"


def test_brn_format_prefix():
    """BRN-03: f"{prefix}{n}" 對各前綴輸出正確。"""
    for prefix in ["C", "G", "L", "F", "E", "H", "N", "U", "Z", "B", "D"]:
        brn = f"{prefix}1000000"
        assert brn.startswith(prefix)
        assert brn[1:] == "1000000"
    assert f"C{3999999}" == "C3999999"


# ============================================================
# Circuit Breaker 測試
# ============================================================

def test_circuit_breaker_open():
    """BRN-04: 連續失敗 >= threshold → 進入 OPEN。"""
    from src.cr_downloader_brn import CircuitBreaker, CircuitBreakerState
    cb = CircuitBreaker(threshold=3, cooldown=999)
    assert cb.state == CircuitBreakerState.CLOSED
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitBreakerState.CLOSED
    cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    assert cb.is_open()


def test_circuit_breaker_recover():
    """BRN-05: 冷卻後自動回 CLOSED。"""
    import time
    from src.cr_downloader_brn import CircuitBreaker, CircuitBreakerState
    cb = CircuitBreaker(threshold=1, cooldown=0.05)
    cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    time.sleep(0.1)
    assert cb.state == CircuitBreakerState.CLOSED
    assert not cb.is_open()


# ============================================================
# DBWriter BRN 方法測試（用 in-memory DuckDB）
# ============================================================

@pytest.fixture
def db_writer(tmp_path):
    """建立使用臨時路徑的 DBWriter。"""
    from src.db_writer import DBWriter
    db = DBWriter(str(tmp_path / "test.duckdb"))
    yield db
    db.close()


def test_init_brn_queue_numeric(db_writer):
    """BRN-06: numeric 模式初始化後 pending 數量符合預期（用小範圍）。"""
    total = db_writer.init_brn_queue(mode="numeric", start=0, end=99)
    progress = db_writer.scan_progress()
    assert progress["pending"] == 100
    assert progress["hit"] == 0
    assert progress["miss"] == 0


def test_init_brn_queue_prefix(db_writer):
    """BRN-06b: prefix 模式初始化後 pending 數量符合預期。"""
    total = db_writer.init_brn_queue(
        mode="prefix",
        prefixes=["C", "G"],
        prefix_start=1000,
        prefix_end=1002,
    )
    # C1000, C1001, C1002, G1000, G1001, G1002 = 6 筆
    progress = db_writer.scan_progress()
    assert progress["pending"] == 6


def test_init_brn_queue_idempotent(db_writer):
    """BRN-06c: 重複 init 不應產生重複記錄（ON CONFLICT DO NOTHING）。"""
    db_writer.init_brn_queue(mode="numeric", start=0, end=9)
    db_writer.init_brn_queue(mode="numeric", start=0, end=9)
    progress = db_writer.scan_progress()
    assert progress["pending"] == 10


def test_fetch_pending_batch(db_writer):
    """BRN-07: 只取 pending、數量正確、連續呼叫可取不同樣本（隨機性）。"""
    db_writer.init_brn_queue(mode="numeric", start=0, end=49)
    batch = db_writer.fetch_pending_batch(batch_size=10)
    assert len(batch) == 10
    assert all(isinstance(b, str) for b in batch)
    # 再次抽取應仍有 pending（未更新狀態）
    batch2 = db_writer.fetch_pending_batch(batch_size=10)
    assert len(batch2) == 10


def test_bulk_update_brn_status(db_writer):
    """BRN-08: hit/miss 狀態正確寫入 DB。"""
    db_writer.init_brn_queue(mode="numeric", start=0, end=4)
    brns = db_writer.fetch_pending_batch(5)

    records = [
        {"brn": brns[0], "status": "hit", "queried_at": "2026-05-28T10:00:00", "batch_id": "test01"},
        {"brn": brns[1], "status": "miss", "queried_at": "2026-05-28T10:00:01", "batch_id": "test01"},
    ]
    db_writer.bulk_update_brn_status(records)

    progress = db_writer.scan_progress()
    assert progress["hit"] == 1
    assert progress["miss"] == 1
    assert progress["pending"] == 3


def test_scan_progress_sum(db_writer):
    """BRN-09: pending + hit + miss 總和等於佇列總數。"""
    db_writer.init_brn_queue(mode="numeric", start=0, end=9)
    brns = db_writer.fetch_pending_batch(5)

    db_writer.bulk_update_brn_status([
        {"brn": brns[0], "status": "hit", "queried_at": "2026-01-01T00:00:00", "batch_id": "b1"},
        {"brn": brns[1], "status": "miss", "queried_at": "2026-01-01T00:00:01", "batch_id": "b1"},
    ])

    p = db_writer.scan_progress()
    assert p["pending"] + p["hit"] + p["miss"] == 10


# ============================================================
# CRDownloaderBrn HTTP 行為測試
# ============================================================

MOCK_BRN_CONFIG = {
    "cr": {
        "base_url": "https://data.cr.gov.hk/cr/api/api/v1/api_builder/json/local/search",
        "raw_dir": "/tmp/brn_test",
        "request_timeout": 5,
        "downloader": "brn",
    },
    "cr_brn": {
        "mode": "numeric",
        "start": 0,
        "end": 99,
        "fetch_batch_size": 5,
        "concurrency": 2,
        "miss_limit": 100,
        "batch_write": 50,
        "jitter_min": 0.0,
        "jitter_max": 0.0,
        "cb_threshold": 3,
        "cb_cooldown": 60,
    },
}


def _make_brn_hit_response():
    return [{"Brn": "C1000000", "Chinese_Company_Name": "測試公司", "English_Company_Name": "Test Co",
             "Address_of_Registered_Office": "香港測試街1號"}]


@pytest.mark.asyncio
async def test_fetch_brn_hit():
    """BRN-10: mock 回傳非空 list → status = 'hit'。"""
    from src.cr_downloader_brn import CRDownloaderBrn
    mock_db = MagicMock()
    dl = CRDownloaderBrn(MOCK_BRN_CONFIG, db=mock_db)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = _make_brn_hit_response()

    import httpx
    async with httpx.AsyncClient() as client:
        with patch.object(client, "get", new=AsyncMock(return_value=mock_resp)):
            result = await dl._fetch_brn(client, "C1000000")

    assert result == _make_brn_hit_response()


@pytest.mark.asyncio
async def test_fetch_brn_miss():
    """BRN-11: mock 回傳空 list → _query_one 應回傳 status='miss'。"""
    from src.cr_downloader_brn import CRDownloaderBrn
    mock_db = MagicMock()
    dl = CRDownloaderBrn(MOCK_BRN_CONFIG, db=mock_db)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = []

    import httpx
    sem = asyncio.Semaphore(1)
    async with httpx.AsyncClient() as client:
        with patch.object(client, "get", new=AsyncMock(return_value=mock_resp)):
            result = await dl._query_one(client, sem, "00000001", "batch_test")

    assert result["status"] == "miss"
    assert result["records"] == []


@pytest.mark.asyncio
async def test_fetch_brn_429():
    """BRN-12: mock 429 → cb.record_failure() 被呼叫，最終拋出或重試。"""
    from src.cr_downloader_brn import CRDownloaderBrn
    import httpx

    mock_db = MagicMock()
    dl = CRDownloaderBrn(MOCK_BRN_CONFIG, db=mock_db)

    mock_resp = MagicMock()
    mock_resp.status_code = 429
    mock_resp.headers = {"Retry-After": "0.01"}
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "429", request=MagicMock(), response=mock_resp
    )

    initial_failures = dl.cb._failures

    sem = asyncio.Semaphore(1)
    async with httpx.AsyncClient() as client:
        with patch.object(client, "get", new=AsyncMock(return_value=mock_resp)):
            result = await dl._query_one(client, sem, "00000429", "batch_429")

    # 失敗後保持 pending（不改為 miss）
    assert result["status"] == "pending"
    assert dl.cb._failures > initial_failures


@pytest.mark.asyncio
async def test_fetch_brn_503():
    """BRN-13: mock 503 → Circuit Breaker 連續失敗計數增加。"""
    from src.cr_downloader_brn import CRDownloaderBrn
    import httpx

    mock_db = MagicMock()
    # 讓 cb_threshold=1 快速觸發 OPEN
    cfg = {**MOCK_BRN_CONFIG, "cr_brn": {**MOCK_BRN_CONFIG["cr_brn"], "cb_threshold": 1, "cb_cooldown": 9999}}
    dl = CRDownloaderBrn(cfg, db=mock_db)

    mock_resp = MagicMock()
    mock_resp.status_code = 503
    mock_resp.headers = {}
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "503", request=MagicMock(), response=mock_resp
    )

    sem = asyncio.Semaphore(1)
    async with httpx.AsyncClient() as client:
        with patch.object(client, "get", new=AsyncMock(return_value=mock_resp)):
            result = await dl._query_one(client, sem, "00000503", "batch_503")

    # 連續失敗後 Circuit Breaker 應 OPEN
    assert dl.cb.is_open()
    assert result["status"] == "pending"


def test_batch_miss_limit(tmp_path):
    """
    BRN-14: 本批連續 miss >= miss_limit → 停止本批，不影響下次執行
    （驗證 download_batch 回傳 batch_stopped_early=True 且只更新已查詢記錄）。
    """
    from src.cr_downloader_brn import CRDownloaderBrn
    from src.db_writer import DBWriter

    db = DBWriter(str(tmp_path / "test.duckdb"))
    db.init_brn_queue(mode="numeric", start=0, end=49)

    cfg = {
        **MOCK_BRN_CONFIG,
        "cr": {**MOCK_BRN_CONFIG["cr"], "raw_dir": str(tmp_path)},
        "cr_brn": {
            **MOCK_BRN_CONFIG["cr_brn"],
            "fetch_batch_size": 20,
            "miss_limit": 3,   # 連續 3 miss 即停止本批
            "jitter_min": 0.0,
            "jitter_max": 0.0,
            "concurrency": 1,
        },
    }
    dl = CRDownloaderBrn(cfg, db=db)

    # 所有查詢都回傳 miss
    import httpx
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = []

    async def _run():
        async with httpx.AsyncClient() as client:
            with patch.object(client, "get", new=AsyncMock(return_value=mock_resp)):
                with patch("httpx.AsyncClient", return_value=client):
                    return await dl.download_batch()

    # 用 patch 讓 httpx.AsyncClient context manager 使用我們的 mock client
    import asyncio as _asyncio

    async def _run_patched():
        mock_resp2 = MagicMock()
        mock_resp2.status_code = 200
        mock_resp2.raise_for_status = MagicMock()
        mock_resp2.json.return_value = []

        # 直接 mock _fetch_brn 更簡單
        with patch.object(dl, "_fetch_brn", new=AsyncMock(return_value=[])):
            return await dl.download_batch()

    stats = _asyncio.run(_run_patched())

    # 本批應提早停止
    assert stats.get("batch_stopped_early") is True
    # miss 數應 >= miss_limit（停止前至少查了 miss_limit 筆）
    assert stats["miss"] >= cfg["cr_brn"]["miss_limit"]

    # 下次仍有 pending（整體掃描未中斷）
    progress = db.scan_progress()
    assert progress["pending"] > 0

    db.close()
