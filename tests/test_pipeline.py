"""
test_pipeline.py
回歸測試：確保清洗規則與 ALS 解析不因改動而退化。
"""

import pytest
from src.address_cleaner import AddressCleaner
from src.als_client import ALSClient


cleaner = AddressCleaner("data/alias_map.json")


# ---- AddressCleaner 測試 ----

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
])
def test_cleaner(raw, expected_contains):
    result = cleaner.clean(raw)
    assert expected_contains in result or (raw == "" and result == "")


# ---- ALS 回應解析測試 ----

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


class MockDB:
    def get_cache(self, h): return None
    def upsert_cache(self, r): pass


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


def test_als_parse_high_confidence():
    als = ALSClient(MOCK_CONFIG, MockDB())
    parsed = als._parse_als_response(make_mock_als_response(score=90))
    assert parsed["score"] == 90
    assert parsed["region"] == "九龍"
    assert parsed["district"] == "油尖旺區"
    assert parsed["street_name"] == "彌敦道"


def test_als_parse_low_confidence():
    als = ALSClient(MOCK_CONFIG, MockDB())
    parsed = als._parse_als_response(make_mock_als_response(score=50))
    assert parsed["score"] == 50


def test_als_parse_empty():
    als = ALSClient(MOCK_CONFIG, MockDB())
    parsed = als._parse_als_response({"SuggestedAddress": []})
    assert parsed["geo_address"] is None
    assert parsed["score"] == 0
