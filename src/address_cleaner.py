"""
address_cleaner.py
地址前處理：去雜訊、Unicode 正規化、繁簡統一、別名替換。
送 ALS 前先清洗，命中率可從 ~60% 提升至 ~90%+。
"""

import json
import re
import unicodedata
from pathlib import Path

try:
    import opencc
    _converter = opencc.OpenCC("s2hk")  # 簡體 -> 香港繁體
except ImportError:
    try:
        from opencc import OpenCC
        _converter = OpenCC("s2hk")
    except Exception:
        _converter = None


# 不需要送 ALS 的雜訊模式
_NOISE_PATTERNS = [
    r"香港特別行政區",
    r"香港特區",
    r"HONG\s*KONG\s*SAR",
    r"HONG\s*KONG",
    r"c/?o\s+[^,，]+",          # c/o 轉交
    r"attn[：:.]?\s*[^,，]+",   # Attn 聯絡人
    r"\b\d{8}\b",               # 8位電話號碼
    r"[Ff]ax[：:]?\s*[\d\-]+",
    r"[Tt]el[：:]?\s*[\d\-]+",
    r"[Ee]-?[Mm]ail[：:]?\s*\S+",
    r"\bP\.?O\.?\s*Box\s*\d+",  # 郵政信箱
    r"[\U0001F600-\U0001FFFF]",  # Emoji
    r"\u200b|\ufeff|\u00a0",     # 零寬空格、BOM、非斷行空格
]
_NOISE_RE = re.compile("|".join(_NOISE_PATTERNS), re.IGNORECASE)

# 全形 -> 半形數字/英文/符號
_FULLWIDTH_TABLE = str.maketrans(
    "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ，。！？；：「」『』（）【】",
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz,。!?;:""''()[]" ,
)


class AddressCleaner:
    def __init__(self, alias_map_path: str = "data/alias_map.json"):
        self.alias_map: dict[str, str] = {}
        alias_path = Path(alias_map_path)
        if alias_path.exists():
            raw = json.loads(alias_path.read_text(encoding="utf-8"))
            self.alias_map = {k: v for k, v in raw.items() if not k.startswith("_")}

    def clean(self, address: str) -> str:
        """主清洗流程，回傳清洗後的地址字串。"""
        if not address or not isinstance(address, str):
            return ""

        s = address.strip()

        # 1. Unicode 正規化（全形->半形、相容字元分解）
        s = unicodedata.normalize("NFKC", s)

        # 2. 全形數字/英文/標點 -> 半形
        s = s.translate(_FULLWIDTH_TABLE)

        # 3. 繁簡轉換（簡體->香港繁體）
        if _converter:
            s = _converter.convert(s)

        # 4. 去除雜訊
        s = _NOISE_RE.sub(" ", s)

        # 5. 別名替換（最長詞優先，避免部分匹配問題）
        for alias, standard in sorted(self.alias_map.items(), key=lambda x: -len(x[0])):
            s = s.replace(alias, standard)

        # 6. 多餘空白壓縮
        s = re.sub(r"\s+", " ", s).strip()

        # 7. 移除開頭結尾的標點
        s = s.strip("，,;；.。-/\\")

        return s

    def clean_batch(self, addresses: list[str]) -> list[str]:
        return [self.clean(a) for a in addresses]
