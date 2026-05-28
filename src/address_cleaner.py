"""
address_cleaner.py
地址前處理：去雜訊、Unicode 正規化、樓層格式標準化。
清洗結果保持英文（ALS 對英文地址同樣高分匹配）。

清洗策略：
1. Unicode 正規化（全形->semifullwidth）
2. 去雜訊：HONG KONG / KOWLOON / NEW TERRITORIES 等對 ALS 無用的地域詞
3. 連續逗號壓縮（",," -> ","）
4. 樓層寫法標準化（"8/F" -> "8/F" 保持，只移除 Room/Flat/Unit 前置詞）
5. 中文錯別字修正（限中文->中文，不轉換英文）
"""

import json
import re
import unicodedata
from pathlib import Path


# 不需要送 ALS 的雜訊模式
def _make_noise_re() -> re.Pattern:
    patterns = [
        r"HONG\s*KONG\s*SAR",
        r"HONG\s*KONG",
        r"香港特別行政區",
        r"香港特區",
        # 地域詞（對 ALS 無輔助作用，且常被 CR 尾巴套用）
        r"\bNEW\s*KOWLOON\b",
        r"\bKOWLOON\b",
        r"\bNEW\s*TERRITORIES\b",
        r"\bISLANDS\s*DISTRICT\b",
        # 聯絡資訊
        r"c/?o\s+[^,，]+",
        r"attn[：:.]?\s*[^,，]+",
        r"\b\d{8}\b",
        r"[Ff]ax[：:]?\s*[\d\-]+",
        r"[Tt]el[：:]?\s*[\d\-]+",
        r"[Ee]-?[Mm]ail[：:]?\s*\S+",
        r"\bP\.?O\.?\s*Box\s*\d+",
        # 其他雜訊
        r"[\U0001F600-\U0001FFFF]",
        r"\u200b|\ufeff|\u00a0",
        # Room/Flat/Unit 前置詞（只移前置詞，保留後面的序號）
        # Note: inline (?i) flags removed; re.IGNORECASE is applied at compile time
        r"\broom\s*(?=\d|[A-Z]\d)",
        r"\bflat\s*(?=\d|[A-Z]\d)",
        r"\bunit\s*(?=\d|[A-Z]\d)",
    ]
    return re.compile("|".join(patterns), re.IGNORECASE)


_NOISE_RE = _make_noise_re()

# 連續逗號壓縮
_MULTI_COMMA_RE = re.compile(r"[,，]\s*[,，]+")

# 全形 -> 半形
_FULLWIDTH_TABLE = str.maketrans({
    '０': '0', '１': '1', '２': '2', '３': '3', '４': '4',
    '５': '5', '６': '6', '７': '7', '８': '8', '９': '9',
    'Ａ': 'A', 'Ｂ': 'B', 'Ｃ': 'C', 'Ｄ': 'D', 'Ｅ': 'E',
    'Ｆ': 'F', 'Ｇ': 'G', 'Ｈ': 'H', 'Ｉ': 'I', 'Ｊ': 'J',
    'Ｋ': 'K', 'Ｌ': 'L', 'Ｍ': 'M', 'Ｎ': 'N', 'Ｏ': 'O',
    'Ｐ': 'P', 'Ｑ': 'Q', 'Ｒ': 'R', 'Ｓ': 'S', 'Ｔ': 'T',
    'Ｕ': 'U', 'Ｖ': 'V', 'Ｗ': 'W', 'Ｘ': 'X', 'Ｙ': 'Y', 'Ｚ': 'Z',
    'ａ': 'a', 'ｂ': 'b', 'ｃ': 'c', 'ｄ': 'd', 'ｅ': 'e',
    'ｆ': 'f', 'ｇ': 'g', 'ｈ': 'h', 'ｉ': 'i', 'ｊ': 'j',
    'ｋ': 'k', 'ｌ': 'l', 'ｍ': 'm', 'ｎ': 'n', 'ｏ': 'o',
    'ｐ': 'p', 'ｑ': 'q', 'ｒ': 'r', 'ｓ': 's', 'ｔ': 't',
    'ｕ': 'u', 'ｖ': 'v', 'ｗ': 'w', 'ｘ': 'x', 'ｙ': 'y', 'ｚ': 'z',
    '，': ',', '！': '!', '？': '?', '；': ';', '：': ':',
    '「': '"', '」': '"', '『': "'", '』': "'",
    '（': '(', '）': ')', '【': '[', '】': ']',
})

# 樓層寫法標準化（保持英文 X/F 格式， ALS 識別 "8/F" 沒問題）
# 只統一小寫 -> 大寫，移除多餘空格
_FLOOR_NORM_RE = re.compile(r'(?i)(?P<n>\d{1,3}|B\d?|M|G)/F', re.IGNORECASE)


def _norm_floor(m: re.Match) -> str:
    return m.group('n').upper() + '/F'


# 限中文錯別字對照（不加英文->中文）
_ZH_CORRECTIONS: list[tuple[str, str]] = [
    ("汪角",  "旺角"),
    ("佐頓",  "佐敦"),
    ("鰂漁涌", "鰂魚涌"),
    ("彌頓",  "彌敦"),
    ("九廣鐵路", ""),
]


class AddressCleaner:
    def __init__(self, alias_map_path: str = "data/alias_map.json"):
        # 只讀入外部 alias_map 中的中文->中文別名（跳過英文 key）
        self._zh_corrections: list[tuple[str, str]] = list(_ZH_CORRECTIONS)
        alias_path = Path(alias_map_path)
        if alias_path.exists():
            raw = json.loads(alias_path.read_text(encoding="utf-8"))
            for k, v in raw.items():
                if k.startswith("_"):
                    continue
                # 只接受純中文 key（不含任何 ASCII 英文）
                if not re.search(r'[A-Za-z]', k):
                    self._zh_corrections.append((k, v))
        # 長詞優先
        self._zh_corrections.sort(key=lambda x: -len(x[0]))

    def clean(self, address: str) -> str:
        """主清洗流程，結果保持英文。"""
        if not address or not isinstance(address, str):
            return ""

        s = address.strip()

        # 1. Unicode 正規化（全形->semifull）
        s = unicodedata.normalize("NFKC", s)

        # 2. 全形數字/英文/標點 -> 半形
        s = s.translate(_FULLWIDTH_TABLE)

        # 3. 去除雜訊
        s = _NOISE_RE.sub(" ", s)

        # 4. 壓縮連續逗號
        s = _MULTI_COMMA_RE.sub(", ", s)

        # 5. 樓層格式標準化（保持英文 X/F）
        s = _FLOOR_NORM_RE.sub(_norm_floor, s)

        # 6. 中文錯別字修正
        for wrong, correct in self._zh_corrections:
            if wrong in s:
                s = s.replace(wrong, correct)

        # 7. 多餘空白壓縮
        s = re.sub(r"\s+", " ", s).strip()

        # 8. 移除開頭結尾的標點
        s = s.strip(",;./-")

        return s

    def clean_batch(self, addresses: list[str]) -> list[str]:
        return [self.clean(a) for a in addresses]
