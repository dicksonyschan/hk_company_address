"""
address_cleaner.py
地址前處理：去雜訊、Unicode 正規化、繁簡統一、別名替換。
送 ALS 前先清洗，命中率可從 ~60% 提升至 ~90%+。

修補 (audit):
- 新增 KOWLOON / NEW TERRITORIES / ISLANDS 移除規則（避免干擾 ALS 評分）
- 新增連續逗號/空格清理（如 "803B,, 8樓,," -> "803B, 8樓"）
- 補充九龍著名大廈/街道別名，提升 ALS 命中率
"""

import json
import re
import unicodedata
from pathlib import Path

# --- OpenCC 初始化（instance 層，thread-safe）---
def _make_converter():
    try:
        import opencc
        return opencc.OpenCC("s2hk")
    except Exception:
        return None


# 不需要送 ALS 的雜訊模式
_NOISE_PATTERNS = [
    r"香港特別行政區",
    r"香港特區",
    r"HONG\s*KONG\s*SAR",
    r"HONG\s*KONG",
    # audit fix: 移除 KOWLOON / NEW TERRITORIES / ISLANDS 等對 ALS 毫無幫助的地域詞
    r"\bKOWLOON\b",
    r"\bNEW\s*TERRITORIES\b",
    r"\bISLANDS\s*DISTRICT\b",
    r"\bNEW\s*KOWLOON\b",
    r"c/?o\s+[^,，]+",
    r"attn[：:.]?\s*[^,，]+",
    r"\b\d{8}\b",
    r"[Ff]ax[：:]?\s*[\d\-]+",
    r"[Tt]el[：:]?\s*[\d\-]+",
    r"[Ee]-?[Mm]ail[：:]?\s*\S+",
    r"\bP\.?O\.?\s*Box\s*\d+",
    r"[\U0001F600-\U0001FFFF]",
    r"\u200b|\ufeff|\u00a0",
    r"(?i)\broom\s*(?=\d)",
    r"(?i)\bflat\s*(?=[A-Z]\d|\d)",
    r"(?i)\bunit\s*(?=[A-Z]\d|\d)",
]
_NOISE_RE = re.compile("|".join(_NOISE_PATTERNS), re.IGNORECASE)

# audit fix: 連續逗號/空格清理（如 "803B,, 8樓,, 街道" -> "803B, 8樓, 街道"）
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

# 內建的常見 HK 地址別名
_BUILTIN_ALIASES: dict[str, str] = {
    # 地區（英文全稱）
    "Causeway Bay": "銅鑼灣",
    "Tsim Sha Tsui": "尖沙咀",
    "Mong Kok": "旺角",
    "Wan Chai": "灣仔",
    "Sham Shui Po": "深水埗",
    "Kwun Tong": "觀塘",
    "Tuen Mun": "屯門",
    "Yuen Long": "元朗",
    "Sha Tin": "沙田",
    "Tai Po": "大埔",
    "Sai Kung": "西貢",
    "Tseung Kwan O": "將軍澳",
    "Tung Chung": "東涌",
    "Hung Hom": "紅磡",
    "To Kwa Wan": "土瓜灣",
    "Ho Man Tin": "何文田",
    "Kowloon City": "九龍城",
    "Wong Tai Sin": "黃大仙",
    "Diamond Hill": "鑽石山",
    "Kowloon Bay": "九龍灣",
    "Ngau Tau Kok": "牛頭角",
    "Lam Tin": "藍田",
    "Yau Tong": "油塘",
    "Chai Wan": "柴灣",
    "Siu Sai Wan": "小西灣",
    "Quarry Bay": "鰂魚涌",
    "North Point": "北角",
    "Fortress Hill": "炮台山",
    "Tin Hau": "天后",
    "Tai Hang": "大坑",
    "Happy Valley": "跑馬地",
    "Sheung Wan": "上環",
    "Central": "中環",
    "Admiralty": "金鐘",
    "Kennedy Town": "堅尼地城",
    "Sai Ying Pun": "西營盤",
    "Pok Fu Lam": "薄扶林",
    "Aberdeen": "香港仔",
    "Ap Lei Chau": "鴨脷洲",
    "Wong Chuk Hang": "黃竹坑",
    "Shau Kei Wan": "筲箕灣",
    "Heng Fa Chuen": "杏花邨",
    "Taikoo": "太古",
    "Tai Koo": "太古",
    "Kornhill": "康怡",
    "Tsuen Wan": "荃灣",
    "Kwai Chung": "葵涌",
    "Kwai Fong": "葵芳",
    "Tsing Yi": "青衣",
    "Lantau": "大嶼山",
    "Discovery Bay": "愉景灣",
    "Tai Wai": "大圍",
    "Fo Tan": "火炭",
    "University": "大學",
    "Racecourse": "馬場",
    "Pak Tin": "白田",
    "Cheung Sha Wan": "長沙灣",
    "Lai Chi Kok": "荔枝角",
    "Mei Foo": "美孚",
    "Tsz Wan Shan": "慈雲山",
    "Ngau Chi Wan": "牛池灣",
    "Ping Shek": "平石",
    "Choi Hung": "彩虹",
    "Lok Fu": "樂富",
    "Jordan": "佐敦",
    "Yau Ma Tei": "油麻地",
    "Prince Edward": "太子",
    "Shek Kip Mei": "石硤尾",
    "Nam Cheong": "南昌",
    "Long Ping": "朗屏",
    "Tin Shui Wai": "天水圍",
    "Kam Sheung Road": "錦上路",
    "Pat Heung": "八鄉",
    "Fanling": "粉嶺",
    "Sheung Shui": "上水",
    "Lok Ma Chau": "落馬洲",
    "Wu Kai Sha": "烏溪沙",
    "Ma On Shan": "馬鞍山",
    "Hang Hau": "坑口",
    "Po Lam": "寶琳",
    "Lohas Park": "日出康城",
    "Sunny Bay": "欣澳",
    "Disneyland": "迪士尼",
    # audit: 補充九龍著名街道/大廈英文名（常見於 CR 地址）
    "Nathan Road": "彌敦道",
    "Un Chau Street": "元州街",
    "Cheung Sha Wan Road": "長沙灣道",
    "Castle Peak Road": "青山道",
    "Argyle Street": "亞皆老街",
    "Boundary Street": "界限街",
    "Prince Edward Road": "太子道",
    "Waterloo Road": "窩打老道",
    "Lai Chi Kok Road": "荔枝角道",
    "Ma Tau Wai Road": "馬頭圍道",
    "Hung Hom Road": "紅磡道",
    "Chatham Road": "漆咸道",
    "Austin Road": "柯士甸道",
    "Canton Road": "廣東道",
    "Granville Road": "加連威老道",
    "Kimberley Road": "金巴利道",
    "Carnarvon Road": "加拿芬道",
    "Haiphong Road": "海防道",
    "Peking Road": "北京道",
    "Salisbury Road": "梳士巴利道",
    "Reclamation Street": "填地街",
    "Shanghai Street": "上海街",
    "Temple Street": "廟街",
    "Portland Street": "砵蘭街",
    "Dundas Street": "登打士街",
    "Sai Yeung Choi Street": "西洋菜街",
    # 縮寫
    "TST": "尖沙咀",
    "CWB": "銅鑼灣",
    "MK": "旺角",
    "TKO": "將軍澳",
    "KT": "觀塘",
    "SSP": "深水埗",
    "TW": "荃灣",
    "TM": "屯門",
    "YL": "元朗",
    "SK": "西貢",
    "ST": "沙田",
    "FO": "火炭",
    "HH": "紅磡",
    "WC": "灣仔",
    "HV": "跑馬地",
    "KB": "九龍灣",
    "KC": "葵涌",
    "TC": "東涌",
    "NP": "北角",
    "CB": "柴灣",
    # 常見錯別字 / 舊地名
    "汪角": "旺角",
    "佐頓": "佐敦",
    "鰂漁涌": "鰂魚涌",
    "彌頓": "彌敦",
    "彌敦道": "彌敦道",
    "啟德": "啟德",
    "九廣鐵路": "",
    "地下": "地鋪",
    "G/F": "地鋪",
    "G/f": "地鋪",
    "UG": "地鋪",
    # 街道類型
    "Road": "道",
    "Street": "街",
    "Avenue": "道",
    "Lane": "里",
    "Path": "徑",
    "Drive": "道",
}

# 樓層寫法統一用精確 regex
_FLOOR_ALIASES: list[tuple[str, str]] = [
    (r'(?<!\d)B2/F(?!\d)', '地庫2樓'),
    (r'(?<!\d)B1/F(?!\d)', '地庫1樓'),
    (r'(?<!\d)B/F(?!\d)',  '地庫'),
    (r'(?i)(?<!\d)G/F(?!\d)', '地鋪'),
    (r'(?<!\d)M/F(?!\d)',  '夾層'),
    (r'(?<!\d)(\d{1,3})/F(?!\d)', r'\1樓'),
]


class AddressCleaner:
    def __init__(self, alias_map_path: str = "data/alias_map.json"):
        merged: dict[str, str] = dict(_BUILTIN_ALIASES)
        alias_path = Path(alias_map_path)
        if alias_path.exists():
            raw = json.loads(alias_path.read_text(encoding="utf-8"))
            external = {k: v for k, v in raw.items() if not k.startswith("_")}
            merged.update(external)

        self._regex_aliases: list[tuple[re.Pattern, str]] = []
        self._plain_aliases: list[tuple[str, str]] = []
        for alias, standard in sorted(merged.items(), key=lambda x: -len(x[0])):
            if re.search(r'[A-Za-z]', alias):
                self._regex_aliases.append(
                    (re.compile(r'\b' + re.escape(alias) + r'\b', re.IGNORECASE), standard)
                )
            else:
                self._plain_aliases.append((alias, standard))

        self._converter = _make_converter()

    def clean(self, address: str) -> str:
        """主清洗流程，回傳清洗後的地址字串。"""
        if not address or not isinstance(address, str):
            return ""

        s = address.strip()

        # 1. Unicode 正規化
        s = unicodedata.normalize("NFKC", s)

        # 2. 全形 -> 半形
        s = s.translate(_FULLWIDTH_TABLE)

        # 3. 繁簡轉換
        if self._converter:
            s = self._converter.convert(s)

        # 4. 去除雜訊（含 KOWLOON / NEW TERRITORIES）
        s = _NOISE_RE.sub(" ", s)

        # 5. audit fix: 壓縮連續逗號（",," -> ","）
        s = _MULTI_COMMA_RE.sub(",", s)

        # 6a. 樓層寫法用精確 regex
        for floor_pat, floor_std in _FLOOR_ALIASES:
            s = re.sub(floor_pat, floor_std, s)

        # 6b. 英文別名用 word-boundary regex
        for pattern, standard in self._regex_aliases:
            s = pattern.sub(standard, s)

        # 6c. 中文別名直接 str.replace
        for alias, standard in self._plain_aliases:
            if alias in s:
                s = s.replace(alias, standard)

        # 7. 多餘空白壓縮
        s = re.sub(r"\s+", " ", s).strip()

        # 8. 移除開頭結尾的標點
        s = s.strip("，,;；.。-/\\")

        return s

    def clean_batch(self, addresses: list[str]) -> list[str]:
        """批次清洗（順序保留）。"""
        return [self.clean(a) for a in addresses]
