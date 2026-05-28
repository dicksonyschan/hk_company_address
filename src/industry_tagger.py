"""
industry_tagger.py
行業標籤主模組：
  1. 從 DuckDB master 表的公司名稱擷取高頻詞，建立關鍵詞表
  2. 關鍵詞規則比對（第一層，離線）
  3. 無法比對的，送 DeepSeek API 批次分類（第二層）
  4. 結果寫回 master 表 industry_* 欄位
"""

import hashlib
import json
import logging
import os
import re
import time
from collections import Counter
from pathlib import Path
from typing import Optional

import duckdb
import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HKICLS 行業代碼對照表（香港標準行業分類 2.0，精簡版主要行業）
# 來源：政府統計處《香港標準行業分類（第 2.0 版）》
# ---------------------------------------------------------------------------
HKICLS_MAP: dict[str, dict] = {
    "A": {"name_zh": "農業、林業及漁業", "name_en": "Agriculture, Forestry and Fishing",
          "keywords_zh": ["農業", "漁業", "林業", "農場", "種植", "水產", "農莊"],
          "keywords_en": ["Farm", "Fishery", "Agriculture", "Aquaculture", "Horticulture"]},
    "B": {"name_zh": "採礦及採石", "name_en": "Mining and Quarrying",
          "keywords_zh": ["採石", "採礦", "礦業", "石礦"],
          "keywords_en": ["Mining", "Quarrying", "Mineral"]},
    "C": {"name_zh": "製造業", "name_en": "Manufacturing",
          "keywords_zh": ["製造", "加工", "廠", "生產", "工廠", "製品", "印刷", "紡織", "食品製造"],
          "keywords_en": ["Manufacturing", "Factory", "Production", "Printing", "Textile", "Processing"]},
    "D": {"name_zh": "電力、燃氣、蒸汽及空調供應", "name_en": "Electricity, Gas, Steam and Air Conditioning",
          "keywords_zh": ["電力", "燃氣", "煤氣", "能源", "電網", "發電"],
          "keywords_en": ["Power", "Electric", "Gas", "Energy", "Utility"]},
    "E": {"name_zh": "供水、廢水及廢物處理", "name_en": "Water Supply, Sewerage and Waste",
          "keywords_zh": ["供水", "廢物", "環保", "回收", "污水", "廢水", "清潔"],
          "keywords_en": ["Water", "Waste", "Recycling", "Environmental", "Sanitation", "Sewerage"]},
    "F": {"name_zh": "建造業", "name_en": "Construction",
          "keywords_zh": ["建築", "建造", "工程", "裝修", "裝潢", "裝飾", "地基", "土木", "建設", "施工",
                           "鋁窗", "鋼構", "機電", "消防工程", "冷氣工程", "管道", "玻璃工程"],
          "keywords_en": ["Construction", "Engineering", "Renovation", "Contractors", "Building",
                           "Civil", "Structural", "Fitting", "Interior Design"]},
    "G": {"name_zh": "批發及零售業", "name_en": "Wholesale and Retail Trade",
          "keywords_zh": ["貿易", "批發", "零售", "商行", "百貨", "超市", "商店", "行", "店", "商貿",
                           "進出口", "出入口", "銷售", "供應商", "代理", "經銷"],
          "keywords_en": ["Trading", "Wholesale", "Retail", "Import", "Export", "Distribution",
                           "Merchandise", "Dealer", "Supplier", "Distributor"]},
    "H": {"name_zh": "運輸、倉庫及郵政", "name_en": "Transport, Storage and Post",
          "keywords_zh": ["運輸", "物流", "倉庫", "貨運", "速遞", "航運", "港口", "郵政", "快遞",
                           "倉儲", "搬運", "車隊", "的士", "巴士"],
          "keywords_en": ["Transport", "Logistics", "Freight", "Shipping", "Courier", "Warehouse",
                           "Storage", "Port", "Aviation", "Airlines"]},
    "I": {"name_zh": "住宿及膳食服務業", "name_en": "Accommodation and Food Services",
          "keywords_zh": ["餐廳", "飲食", "食品", "酒樓", "茶餐廳", "咖啡", "酒店", "旅館", "賓館",
                           "餐飲", "外賣", "飯店", "小食", "甜品", "燒烤", "火鍋"],
          "keywords_en": ["Restaurant", "Hotel", "Catering", "Food", "Cafe", "Bistro", "Bakery",
                           "Kitchen", "Hospitality", "Motel", "Hostel"]},
    "J": {"name_zh": "資訊及通訊業", "name_en": "Information and Communications",
          "keywords_zh": ["科技", "資訊", "軟件", "系統", "網絡", "電訊", "通訊", "電腦", "互聯網",
                           "數碼", "數位", "雲端", "人工智能", "AI", "應用", "平台", "媒體"],
          "keywords_en": ["Technology", "Software", "IT", "Digital", "Internet", "Telecom",
                           "Network", "Cloud", "Media", "Platform", "AI", "Data", "Cyber"]},
    "K": {"name_zh": "金融及保險業", "name_en": "Finance and Insurance",
          "keywords_zh": ["銀行", "財務", "金融", "保險", "證券", "投資", "基金", "信貸", "融資",
                           "資本", "資產管理", "財富", "外匯", "期貨"],
          "keywords_en": ["Bank", "Finance", "Insurance", "Securities", "Investment", "Fund",
                           "Capital", "Asset", "Wealth", "Credit", "Forex", "Futures"]},
    "L": {"name_zh": "地產業", "name_en": "Real Estate",
          "keywords_zh": ["地產", "物業", "房地產", "發展商", "樓盤", "屋苑", "測量", "業主",
                           "租賃", "樓宇"],
          "keywords_en": ["Property", "Real Estate", "Realty", "Estate", "Surveying", "Leasing",
                           "Developer"]},
    "M": {"name_zh": "專業、科學及技術服務業", "name_en": "Professional and Technical Services",
          "keywords_zh": ["顧問", "諮詢", "律師", "法律", "會計", "核數", "工程師", "建築師",
                           "設計", "研究", "測試", "實驗室", "管理顧問"],
          "keywords_en": ["Consulting", "Advisory", "Legal", "Accounting", "Audit", "Architect",
                           "Design", "Research", "Laboratory", "Management"]},
    "N": {"name_zh": "行政及支援服務業", "name_en": "Administrative and Support Services",
          "keywords_zh": ["人力資源", "人事", "外判", "清潔服務", "保安", "物業管理", "旅行社",
                           "租車", "會議"],
          "keywords_en": ["Staffing", "Recruitment", "Cleaning", "Security", "Facilities",
                           "Travel", "Car Rental", "Events", "Outsourcing"]},
    "O": {"name_zh": "公共行政、社會保障及強制性社會保險",
          "name_en": "Public Administration and Social Security",
          "keywords_zh": ["政府", "公務", "市政"],
          "keywords_en": ["Government", "Public", "Municipal"]},
    "P": {"name_zh": "教育業", "name_en": "Education",
          "keywords_zh": ["學校", "教育", "培訓", "學院", "大學", "幼稚園", "補習", "書院",
                           "教學", "學習"],
          "keywords_en": ["School", "Education", "Training", "Academy", "University", "College",
                           "Learning", "Institute", "Tutorial"]},
    "Q": {"name_zh": "人體健康及社會工作活動", "name_en": "Human Health and Social Work",
          "keywords_zh": ["醫療", "醫院", "診所", "藥房", "健康", "醫學", "護理", "社會服務",
                           "養老", "康復", "牙醫", "中醫", "西醫"],
          "keywords_en": ["Medical", "Hospital", "Clinic", "Pharmacy", "Health", "Healthcare",
                           "Nursing", "Dental", "Welfare", "Social"]},
    "R": {"name_zh": "藝術、娛樂及康樂活動", "name_en": "Arts, Entertainment and Recreation",
          "keywords_zh": ["娛樂", "藝術", "體育", "音樂", "電影", "遊戲", "健身", "球場",
                           "博彩", "展覽", "表演"],
          "keywords_en": ["Entertainment", "Arts", "Sports", "Music", "Film", "Gaming",
                           "Fitness", "Recreation", "Events", "Exhibition"]},
    "S": {"name_zh": "其他服務業", "name_en": "Other Service Activities",
          "keywords_zh": ["美容", "美髮", "洗衣", "修理", "維修", "寵物", "禮儀", "婚禮",
                           "殯儀", "宗教"],
          "keywords_en": ["Beauty", "Salon", "Laundry", "Repair", "Maintenance", "Pet",
                           "Wedding", "Funeral", "Religious"]},
}


class IndustryTagger:
    """
    行業標籤器。
    流程：關鍵詞比對 → 未命中送 DeepSeek API → 結果寫回 DB。
    """

    def __init__(self, config: dict, db_path: str):
        self.config = config
        self.db_path = db_path
        self.industry_cfg = config.get("industry", {})
        self.confidence_threshold = self.industry_cfg.get("confidence_threshold", 0.6)
        self.deepseek_api_key = self.industry_cfg.get("deepseek_api_key") or os.getenv("DEEPSEEK_API_KEY", "")
        self.deepseek_model = self.industry_cfg.get("deepseek_model", "deepseek-chat")
        self.deepseek_batch_size = self.industry_cfg.get("batch_size", 50)
        self.keywords_path = Path(self.industry_cfg.get("keywords_path", "data/industry_keywords.json"))
        # 載入或初始化關鍵詞表
        self._kw_map: dict[str, list[str]] = self._load_keywords()

    # ------------------------------------------------------------------
    # 關鍵詞管理
    # ------------------------------------------------------------------

    def _load_keywords(self) -> dict[str, list[str]]:
        """從 JSON 讀取關鍵詞表，若不存在則從 HKICLS_MAP 初始化。"""
        if self.keywords_path.exists():
            with open(self.keywords_path, encoding="utf-8") as f:
                return json.load(f)
        # 初始化：合併 zh + en 關鍵詞
        kw_map = {}
        for code, info in HKICLS_MAP.items():
            kw_map[code] = info["keywords_zh"] + info["keywords_en"]
        return kw_map

    def save_keywords(self):
        """將關鍵詞表持久化到 JSON（方便人工擴充）。"""
        self.keywords_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.keywords_path, "w", encoding="utf-8") as f:
            json.dump(self._kw_map, f, ensure_ascii=False, indent=2)
        logger.info(f"關鍵詞表已儲存至 {self.keywords_path}")

    def add_keywords(self, industry_code: str, keywords: list[str]):
        """手動新增關鍵詞（人工擴充入口）。"""
        if industry_code not in self._kw_map:
            self._kw_map[industry_code] = []
        existing = set(self._kw_map[industry_code])
        new_kw = [k for k in keywords if k not in existing]
        self._kw_map[industry_code].extend(new_kw)
        logger.info(f"已為 {industry_code} 新增 {len(new_kw)} 個關鍵詞")

    def mine_keywords_from_db(self, top_n: int = 200) -> dict[str, list[str]]:
        """
        從 master 表的公司名稱擷取高頻詞（中文 2-char + 英文單字），
        交 DeepSeek 分類後自動擴充關鍵詞表。
        """
        con = duckdb.connect(self.db_path, read_only=True)
        rows = con.execute(
            "SELECT name_zh, name_en FROM master WHERE industry_tag IS NULL LIMIT 100000"
        ).fetchall()
        con.close()

        zh_counter: Counter = Counter()
        en_counter: Counter = Counter()
        for name_zh, name_en in rows:
            if name_zh:
                # 取 2-char bigram
                clean = re.sub(r"[\s（）()【】《》\-—,，.。、]", "", name_zh)
                for i in range(len(clean) - 1):
                    zh_counter[clean[i:i+2]] += 1
            if name_en:
                for word in re.findall(r"[A-Za-z]{3,}", name_en):
                    en_counter[word.title()] += 1

        top_zh = [w for w, _ in zh_counter.most_common(top_n)]
        top_en = [w for w, _ in en_counter.most_common(top_n)]
        logger.info(f"擷取高頻詞：中文 {len(top_zh)} 個，英文 {len(top_en)} 個")
        return {"zh": top_zh, "en": top_en}

    def enrich_keywords_via_llm(self, top_n: int = 200):
        """
        擷取高頻詞後，批次送 DeepSeek 分類到各行業代碼，擴充關鍵詞表。
        只在人工覺得關鍵詞表不足時執行一次即可。
        """
        mined = self.mine_keywords_from_db(top_n)
        all_words = mined["zh"] + mined["en"]

        industry_list = [
            {"code": code, "name_zh": info["name_zh"], "name_en": info["name_en"]}
            for code, info in HKICLS_MAP.items()
        ]

        prompt = f"""你是香港行業分類專家。以下是從香港公司名稱擷取的高頻詞列表，請將每個詞分類到最符合的 HKICLS（香港標準行業分類）代碼。

行業代碼對照：
{json.dumps(industry_list, ensure_ascii=False)}

高頻詞列表：
{json.dumps(all_words, ensure_ascii=False)}

請回傳 JSON，格式：
{{"results": [{{"word": "詞", "code": "行業代碼"}}, ...]}}
只包含明顯屬於某行業的詞，不確定的請跳過。"""

        response = self._call_deepseek([{"role": "user", "content": prompt}])
        try:
            data = json.loads(response)
            results = data.get("results", [])
            added = 0
            for item in results:
                word = item.get("word")
                code = item.get("code", "").upper()
                if word and code in self._kw_map:
                    if word not in self._kw_map[code]:
                        self._kw_map[code].append(word)
                        added += 1
            self.save_keywords()
            logger.info(f"LLM 擴充關鍵詞完成，新增 {added} 個")
        except json.JSONDecodeError as e:
            logger.error(f"LLM 回應解析失敗：{e}")

    # ------------------------------------------------------------------
    # 行業標籤邏輯
    # ------------------------------------------------------------------

    def _tag_by_keyword(self, name_zh: str, name_en: str) -> Optional[str]:
        """關鍵詞規則比對，回傳 HKICLS 代碼或 None。"""
        combined = (name_zh or "") + " " + (name_en or "")
        for code, keywords in self._kw_map.items():
            for kw in keywords:
                if kw and kw in combined:
                    return code
        return None

    def _tag_batch_by_llm(self, companies: list[dict]) -> list[Optional[str]]:
        """
        批次送 DeepSeek API 分類。
        companies: [{"cr_no": ..., "name_zh": ..., "name_en": ...}, ...]
        回傳與 companies 等長的 code 列表（無法分類為 None）。
        """
        if not companies:
            return []
        if not self.deepseek_api_key:
            logger.warning("未設定 DEEPSEEK_API_KEY，跳過 LLM 標籤")
            return [None] * len(companies)

        industry_list = [
            {"code": code, "name_zh": info["name_zh"]}
            for code, info in HKICLS_MAP.items()
        ]
        names_list = [
            {"idx": i, "name_zh": c.get("name_zh", ""), "name_en": c.get("name_en", "")}
            for i, c in enumerate(companies)
        ]

        prompt = f"""你是香港行業分類專家。請根據公司名稱，為每間公司標記最符合的 HKICLS 行業代碼。

行業代碼對照：
{json.dumps(industry_list, ensure_ascii=False)}

公司列表：
{json.dumps(names_list, ensure_ascii=False)}

請回傳 JSON，格式：
{{"results": [{{"idx": 數字, "code": "行業代碼"}}, ...]}}
只回傳 JSON，不需解釋。若真的無法判斷行業，省略該條目（不要回傳 null）。"""

        try:
            response = self._call_deepseek([{"role": "user", "content": prompt}])
            data = json.loads(response)
            result_map = {item["idx"]: item["code"].upper() for item in data.get("results", [])}
        except Exception as e:
            logger.error(f"DeepSeek 分類失敗：{e}")
            result_map = {}

        return [result_map.get(i) for i in range(len(companies))]

    def _call_deepseek(self, messages: list[dict]) -> str:
        """呼叫 DeepSeek Chat API，回傳 content 字串。"""
        url = "https://api.deepseek.com/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.deepseek_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.deepseek_model,
            "messages": messages,
            "response_format": {"type": "json_object"},
            "temperature": 0,
        }
        for attempt in range(3):
            try:
                resp = httpx.post(url, headers=headers, json=payload, timeout=60)
                resp.raise_for_status()
                return resp.json()["choices"][0]["message"]["content"]
            except Exception as e:
                logger.warning(f"DeepSeek API 嘗試 {attempt+1}/3 失敗：{e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        raise RuntimeError("DeepSeek API 三次重試均失敗")

    # ------------------------------------------------------------------
    # 主執行入口
    # ------------------------------------------------------------------

    def run(self, batch_size: int = 500, llm_fallback: bool = True):
        """
        對 master 表中尚未標記（industry_tag IS NULL）的記錄進行行業標籤。
        先關鍵詞比對，未命中且 llm_fallback=True 的送 DeepSeek，
        信心分數低於閾值的標記 industry_review=True。
        """
        con = duckdb.connect(self.db_path)
        self._ensure_industry_columns(con)

        offset = 0
        total_tagged = 0
        total_llm = 0
        total_low_conf = 0

        while True:
            rows = con.execute(
                "SELECT cr_no, name_zh, name_en FROM master "
                "WHERE industry_tag IS NULL "
                f"LIMIT {batch_size} OFFSET {offset}"
            ).fetchall()
            if not rows:
                break

            kw_hits = []
            llm_queue = []
            llm_idx_map = []  # llm_queue 的 index → rows index

            for i, (cr_no, name_zh, name_en) in enumerate(rows):
                code = self._tag_by_keyword(name_zh, name_en)
                if code:
                    kw_hits.append((cr_no, code, "keyword", 1.0, False))
                else:
                    llm_idx_map.append(i)
                    llm_queue.append({"cr_no": cr_no, "name_zh": name_zh, "name_en": name_en})

            # LLM 批次（按 deepseek_batch_size 再細分）
            llm_results: list[Optional[str]] = []
            if llm_fallback and llm_queue:
                for chunk_start in range(0, len(llm_queue), self.deepseek_batch_size):
                    chunk = llm_queue[chunk_start:chunk_start + self.deepseek_batch_size]
                    llm_results.extend(self._tag_batch_by_llm(chunk))
            else:
                llm_results = [None] * len(llm_queue)

            # 整合結果並批次更新
            update_rows = []
            for (cr_no, code, method, conf, review) in kw_hits:
                update_rows.append((code, method, conf, review, cr_no))

            for j, llm_code in enumerate(llm_results):
                cr_no = llm_queue[j]["cr_no"]
                if llm_code:
                    conf = 0.75  # LLM 預設信心分數
                    review = conf < self.confidence_threshold
                    total_llm += 1
                    if review:
                        total_low_conf += 1
                    update_rows.append((llm_code, "llm", conf, review, cr_no))
                # 未能標記 → 跳過（industry_tag 維持 NULL）

            if update_rows:
                con.executemany(
                    """
                    UPDATE master SET
                        industry_tag      = ?,
                        industry_method   = ?,
                        industry_conf     = ?,
                        industry_review   = ?
                    WHERE cr_no = ?
                    """,
                    update_rows,
                )
                total_tagged += len(update_rows)

            offset += batch_size
            logger.info(f"進度：offset={offset}, 已標記={total_tagged}")

        con.close()
        logger.info(
            f"行業標籤完成：總標記={total_tagged}，其中 LLM={total_llm}，"
            f"低信心待覆核={total_low_conf}"
        )
        return {
            "total_tagged": total_tagged,
            "llm_tagged": total_llm,
            "low_confidence": total_low_conf,
        }

    def _ensure_industry_columns(self, con: duckdb.DuckDBPyConnection):
        """若 master 表尚未有行業欄位，動態 ALTER TABLE 加入。"""
        existing = {row[0] for row in con.execute("PRAGMA table_info('master')").fetchall()}
        cols_to_add = {
            "industry_tag": "VARCHAR",      # HKICLS 代碼，如 'F'
            "industry_name_zh": "VARCHAR",  # 行業中文名，如 '建造業'
            "industry_name_en": "VARCHAR",  # 行業英文名
            "industry_method": "VARCHAR",   # 標籤來源：keyword / llm
            "industry_conf": "DOUBLE",       # 信心分數 0-1
            "industry_review": "BOOLEAN",   # 低信心待人工覆核
        }
        for col, dtype in cols_to_add.items():
            if col not in existing:
                con.execute(f"ALTER TABLE master ADD COLUMN {col} {dtype}")
                logger.info(f"已新增欄位 master.{col}")

        # 補全 industry_name_zh / name_en（根據 industry_tag）
        # 以後每次 run() 後呼叫
        self._backfill_industry_names(con)

    def _backfill_industry_names(self, con: duckdb.DuckDBPyConnection):
        """根據 industry_tag 填回行業中英文名稱。"""
        for code, info in HKICLS_MAP.items():
            con.execute(
                "UPDATE master SET industry_name_zh = ?, industry_name_en = ? "
                "WHERE industry_tag = ? AND industry_name_zh IS NULL",
                [info["name_zh"], info["name_en"], code],
            )
