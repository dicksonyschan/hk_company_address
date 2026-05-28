# hk_company_address

香港公司地址正規化工具 — 自動下載**公司註冊處（CR）**公開地址資料，透過政府 **ALS（Address Lookup Service）API** 標準化，並寫入本地 **DuckDB** 資料庫，建立「公司名稱 → 標準化地址」主檔。

---

## 目錄結構

```
hk_company_address/
├── README.md
├── requirements.txt
├── config.yaml                  # 所有可調參數（含 cr_brn 區塊）
├── src/
│   ├── __init__.py
│   ├── cr_downloader.py          # 下載 CR 公開資料（前綴掃描模式）
│   ├── cr_downloader_brn.py      # BRN 盲查下載器（隨機抽取掃描）
│   ├── address_cleaner.py        # 地址前處理 / 清洗
│   ├── als_client.py             # Async ALS API 呼叫 + cache
│   ├── db_writer.py              # DuckDB 讀寫（含 brn_scan_queue）
│   ├── pipeline.py               # 串接所有步驟的主流程
│   ├── cr_industry_codes.py      # 從 data.gov.hk 下載 HSIC 代碼表
│   └── industry_tagger.py        # 行業標籤（關鍵詞 + DeepSeek LLM）
├── data/
│   ├── raw/                     # CR 原始下載 (自動建立)
│   ├── cleaned/                 # 清洗後暫存 (自動建立)
│   └── alias_map.json           # 地址別名對照表 (可自行擴充)
├── tests/
│   ├── sample_addresses.csv     # 測試用地址樣本
│   └── test_pipeline.py         # 回歸測試（含 BRN 測試）
└── main.py                      # 主程式入口
```

---

## 環境需求

| 項目 | 版本 |
|------|------|
| Python | 3.11+ |
| 作業系統 | macOS (Apple Silicon M1) |
| RAM | 建議 8GB（本程式約用 1–2GB）|
| 網絡 | 需可訪問 data.cr.gov.hk 及 als.ogcio.gov.hk |

---

## 安裝步驟

### 1. Clone 本 Repo

```bash
git clone https://github.com/dicksonyschan/hk_company_address.git
cd hk_company_address
```

### 2. 建立虛擬環境（建議）

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. 安裝依賴套件

```bash
pip install -r requirements.txt
```

> **注意**：`opencc-python-reimplemented` 在 Apple Silicon 上須用 Rosetta 或 native arm64 build。若安裝失敗，可改用 `opencc`：
> ```bash
> pip install opencc
> ```

### 4. 確認設定檔

編輯 `config.yaml`，主要可調整：
- `als.concurrency`：ALS 同時連線數（建議 5–10）
- `als.confidence_threshold`：低於此分數標記人工覆核（預設 70）
- `db.path`：DuckDB 資料庫檔案路徑
- `cr.downloader`：下載器模式，`prefix`（前綴掃描，預設）或 `brn`（BRN 盲查）
- `cr_brn.*`：BRN 盲查相關參數（掃描範圍、並發數、反爬設定等）

---

## 執行方式

### 全量執行（首次使用）

```bash
python main.py --mode full
```

執行流程：
1. 從 `data.cr.gov.hk` 分頁下載所有本地公司地址
2. 清洗地址（去雜訊、繁簡統一、別名替換）
3. 對 unique 地址批次呼叫 ALS API（自動 cache，重複地址不重複呼叫）
4. 將結果寫入 DuckDB（`data/hk_companies.duckdb`）
5. 輸出摘要報告（命中率、低信心筆數等）

### 增量更新（每日）

```bash
python main.py --mode delta
```

### 行業代碼初始化

```bash
python main.py --init-hsic
```

### 行業標籤

```bash
export DEEPSEEK_API_KEY=your_key_here
python main.py --tag-industry
```

只處理自上次執行後 CR 新增或修改的記錄，大幅節省 API 呼叫量。

> **注意**：增量模式需提供昨日 Parquet 路徑：
> ```bash
> python main.py --mode delta --yesterday data/raw/cr_raw_20260527.parquet
> ```

### 單筆地址查詢（測試用）

```bash
python main.py --lookup "九龍旺角彌敦道123號ABC大廈5樓"
```

### BRN 盲查模式

透過逐筆查詢 BRN（Business Registration Number）找出未被前綴掃描涵蓋的公司記錄。

**步驟一：初始化 BRN 掃描佇列（首次使用）**

```bash
# numeric 模式：掃描 00000000–99999999（約需數分鐘寫入佇列）
python main.py --init-brn-queue
```

> 佇列資料寫入 DuckDB 的 `brn_scan_queue` 表。可重複執行，已存在的 BRN 自動跳過。

**步驟二：執行 BRN 盲查**

```bash
# 每次隨機抽取 10,000 筆 pending BRN 查詢
python main.py --mode full --downloader brn
```

- 查詢後自動更新 `hit`（有記錄）/ `miss`（無此 BRN）狀態
- 中斷或失敗的 BRN 保持 `pending`，下次自動重試
- 可重複排程執行，每次自動續掃

**步驟三：查看掃描進度**

```bash
python main.py --scan-status
```

輸出範例：
```
=== BRN 佇列進度 ===
  pending: 99850000
  hit: 125000
  miss: 25000
  last_batch_id: a1b2c3d4
  last_queried_at: 2026-05-28T10:30:00
  scanned_pct: 0.15%
  hit_rate_of_scanned: 83.33%
```

**步驟四：重複步驟二**（可排程，每次自動從 pending 繼續）

#### config.yaml BRN 盲查設定

```yaml
cr_brn:
  mode: numeric             # "numeric" 或 "prefix"

  # numeric 模式：掃描 00000000–99999999
  start: 0
  end: 99999999

  # prefix 模式：掃描字母前綴 + 數字
  prefixes: ["C", "G", "L", "F", "E", "H", "N", "U", "Z", "B", "D"]
  prefix_start: 1000000
  prefix_end: 4000000

  fetch_batch_size: 10000   # 每次隨機抽取筆數
  concurrency: 20           # 並發查詢數
  miss_limit: 2000          # 本批連續 miss 門檻（不終止整體掃描）
  batch_write: 2000         # hit 記錄 Parquet flush 門檻
  jitter_min: 0.05          # 最小 Jitter 延遲（秒）
  jitter_max: 0.30          # 最大 Jitter 延遲（秒）
  cb_threshold: 10          # Circuit Breaker 連續失敗門檻
  cb_cooldown: 60           # Circuit Breaker 冷卻時間（秒）
```

### 回歸測試

```bash
python -m pytest tests/
```

---

## 輸出資料庫結構

資料庫位於 `data/hk_companies.duckdb`，包含以下主要表：

### `companies_raw`
CR 原始下載資料（不修改）

| 欄位 | 說明 |
|------|------|
| cr_no | 公司註冊編號 |
| name_zh | 中文公司名稱 |
| name_en | 英文公司名稱 |
| address_raw | 原始地址字串 |
| fetched_at | 下載時間戳 |

### `address_cache`
ALS API 呼叫快取（避免重複請求）

| 欄位 | 說明 |
|------|------|
| address_hash | 清洗後地址的 SHA-1 |
| address_clean | 清洗後地址字串 |
| als_json | ALS 完整回應 (JSON) |
| geo_address | 19 字元地理地址碼 |
| score | GeoreferencingScore |
| fetched_at | 呼叫時間戳 |

### `brn_scan_queue`
BRN 盲查掃描佇列（由 `--init-brn-queue` 建立）

| 欄位 | 說明 |
|------|------|
| brn | Business Registration Number（主鍵）|
| status | `pending`（待查）/ `hit`（有結果）/ `miss`（無此 BRN）|
| queried_at | 最後查詢時間 |
| batch_id | 所屬批次 ID |

> `miss` 即為 miss cache，與 `status='hit'` 同表管理，無需獨立 miss_cache 表。

### `master`
最終主檔（公司名稱 → 標準化地址）

| 欄位 | 說明 |
|------|------|
| cr_no | 公司註冊編號 |
| name_zh / name_en | 公司名稱 |
| geo_address | ALS 地理地址碼 |
| region | 地區（香港/九龍/新界）|
| district | 18 區分區 |
| street_name | 街道名稱 |
| building_name | 大廈/屋苑名稱 |
| latitude / longitude | 座標 |
| confidence | ALS 信心分數 |
| manual_review | 是否需人工覆核 |
| last_updated | 最後更新時間 |

---

## 行業分類功能

### 目錄結構（更新）

```
src/
├── cr_industry_codes.py  # 從 data.gov.hk 下載 HSIC 代碼表
└── industry_tagger.py    # 行業標籤主模組（關鍵詞 + DeepSeek LLM）
```

### 行業資料庫結構

#### `industry_codes`（HSIC 參考表）

| 欄位 | 說明 |
|------|------|
| code | HSIC 代碼（字母大類 A-S 或數字細類） |
| level | 層級（1=大類, 2=中類, 3=小類） |
| name_zh | 行業中文名稱 |
| name_en | 行業英文名稱 |
| parent_code | 父層代碼 |

#### `master` 表新增行業欄位

| 欄位 | 說明 |
|------|------|
| industry_tag | HSIC 大類代碼（如 "F"=建造業）|
| industry_name_zh | 行業中文名稱 |
| industry_name_en | 行業英文名稱 |
| industry_method | 標籤來源：`keyword`（關鍵詞比對）/ `llm`（DeepSeek 分類）|
| industry_conf | 信心分數（0–1）|
| industry_review | 低信心待人工覆核（`TRUE`/`FALSE`）|

### 行業功能 CLI

**步驟一：初始化 HSIC 代碼表（首次使用）**

```bash
python main.py --init-hsic
```

從 `data.gov.hk` 下載完整 HSIC 代碼表，寫入 `industry_codes` 表。
若政府 API 不可用，程式會直接報錯（不自動 fallback），請檢查網絡後重試。

**步驟二：執行行業標籤**

```bash
python main.py --tag-industry
```

- **第一層**：關鍵詞規則比對（離線，極快）
- **第二層**：未命中的送 DeepSeek API 批次分類（需設定 `DEEPSEEK_API_KEY`）
- 信心分數 < 閾值（預設 0.8）的記錄標記 `industry_review = TRUE`

**環境變數設定**

```bash
# .env 檔（不要 commit 到 git）
DEEPSEEK_API_KEY=your_key_here
```

```bash
# 或直接設定
export DEEPSEEK_API_KEY=your_key_here
python main.py --tag-industry
```

### 人工覆核流程

查詢需覆核的行業記錄：

```python
import duckdb
con = duckdb.connect('data/hk_companies.duckdb')
df = con.execute("""
    SELECT cr_no, name_zh, industry_tag, industry_conf
    FROM master
    WHERE industry_review = TRUE
    ORDER BY industry_conf
    LIMIT 20
""").df()
print(df)
```

人工確認後，更新並清除覆核旗標：

```python
con.execute("""
    UPDATE master SET
        industry_tag    = 'K',  -- 正確的 HSIC 代碼
        industry_review = FALSE
    WHERE cr_no = '12345678'
""")
```

### config.yaml 行業設定

```yaml
industry:
  keywords_path: data/industry_keywords.json   # 關鍵詞表路徑
  confidence_threshold: 0.8                     # 低於此值標記人工覆核
  enable_llm_fallback: true                     # 是否啟用 DeepSeek 二層分類
  enable_llm_keyword_mining: true               # 是否自動擷取高頻詞擴充關鍵詞表
  deepseek_model: deepseek-v4-flash             # DeepSeek 模型名稱
  auto_mine_threshold: 10000                    # 未標記記錄超過此數量觸發自動挖掘
  batch_size: 50                                # DeepSeek API 每批筆數
```

---

## 注意事項

### API 使用
- CR 及 ALS 均為香港政府免費開放資料，使用時須遵守各自使用條款及標示出處
- ALS 無官方公布速率限制，**請勿超過 20 req/s**，程式預設已加入限流
- 建議排程在凌晨 02:00–05:00 (HKT) 執行，避開政府系統高峰時段

### 資料準確性
- ALS 每月更新，CR 每日更新；主檔需定期 re-sync
- 3D 地址（含樓層/單位）目前只適用於公屋，私人樓宇只能標準化至大廈門牌層級
- `confidence < 70` 的記錄已標記 `manual_review = True`，建議人工確認

### 個人資料
- 若你的 CSV 含個人姓名、電話等個人資料，請遵守《個人資料（私隱）條例》（PDPO）
- 本程式所有處理均在本地進行，不會上傳資料至任何第三方

### M1 記憶體
- 全量資料（百萬級）採用 Polars streaming，記憶體用量約 1–2GB
- 若出現 OOM，可調低 `config.yaml` 的 `cr.chunk_size`

---

## 常見問題

**Q: ALS API 回傳空結果？**  
A: 先檢查輸入地址是否含多餘雜訊（公司名、電話、「香港特別行政區」等），`address_cleaner.py` 的清洗規則可按需擴充。

**Q: opencc 安裝失敗？**  
A: 在 M1 上可改用 `pip install opencc`（非 reimplemented 版），修改 `address_cleaner.py` import 即可。

**Q: DuckDB 如何查詢？**  
```python
import duckdb
con = duckdb.connect('data/hk_companies.duckdb')
df = con.execute("SELECT * FROM master WHERE district = '油尖旺區' LIMIT 10").df()
print(df)
```

---

## 資料來源

- [公司註冊處開放資料](https://data.cr.gov.hk) — 香港公司註冊處
- [地址搜尋服務 (ALS)](https://www.als.ogcio.gov.hk/) — 香港數字政策辦公室
- [data.gov.hk](https://data.gov.hk) — 香港政府資料一站通
