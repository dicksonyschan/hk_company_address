# hk_company_address

香港公司地址正規化工具 — 自動下載**公司註冊處（CR）**公開地址資料，透過政府 **ALS（Address Lookup Service）API** 標準化，並寫入本地 **DuckDB** 資料庫，建立「公司名稱 → 標準化地址」主檔。

---

## 目錄結構

```
hk_company_address/
├── README.md
├── requirements.txt
├── config.yaml              # 所有可調參數
├── src/
│   ├── __init__.py
│   ├── cr_downloader.py     # 下載 CR 公開資料
│   ├── address_cleaner.py   # 地址前處理 / 清洗
│   ├── als_client.py        # Async ALS API 呼叫 + cache
│   ├── db_writer.py         # DuckDB 讀寫
│   └── pipeline.py          # 串接所有步驟的主流程
├── data/
│   ├── raw/                 # CR 原始下載 (自動建立)
│   ├── cleaned/             # 清洗後暫存 (自動建立)
│   └── alias_map.json       # 地址別名對照表 (可自行擴充)
├── tests/
│   ├── sample_addresses.csv # 測試用地址樣本
│   └── test_pipeline.py     # 回歸測試
└── main.py                  # 主程式入口
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
- `cr.page_size`：每次向 CR API 抓取筆數（預設 1000）

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

只處理自上次執行後 CR 新增或修改的記錄，大幅節省 API 呼叫量。

### 單筆地址查詢（測試用）

```bash
python main.py --lookup "九龍旺角彌敦道123號ABC大廈5樓"
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
