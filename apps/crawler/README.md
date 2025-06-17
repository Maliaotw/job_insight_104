# 104人力銀行職缺爬蟲系統

此目錄包含用於爬取104人力銀行網站職缺資料的爬蟲系統。系統經過重構，遵循現代軟體設計原則，提供更好的可維護性、可擴展性和可測試性。

## 設計原則

重構後的架構遵循以下設計原則：

1. **單一職責原則 (SRP)**：每個類只負責一項獨立功能
2. **開放/封閉原則 (OCP)**：系統可以輕鬆擴展而無需修改現有程式碼
3. **依賴反轉原則 (DIP)**：高層模組不依賴低層模組，兩者都依賴抽象

## 系統架構

系統由以下主要組件組成：

### 1. 常數定義 (`constants.py`)

集中管理所有常數，提高可維護性。

### 2. 資料儲存 (`storage.py`)

提供抽象的資料儲存介面和具體實現：

- `JobStorage`：抽象基類，定義儲存和檢索職缺資料的標準介面
- `MongoDBJobStorage`：使用MongoDB作為後端儲存職缺資料
- `FileJobStorage`：使用檔案系統（CSV和JSON）作為後端儲存職缺資料

### 3. 資料處理 (`processor.py`)

負責處理爬取到的職缺資料：

- `JobDataProcessor`：處理職缺資料，包括合併、狀態處理、字段處理等

### 4. 搜索功能 (`searcher.py`)

負責搜索104人力銀行網站上的職缺：

- `JobSearcher`：搜索職缺，包括構建URL、發送請求、處理響應等

### 5. 協調器 (`orchestrator.py`)

協調整個爬蟲流程：

- `CrawlerOrchestrator`：協調搜索、處理和儲存職缺資料的流程

### 6. 爬蟲主類 (`crawler_v2.py`)

提供簡單的介面來啟動爬蟲：

- `CrawlerV2`：爬蟲系統的主入口點，使用協調器模式來組織爬蟲流程

## 使用方法

### 基本用法

```python
from app.crawler.crawler_v2 import CrawlerV2

# 創建爬蟲實例
crawler = CrawlerV2()

# 執行爬蟲
jobs = crawler.run(keywords=['Python', 'Java', 'DevOps'])

# 輸出結果
print(f"共爬取到 {len(jobs)} 筆職缺資料")
```

### 指定輸出目錄

```python
from app.crawler.crawler_v2 import CrawlerV2

# 創建爬蟲實例，指定輸出目錄
crawler = CrawlerV2(output_dir="path/to/output")

# 執行爬蟲
jobs = crawler.run(keywords=['Python', 'Java', 'DevOps'])
```

### 使用命令行腳本

系統提供了一個命令行腳本來執行爬蟲：

```bash
python scripts/run_crawler_v2.py --keywords Python Java DevOps --output-dir path/to/output
```

參數說明：
- `--keywords`：要搜索的關鍵字列表，默認為 `['Python', 'Java', 'DevOps']`
- `--output-dir`：保存爬取數據的目錄，默認使用專案的預設目錄

## 自定義儲存後端

系統支援自定義儲存後端，只需實現 `JobStorage` 介面：

```python
from app.crawler.storage import JobStorage
from app.crawler.crawler_v2 import CrawlerV2

# 自定義儲存後端
class CustomStorage(JobStorage):
    # 實現所有抽象方法
    ...

# 創建爬蟲實例，使用自定義儲存後端
storage = CustomStorage()
crawler = CrawlerV2()
crawler.storage = storage

# 執行爬蟲
jobs = crawler.run(keywords=['Python', 'Java', 'DevOps'])
```

## 與舊版爬蟲的區別

新版爬蟲 (`CrawlerV2`) 與舊版爬蟲 (`Crawler`) 的主要區別：

1. **模組化設計**：新版爬蟲將功能分解為多個專注於特定任務的類
2. **可擴展性**：新版爬蟲支援自定義儲存後端和搜索策略
3. **錯誤處理**：新版爬蟲提供更好的錯誤處理和資源管理
4. **可測試性**：新版爬蟲的模組化設計使得單元測試更容易

## 遷移指南

從舊版爬蟲遷移到新版爬蟲很簡單：

1. 將 `from app.crawler.crawler import Crawler` 改為 `from app.crawler.crawler_v2 import CrawlerV2`
2. 將 `crawler = Crawler()` 改為 `crawler = CrawlerV2()`

其餘的API保持不變，例如 `crawler.run(keywords)` 方法。