# Job Insight 104 - 職缺市場洞察平台

這是一個用於爬取、處理和可視化104人力銀行職缺數據的綜合平台。該專案旨在提供對台灣就業市場的深入分析和洞察。

## 線上演示

您可以通過以下鏈接訪問我們的線上演示系統，體驗職缺市場洞察平台的全部功能：

[https://job-insight-analysis-nupjrozvbq-ue.a.run.app/](https://job-insight-analysis-nupjrozvbq-ue.a.run.app/)

該演示系統部署在Google Cloud Run上，提供了實時的職缺數據分析和可視化功能。您可以探索各種圖表和分析報告，了解台灣就業市場的最新趨勢和洞察。

## 專案結構

專案由三個主要組件組成：

### 1. 爬蟲腳本 (apps/crawler)

爬蟲組件負責從104人力銀行網站收集職缺數據。

**主要功能：**
- 根據關鍵字搜索職缺
- 處理和清洗職缺數據
- 將數據存儲到MongoDB和文件系統（CSV/JSON）

**核心文件：**
- `crawler.py`：原始爬蟲實現
- `crawler_v2.py`：重構版本的爬蟲，遵循現代軟體設計原則
- `orchestrator.py`：協調爬蟲流程
- `processor.py`：處理爬取的數據
- `searcher.py`：處理搜索功能
- `storage.py`：管理數據存儲

爬蟲系統遵循單一職責原則(SRP)、開放/封閉原則(OCP)和依賴反轉原則(DIP)，提供更好的可維護性、可擴展性和可測試性。

### 2. 定時任務 (apps/scheduler)

調度器組件負責自動化數據收集和處理流程。

**主要功能：**
- 按照預定時間運行爬蟲
- 將數據從MongoDB遷移到DuckDB
- 將數據導出為Parquet格式並上傳到S3

**核心文件：**
- `scheduler.py`：使用APScheduler設置定時任務

調度器使用cron表達式配置任務執行時間，可以通過配置文件進行自定義。

### 3. 圖表可視化 (apps/visualization)

可視化組件提供了一個基於Streamlit的Web應用程序，用於分析和可視化收集到的職缺數據。

**主要功能：**
- 總覽儀表板
- 每日職缺變化分析
- 產業職缺分佈與趨勢
- 招聘效率分析
- 薪資與地區分析

**核心文件和目錄：**
- `app.py`：主應用程序文件
- `analysis/`：數據分析模塊
- `components/`：可重用UI組件
- `nav/`：頁面導航
- `pages/`：各個分析頁面

可視化應用程序遵循MVC架構，提供直觀的用戶界面來探索職缺數據。

## 技術堆棧

- **爬蟲**：Python, Requests, BeautifulSoup
- **數據存儲**：MongoDB, DuckDB, AWS S3
- **調度**：APScheduler
- **可視化**：Streamlit, Plotly
- **部署**：Docker, Google Cloud Platform

## 安裝與運行

### 前提條件

- Python 3.8+
- Docker (可選，用於容器化部署)
- MongoDB
- AWS帳戶 (用於S3存儲)

### 使用Docker

```bash
# 構建並啟動容器
docker-compose up -d
```

### 手動安裝

```bash
# 創建虛擬環境
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

# 安裝依賴
pip install -r requirements.txt

# 運行爬蟲
python -m apps.crawler.crawler_v2

# 運行調度器
python -m apps.scheduler.scheduler

# 運行可視化應用
streamlit run apps/visualization/app.py
```

## 配置

主要配置文件位於`config/`目錄中：

- `settings.py`：全局設置
- `code_tables.py`：代碼表和映射

### 環境變數

專案使用`.env`文件來配置環境變數。您可以複製`.env.example`文件並重命名為`.env`，然後根據您的環境進行修改。

以下是環境變數的範例：

```dotenv
# MongoDB 連接配置
# 替換 username 和 password 為您的 MongoDB 用戶名和密碼
MONGODB_CONNECTION_STRING=mongodb://username:password@localhost:27017/
MONGODB_DB_NAME=job_insight_104
MONGODB_AUTH_SOURCE=admin

CURRENT_ENV=dev

# 日誌設置
LOGGING_LEVEL=INFO

# 數據庫設置
DATABASE_PROCESSED_DATA_PATH=data/processed_job_data.duckdb

# 爬蟲設置
CRAWLER_OUTPUT_DIR=data/raw_data
CRAWLER_KEYWORDS=flask,Python,DevOps,SRE,fastapi,django
CRAWLER_MAX_PAGES=0
CRAWLER_CONCURRENCY=10
CRAWLER_SCHEDULE_DAILY_CRAWL_HOUR=12
CRAWLER_SCHEDULE_DAILY_CRAWL_MINUTE=0

# AWS 設置 (用於S3存儲)
AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION=us-east-1
AWS_S3_BUCKET=your-bucket-name

```

## 貢獻

歡迎提交問題和拉取請求。對於重大更改，請先開啟一個問題來討論您想要更改的內容。

## 許可證

[MIT](LICENSE)
