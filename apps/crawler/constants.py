"""
104人力銀行爬蟲常數定義

此模組包含爬蟲使用的所有常數，集中管理以提高可維護性。
"""

# 網站相關常數
WEBSITE_BASE_URL = "https://www.104.com.tw"
SEARCH_API_URL = "https://www.104.com.tw/jobs/search/list"
JOB_DETAIL_API_URL = "https://www.104.com.tw/job/ajax/content/{}"  # 職缺ID佔位符

# 請求相關常數
MAX_RETRIES = 3
MIN_DELAY = 1.5  # 最小延遲秒數
MAX_DELAY = 5.0  # 最大延遲秒數
MAX_CONCURRENCY = 10  # 最大併發請求數

# User-Agent列表，用於輪換避免被封鎖
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1"
]

# 默認請求頭
DEFAULT_HEADERS = {
    "Referer": "https://www.104.com.tw/jobs/search/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
}

# 目標城市列表
TARGET_CITIES = {'台北市', '新北市', '桃園', '新竹'}

# 職缺狀態常數
JOB_STATUS_ACTIVE = 'active'
JOB_STATUS_INACTIVE = 'inactive'

# 資料庫相關常數
MONGODB_COLLECTION_JOBS = 'jobs'
MONGODB_COLLECTION_DAILY = 'daily'

# 檔案格式常數
CSV_ENCODING = 'utf-8-sig'
JSON_INDENT = 2