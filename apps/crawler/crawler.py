import asyncio
import itertools
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union
from urllib.parse import urlencode

import pandas as pd

from config.settings import BASE_DIR, logger
from src.database.mongodb_manager import MongoDBManager
from src.utils.http_adapter import AsyncHttpAdapter
from src.utils.text_processing import (
    extract_lowest_level_area_codes,
    split_link_field,
    split_city_district,
)


# 此模塊實現了一個爬蟲系統，用於從104人力銀行網站爬取職缺數據
# 整體工作流程：
# 1. 初始化爬蟲（設定輸出目錄、HTTP適配器等）
# 2. 獲取台灣地區代碼
# 3. 根據關鍵字和地區搜索職缺
# 4. 處理搜索結果（合併、狀態處理、字段處理）
# 5. 保存數據（CSV、JSON、資料庫）
#
# 主要類：
# - Crawler: 主爬蟲類，包含所有爬取和處理邏輯
#
# 主要工具函數：
# - extract_lowest_level_area_codes: 提取台灣地區代碼
# - split_link_field: 解析職缺鏈接
# - split_city_district: 分割台灣地址為城市和地區


class Crawler:
    """
    104人力銀行職缺資料爬蟲類

    此類提供方法來搜索職缺、獲取職缺詳情，
    並將收集到的數據保存到文件或資料庫中。

    實作細節：
    - 使用非同步HTTP請求提高效率
    - 支持多種搜索條件（關鍵字、地區、產業等）
    - 提供CSV、JSON和資料庫三種數據保存方式
    - 實現了分頁爬取和錯誤處理機制
    - 使用User-Agent輪換和隨機延遲避免被封鎖
    """

    WEBSITE_BASE_URL = "https://www.104.com.tw"  # 104網站基礎URL
    SEARCH_API_URL = "https://www.104.com.tw/jobs/search/list"  # 104搜索API
    JOB_DETAIL_API_URL = (
        "https://www.104.com.tw/job/ajax/content/{}"  # 職缺詳情API，{}將被替換為職缺ID
    )

    MONGO_MANAGER = MongoDBManager()

    # 多個User-Agent列表，用於輪換避免被封鎖
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1",
    ]

    # 請求重試次數和延遲設定
    MAX_RETRIES = 3
    MIN_DELAY = 1.5  # 最小延遲秒數
    MAX_DELAY = 5.0  # 最大延遲秒數

    def __init__(self, output_dir: Union[str, Path] = None):
        """
        初始化職缺爬蟲

        參數:
            output_dir: 保存爬取數據的目錄。默認為None，將使用專案的預設目錄。

        實作細節:
            - 設定輸出目錄，如未指定則使用預設的data/raw_data目錄
            - 確保輸出目錄存在，若不存在則創建
            - 初始化HTTP請求適配器，處理請求頭、重試邏輯等
            - 設定時間戳記，用於生成文件名
            - 設定併發控制相關屬性
        """
        logger.info("初始化 JobCrawler 爬蟲實例")

        # 設定輸出目錄
        self.output_dir = self._setup_output_directory(output_dir)

        # 初始化HTTP請求適配器
        self.http_adapter = AsyncHttpAdapter(
            max_retries=self.MAX_RETRIES,
            min_retry_delay=self.MIN_DELAY,
            max_retry_delay=self.MAX_DELAY,
            user_agents=self.USER_AGENTS,
            rotate_user_agent=True,
            headers={
                "Referer": "https://www.104.com.tw/jobs/search/",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )

        # 設定時間戳記，用於生成文件名
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 在一次用高併發會獲取所有的數據
        self.max_concurrency = 10  # 假設同時最多10個
        self.semaphore = asyncio.Semaphore(self.max_concurrency)
        self.today = datetime.now().strftime("%Y-%m-%d")

    def _setup_output_directory(self, output_dir: Union[str, Path, None]) -> Path:
        """設定並創建輸出目錄"""
        if output_dir:
            directory = Path(output_dir) if isinstance(output_dir, str) else output_dir
        else:
            directory = BASE_DIR / "data" / "raw_data"

        logger.debug(f"設定輸出目錄: {directory}")
        directory.mkdir(parents=True, exist_ok=True)
        logger.debug("確保輸出目錄存在")

        return directory

    def build_url(
        self,
        keyword: str = "",
        page: int = 1,
        area: str = "",
        industry: str = "",
        job_category: str = "",
        experience: str = "",
        education: str = "",
    ):
        """
        構建104人力銀行職缺搜索API的URL

        參數:
            keyword: 搜索關鍵字
            page: 頁碼，默認為第1頁
            area: 地區代碼
            industry: 產業代碼
            job_category: 職務類別代碼
            experience: 經驗要求
            education: 學歷要求

        返回:
            str: 完整的搜索URL，包含所有查詢參數

        實作細節:
            - 將所有搜索參數組合成字典
            - 使用urlencode將參數轉換為URL查詢字符串
            - 將查詢字符串附加到API基礎URL後
            - 記錄完整URL以便調試
            - 支持多種搜索條件組合，靈活構建不同的搜索請求
        """
        # 構建搜索參數字典
        params = {
            "keyword": keyword,  # 搜索關鍵字
            "page": page,  # 頁碼
            "area": area,  # 地區代碼
            "industry": industry,  # 產業代碼
            "jobCat": job_category,  # 職務類別代碼
            "exp": experience,  # 經驗要求
            "edu": education,  # 學歷要求
            "mode": "s",  # 搜索模式
            "jobsource": "index",  # 來源頁面
        }
        logger.debug(f"搜索參數: {params}")
        complete_url = f"{self.SEARCH_API_URL}?{urlencode(params)}"
        logger.debug(f"完整URL={complete_url}")
        return complete_url

    async def search_jobs(self, url: str) -> Dict:
        """
        非同步搜索職缺，發送HTTP請求並處理響應

        參數:
            url: 完整的搜索URL，包含所有查詢參數

        返回:
            Dict: 包含搜索結果的字典或錯誤信息

        實作細節:
            - 使用HTTP適配器發送非同步GET請求
            - 驗證響應格式，確保包含預期的數據結構
            - 記錄重要統計信息（總頁數、總筆數）
            - 處理可能的錯誤情況，返回標準化的錯誤信息
            - 支持後續的分頁處理和數據提取
        """
        # 步驟1: 發送HTTP請求
        # 使用HTTP適配器發送非同步GET請求，獲取搜索結果
        logger.debug(f"開始發送HTTP請求: {url}")
        response_data = await self.http_adapter.get(url=url)
        logger.debug(f"HTTP請求完成: {url}")

        # 步驟2: 檢查是否有錯誤
        # 如果結果中包含"error"字段，表示請求過程中發生了錯誤
        if "error" in response_data:
            logger.warning(f"請求返回錯誤: {response_data['error']}, URL: {url}")
            return response_data

        # 步驟3: 檢查響應是否包含預期的數據結構
        # 確保響應中包含"data"字段，這是API的標準格式
        if "data" not in response_data:
            logger.warning(
                f"響應中未找到'data'字段，可能是API結構變更或請求被限制, URL: {url}"
            )
            return {"error": "響應格式不正確，未找到'data'字段"}

        # 步驟4: 記錄重要統計信息
        # 從響應中提取總頁數和總筆數，記錄到日誌中
        # 這些信息對於了解搜索結果的規模很有用

        # 添加總頁數信息到日誌中（如果存在）
        if "totalPage" in response_data["data"]:
            total_pages = response_data["data"]["totalPage"]
            logger.debug(f"總頁數: {total_pages}, URL: {url}")
        else:
            logger.debug(f"響應中未找到'totalPage'字段, URL: {url}")

        # 添加總筆數信息到日誌中（如果存在）
        if "totalCount" in response_data["data"]:
            total_jobs_count = response_data["data"]["totalCount"]
            logger.debug(f"總筆數: {total_jobs_count}, URL: {url}")

            # 如果總筆數大於0，記錄更詳細的信息
            if total_jobs_count > 0:
                logger.info(f"搜索結果: 找到 {total_jobs_count} 筆職缺, URL: {url}")
            else:
                logger.info(f"搜索結果: 未找到職缺, URL: {url}")
        else:
            logger.debug(f"響應中未找到'totalCount'字段, URL: {url}")

        # 步驟5: 檢查是否有職缺列表
        # 確保響應中包含"list"字段，這是包含職缺數據的字段
        if "list" in response_data["data"]:
            page_jobs_count = len(response_data["data"]["list"])
            logger.debug(f"本頁職缺數: {page_jobs_count}, URL: {url}")
        else:
            logger.warning(f"響應中未找到'list'字段，可能是API結構變更, URL: {url}")

        # 返回完整的搜索結果
        return response_data

    def get_taiwan_area_codes(self) -> Dict[str, str]:
        """
        獲取台灣地區的所有最下層地區代碼

        返回:
            Dict[str, str]: 地區代碼與地區名稱的映射字典

        實作細節:
            - 調用extract_lowest_level_area_codes函數獲取地區代碼
            - 該函數會從area_codes.json文件中提取台灣地區的最下層地區代碼
            - 只保留包含目標城市（台北市、新北市、桃園、新竹）的地區
            - 記錄提取到的地區數量和名稱，便於調試
            - 返回地區代碼和名稱的映射字典，用於後續搜索
        """
        # 使用台灣地區下的所有最下層地區代碼
        area_code_map = extract_lowest_level_area_codes()
        logger.info(
            f"從 area_codes.json 提取了 {len(area_code_map)} 個台灣地區下的最下層地區代碼"
        )
        logger.info(area_code_map.values())
        return area_code_map

    async def get_first_page(
        self, keyword: str, area_code: str, area_name: str
    ) -> Dict:
        """
        獲取特定關鍵字和地區的第一頁搜索結果，並生成所有頁面的URL列表

        參數:
            keyword: 搜索關鍵字
            area_code: 地區代碼
            area_name: 地區名稱

        返回:
            Dict: 包含搜索結果和所有頁面URL的字典

        實作細節:
            - 首先檢查資料庫中是否已有今天的搜索結果，避免重複爬取
            - 如果資料庫中沒有結果或結果有誤，則進行新的搜索
            - 使用信號量控制併發請求數量，避免過度請求
            - 從第一頁結果中獲取總頁數，然後生成所有頁面的URL
            - 將結果封裝為標準格式，包含狀態和完整URL列表
            - 處理可能的異常情況，確保即使出錯也能返回有效結果
        """
        # 步驟1: 構建第一頁的URL
        # 使用build_url方法構建完整的搜索URL，包含關鍵字和地區代碼
        logger.debug(f"構建第一頁URL: 關鍵字={keyword}, 地區={area_name}({area_code})")
        first_page_url = self.build_url(keyword=keyword, area=area_code, page=1)
        logger.debug(f"第一頁URL: {first_page_url}")

        # 步驟2: 檢查資料庫中是否已有今天的搜索結果
        # 使用MongoDB查詢，檢查是否已經爬取過這個關鍵字和地區的組合
        logger.debug(f"檢查資料庫中是否已有今天的搜索結果: {first_page_url}")
        cached_result = self.MONGO_MANAGER.db.daily.find_one(
            {"today": self.today, "first_url": first_page_url}
        )
        if cached_result:
            # 如果找到了記錄，記錄詳細信息
            logger.debug(f"在資料庫中找到記錄: {cached_result}")

            # 如果記錄狀態為"ok"，直接返回記錄，避免重複爬取
            if cached_result["status"] == "ok":
                logger.info(
                    f"使用緩存的結果，跳過爬取: 關鍵字={keyword}, 地區={area_name}"
                )
                return cached_result

        # 步驟3: 使用信號量控制併發請求數量
        # 信號量確保同時執行的請求數不超過設定的最大值
        logger.debug(f"等待信號量獲取許可: 關鍵字={keyword}, 地區={area_name}")
        async with self.semaphore:
            logger.debug(
                f"獲得信號量許可，開始爬取: 關鍵字={keyword}, 地區={area_name}"
            )
            try:
                # 步驟4: 發送請求獲取第一頁搜索結果
                logger.info(f"開始爬取第一頁: 關鍵字={keyword}, 地區={area_name}")
                all_page_urls = []  # 用於存儲所有頁面的URL

                # 發送請求獲取第一頁結果
                first_page_response = await self.search_jobs(url=first_page_url)

                # 步驟5: 從第一頁結果中獲取總頁數，然後生成所有頁面的URL
                total_pages = first_page_response["data"]["totalPage"]
                logger.info(
                    f"搜索結果: 關鍵字={keyword}, 地區={area_name}, 總頁數={total_pages}"
                )

                # 根據總頁數生成所有頁面的URL
                for page_number in range(1, total_pages + 1):
                    page_url = self.build_url(
                        keyword=keyword, area=area_code, page=page_number
                    )
                    all_page_urls.append(page_url)
                logger.info(
                    f"生成了 {len(all_page_urls)} 個頁面URL: 關鍵字={keyword}, 地區={area_name}"
                )

                # 步驟6: 將結果封裝為標準格式
                formatted_result = {
                    "today": self.today,
                    "first_url": first_page_url,
                    "crawl_url": first_page_url,
                    "keyword": keyword,
                    "area_code": area_code,
                    "area_name": area_name,
                    "status": "ok",
                    "result": all_page_urls,
                }
                logger.debug(
                    f"成功獲取第一頁並生成所有頁面URL: 關鍵字={keyword}, 地區={area_name}"
                )
            except Exception as e:
                # 記錄爬取失敗的錯誤
                logger.error(
                    f"爬取第一頁失敗: 關鍵字={keyword}, 地區={area_name}, 錯誤: {str(e)}"
                )

                # 即使失敗也返回標準格式的結果字典，只是狀態為"error"
                formatted_result = {
                    "today": self.today,
                    "first_url": first_page_url,
                    "crawl_url": first_page_url,
                    "keyword": keyword,
                    "area_code": area_code,
                    "area_name": area_name,
                    "status": "error",
                    "result": "",
                }

            return formatted_result

    async def search_keyword(
        self, keyword: str, area_code_map: Dict[str, str]
    ) -> List[str]:
        """
        搜索特定關鍵字在所有地區的職缺

        參數:
            keyword: 搜索關鍵字
            area_code_map: 地區代碼和名稱的字典

        返回:
            List[str]: 所有頁面的URL列表

        實作細節:
            - 為每個地區創建一個非同步任務，獲取第一頁結果
            - 使用asyncio.gather並行執行所有任務，提高效率
            - 將結果保存到資料庫，支持增量更新和緩存
            - 分離成功和失敗的結果，便於後續處理
            - 從成功結果中提取所有頁面的URL列表
            - 使用itertools.chain.from_iterable扁平化嵌套列表
            - 處理可能的異常情況，確保穩定性
        """
        # 創建任務列表，用於並行執行
        first_page_tasks = []

        # 記錄開始搜索的關鍵字和地區數量
        logger.info(f"開始搜索關鍵字 '{keyword}' 在 {len(area_code_map)} 個地區的職缺")

        # 為每個地區創建一個任務，獲取第一頁結果
        # 先只获取第一页，判断总页数，避免無效請求
        for area_code, area_name in area_code_map.items():
            # 創建獲取第一頁的任務
            first_page_task = self.get_first_page(keyword, area_code, area_name)
            first_page_tasks.append(first_page_task)

        # 記錄創建的任務數量
        logger.info(f"為關鍵字 '{keyword}' 創建了 {len(first_page_tasks)} 個搜索任務")

        # 並行執行所有任務
        logger.info(f"開始並行執行關鍵字 '{keyword}' 的搜索任務")
        first_page_results = await asyncio.gather(
            *first_page_tasks, return_exceptions=True
        )
        logger.info(f"關鍵字 '{keyword}' 的所有搜索任務執行完成")

        # 記錄詳細的結果信息（僅在調試模式下）
        logger.debug(f'關鍵字 "{keyword}" 搜索結果: {first_page_results}')

        # 將結果保存到資料庫，支持增量更新和緩存
        logger.info(f"將關鍵字 '{keyword}' 的搜索結果保存到資料庫")
        for result_item in first_page_results:
            # 使用upsert=True確保不會重複插入相同的記錄
            self.MONGO_MANAGER.db.daily.update_one(
                {
                    "today": result_item["today"],
                    "keyword": result_item["keyword"],
                    "first_url": result_item["first_url"],
                    "crawl_url": result_item["crawl_url"],
                },
                {"$set": result_item},
                upsert=True,
            )

        # 分離成功和失敗的結果
        successful_results = [
            result for result in first_page_results if result["status"] == "ok"
        ]
        failed_results = [
            result for result in first_page_results if result["status"] == "error"
        ]

        # 記錄成功和失敗的數量
        logger.info(
            f"關鍵字 '{keyword}' 搜索結果: 成功 {len(successful_results)} 個, 失敗 {len(failed_results)} 個"
        )

        # 從成功結果中提取所有頁面的URL列表
        # 使用itertools.chain.from_iterable扁平化嵌套列表
        all_page_urls = list(
            itertools.chain.from_iterable(
                [result["result"] for result in successful_results if result]
            )
        )

        # 記錄獲取到的URL數量
        logger.info(f"關鍵字 '{keyword}' 共獲取到 {len(all_page_urls)} 個頁面URL")

        return all_page_urls

    async def search_with_semaphore(self, keyword: str, url: str) -> Dict:
        """
        使用信號量控制併發的搜索請求

        參數:
            keyword: 搜索關鍵字
            url: 完整的搜索URL

        返回:
            Dict: 包含搜索結果的字典，標準化格式

        實作細節:
            - 首先檢查資料庫中是否已有今天的搜索結果，避免重複爬取
            - 使用信號量控制併發請求數量，確保不會同時發送過多請求
            - 將搜索結果封裝為標準格式，包含狀態和完整結果
            - 處理可能的異常情況，確保即使出錯也能返回有效結果
            - 支持緩存機制，避免重複請求相同的URL
        """
        # 記錄開始處理的URL（僅在調試模式下）
        logger.debug(f"處理URL: {url} (關鍵字: {keyword})")

        # 步驟1: 檢查資料庫中是否已有今天的搜索結果，避免重複爬取
        # 使用MongoDB查詢，檢查是否已經爬取過這個URL
        cached_result = self.MONGO_MANAGER.db.daily.find_one(
            {"today": self.today, "first_url": "", "crawl_url": url}
        )
        if cached_result:
            # 如果找到了記錄，記錄詳細信息（僅在調試模式下）
            logger.debug(f"在資料庫中找到URL的記錄: {url}")
            logger.debug(f"記錄詳情: {cached_result}")

            # 如果記錄狀態為"ok"，直接返回記錄，避免重複爬取
            if cached_result["status"] == "ok":
                logger.info(f"使用緩存的結果，跳過爬取: {url}")
                return cached_result

        # 步驟2: 使用信號量控制併發請求數量
        # 信號量確保同時執行的請求數不超過設定的最大值
        logger.debug(f"等待信號量獲取許可: {url}")
        async with self.semaphore:
            logger.debug(f"獲得信號量許可，開始爬取: {url}")
            try:
                # 發送請求獲取搜索結果
                logger.debug(f"發送請求: {url}")
                search_result = await self.search_jobs(url)
                logger.debug(f"請求成功: {url}")

                # 構建標準格式的結果字典
                formatted_response = {
                    "today": self.today,
                    "url": url,
                    "first_url": "",
                    "crawl_url": url,
                    "keyword": keyword,
                    "status": "ok",
                    "result": search_result,
                }

                # 記錄成功爬取
                logger.info(f"成功爬取URL: {url}")
                return formatted_response
            except Exception as e:
                # 記錄爬取失敗的錯誤
                logger.error(f"爬取URL失敗: {url}, 錯誤: {str(e)}")

                # 即使失敗也返回標準格式的結果字典，只是狀態為"error"
                return {
                    "today": self.today,
                    "url": url,
                    "first_url": "",
                    "crawl_url": url,
                    "keyword": keyword,
                    "status": "error",
                    "result": "",
                }

    async def main(self, keywords: List[str]) -> List[Dict]:
        """
        爬蟲主函數，協調整個爬取流程

        參數:
            keywords: 要搜索的關鍵字列表

        返回:
            List[Dict]: 所有爬取到的職缺數據

        實作細節:
            - 獲取所有目標地區的代碼和名稱
            - 為每個關鍵字在所有地區進行搜索，獲取所有頁面URL
            - 使用信號量控制併發請求，避免過度請求
            - 並行爬取所有URL的職缺數據
            - 將結果保存到資料庫，支持增量更新
            - 處理爬取結果，添加額外信息（爬取日期、狀態、關鍵字等）
            - 合併所有職缺數據，進行後處理（去重、格式化等）
            - 支持錯誤處理和異常恢復機制
        """
        # 步驟1: 獲取所有目標地區的代碼和名稱
        # 這些地區代碼將用於構建搜索URL
        logger.info("步驟1: 獲取台灣地區代碼")
        area_code_map = self.get_taiwan_area_codes()
        logger.info(f"獲取到 {len(area_code_map)} 個地區代碼")

        # 創建一個字典，用於存儲每個關鍵字對應的URL列表
        keyword_to_urls_mapping = defaultdict(list)

        # 步驟2: 為每個關鍵字在所有地區進行搜索，獲取所有頁面URL
        logger.info("步驟2: 為每個關鍵字獲取所有頁面URL")
        for keyword in keywords:
            logger.info(f"處理關鍵字: {keyword}")
            page_urls = await self.search_keyword(keyword, area_code_map)
            keyword_to_urls_mapping[keyword].extend(page_urls)
            logger.info(f"關鍵字 '{keyword}' 獲取到 {len(page_urls)} 個頁面URL")

        # 步驟3: 創建任務列表，準備並行爬取所有URL的職缺數據
        logger.info("步驟3: 創建爬取任務")
        search_tasks = []
        for keyword, url_list in keyword_to_urls_mapping.items():
            for page_url in url_list:
                search_tasks.append(self.search_with_semaphore(keyword, page_url))
        logger.info(f"共創建 {len(search_tasks)} 個爬取任務")

        # 步驟4: 並行執行所有任務
        logger.info("步驟4: 並行執行爬取任務")
        search_results = await asyncio.gather(*search_tasks, return_exceptions=True)
        logger.info(f"所有任務執行完成，獲取 {len(search_results)} 個結果")

        # 步驟5: 將結果保存到資料庫，支持增量更新
        logger.info("步驟5: 將結果保存到資料庫")
        for result_item in search_results:
            self.MONGO_MANAGER.db.daily.update_one(
                {
                    "today": result_item["today"],
                    "keyword": result_item["keyword"],
                    "first_url": result_item["first_url"],
                    "crawl_url": result_item["crawl_url"],
                },
                {"$set": result_item},
                upsert=True,
            )

        # 步驟6: 分離成功和失敗的結果
        logger.info("步驟6: 分離成功和失敗的結果")
        successful_results = [
            result for result in search_results if result["status"] == "ok"
        ]
        failed_count = sum(
            1 for result in search_results if result["status"] == "error"
        )
        logger.info(f"成功: {len(successful_results)}個, 失敗: {failed_count}個")

        current_date = datetime.now().strftime("%Y-%m-%d")

        # 步驟7: 處理成功的結果，添加元數據
        logger.info("步驟7: 處理成功的結果，添加元數據")
        total_job_count = 0
        for result in successful_results:
            keyword = result.get("keyword", "unknown")

            # 安全地訪問嵌套字典結構
            response_data = result.get("result", {})
            data = response_data.get("data", {})
            jobs_in_result = data.get("list", [])

            jobs_count = len(jobs_in_result)
            total_job_count += jobs_count

            logger.debug(f"處理關鍵字 '{keyword}' 的 {jobs_count} 個職缺")

            # 只有當 jobs_in_result 不為空時才處理
            if jobs_in_result:
                for job in jobs_in_result:
                    job["crawl_date"] = current_date
                    job["status"] = "active"  # 預設為上架狀態
                    job["search_keyword"] = keyword  # 添加搜索關鍵字，方便後續分析
                    job["area_code"] = job.get("jobAddrNo", "")
                    job["area_name"] = job.get("jobAddrNoDesc", "")
            else:
                logger.warning(
                    f"關鍵字 '{keyword}' 沒有找到職缺數據，響應結構: {response_data}"
                )

        logger.info(f"共處理 {total_job_count} 個職缺")

        # 步驟8: 合併所有職缺數據
        logger.info("步驟8: 合併所有職缺數據")
        all_jobs = []
        for result in successful_results:
            response_data = result.get("result", {})
            data = response_data.get("data", {})
            jobs_in_result = data.get("list", [])
            all_jobs.extend(jobs_in_result)

        logger.info(f"合併後共有 {len(all_jobs)} 個職缺")

        # 步驟9: 處理職缺數據（合併、狀態處理、字段處理、保存）
        logger.info("步驟9: 處理職缺數據")
        await self.process_jobs_data(all_jobs)

        return all_jobs

    def _process_job_status(self, jobs: List[Dict]) -> None:
        """
        處理職缺上下架狀態，使用 MongoDB 作為數據源

        參數:
            jobs: 職缺數據列表
        """
        logger.info("開始處理職缺上下架狀態 (使用 MongoDB)")

        if not jobs:
            return

        # 獲取當前日期
        today = datetime.now().strftime("%Y-%m-%d")

        # 從 MongoDB 獲取現有職缺
        mongo_manager = MongoDBManager()
        try:
            # 獲取資料庫中所有現有職缺的ID和狀態
            existing_job_dict = mongo_manager.get_existing_jobs()

            # 獲取當前爬取的所有職缺ID
            current_job_ids = [job["jobNo"] for job in jobs]

            # 更新資料庫中已存在但當前未爬取到的職缺狀態為下架
            if existing_job_dict:
                logger.info("更新已下架職缺狀態")

                # 使用 MongoDB 管理器的 update_job_status 方法更新職缺狀態
                mongo_manager.update_job_status(current_job_ids)

                # 找出需要重新激活的職缺ID
                reactivate_job_ids = []
                for job in jobs:
                    if job["jobNo"] in existing_job_dict:
                        job["last_update_date"] = today
                        # 如果職缺之前是下架狀態，現在又出現了，將其添加到重新激活列表
                        if existing_job_dict[job["jobNo"]] == "inactive":
                            job["status"] = "active"
                            reactivate_job_ids.append(job["jobNo"])
                    else:
                        job["last_update_date"] = today

                # 重新激活之前下架的職缺
                if reactivate_job_ids:
                    logger.info(f"重新激活 {len(reactivate_job_ids)} 筆之前下架的職缺")
                    mongo_manager.reactivate_jobs(reactivate_job_ids)
            else:
                # 如果資料庫中沒有現有職缺，為所有職缺添加last_update_date
                for job in jobs:
                    job["last_update_date"] = today

            logger.info(f"職缺上下架狀態處理完成")
        except Exception as e:
            logger.error(f"處理職缺上下架狀態時發生錯誤: {e}")
        finally:
            mongo_manager.close()

    def _merge_job_keywords(self, jobs: List[Dict]) -> List[Dict]:
        """
        合併相同職缺的多個關鍵字

        參數:
            jobs: 職缺數據列表

        返回:
            處理後的職缺列表，每個職缺只出現一次，但包含所有關鍵字
        """
        logger.info("開始處理相同職缺的多個關鍵字")

        if not jobs:
            return []

        # 按jobNo分組，合併相同職缺的search_keyword
        job_by_id_map = {}
        for job in jobs:
            job_id = job["jobNo"]
            if job_id in job_by_id_map:
                # 如果這個職缺已經存在，將關鍵字添加到列表中
                existing_keywords = job_by_id_map[job_id]["search_keyword"]
                new_keyword = job["search_keyword"]

                # 如果existing_keywords是字符串，轉換為列表
                if isinstance(existing_keywords, str):
                    existing_keywords = [existing_keywords]

                # 如果新關鍵字不在列表中，添加它
                if new_keyword not in existing_keywords:
                    existing_keywords.append(new_keyword)

                # 更新職缺的search_keyword字段
                job_by_id_map[job_id]["search_keyword"] = existing_keywords
            else:
                # 如果這是一個新職缺，將其添加到分組中
                # 將search_keyword轉換為列表
                if isinstance(job["search_keyword"], str):
                    job["search_keyword"] = [job["search_keyword"]]
                job_by_id_map[job_id] = job

        # 創建新的職缺列表，每個職缺只出現一次，但包含所有關鍵字
        deduplicated_jobs = list(job_by_id_map.values())
        logger.info(f"處理後的唯一職缺數量: {len(deduplicated_jobs)}")

        return deduplicated_jobs

    def _process_job_fields(self, jobs: List[Dict]) -> None:
        """
        處理職缺字段，包括link和地址

        參數:
            jobs: 職缺數據列表
        """
        if not jobs:
            return

        # 處理link字段，分割為applyAnalyze、job和cust
        logger.info("處理link字段，分割為applyAnalyze、job和cust")
        successfully_processed_links = 0
        for job in jobs:
            if "link" in job and job["link"]:
                try:
                    # 檢查link是否已經是字典格式
                    if isinstance(job["link"], dict):
                        # 如果已經是字典，直接提取值
                        apply_analyze_url = job["link"].get("applyAnalyze", "")
                        job_detail_url = job["link"].get("job", "")
                        company_url = job["link"].get("cust", "")

                        # 添加協議前綴
                        if apply_analyze_url and not apply_analyze_url.startswith(
                            ("http:", "https:")
                        ):
                            apply_analyze_url = "https:" + apply_analyze_url
                        if job_detail_url and not job_detail_url.startswith(
                            ("http:", "https:")
                        ):
                            job_detail_url = "https:" + job_detail_url
                        if company_url and not company_url.startswith(
                            ("http:", "https:")
                        ):
                            company_url = "https:" + company_url
                    else:
                        # 如果是字符串，使用原有的分割函數
                        apply_analyze_url, job_detail_url, company_url = (
                            split_link_field(job["link"])
                        )

                    job["applyAnalyze"] = apply_analyze_url
                    job["job"] = job_detail_url
                    job["cust"] = company_url
                    successfully_processed_links += 1
                except Exception as e:
                    logger.error(
                        f"處理職缺 {job.get('jobNo', 'unknown')} 的link字段時出錯: {e}, 原始字段: {job['link']}"
                    )
        logger.info(f"成功處理 {successfully_processed_links} 筆職缺的link字段")

        # 處理jobAddrNoDesc字段，分割為city和district
        logger.info("處理jobAddrNoDesc字段，分割為city和district")
        successfully_processed_addresses = 0
        for job in jobs:
            if "jobAddrNoDesc" in job and job["jobAddrNoDesc"]:
                try:
                    city, district = split_city_district(job["jobAddrNoDesc"])
                    job["city"] = city
                    job["district"] = district
                    successfully_processed_addresses += 1
                except Exception as e:
                    logger.error(
                        f"處理職缺 {job.get('jobNo', 'unknown')} 的jobAddrNoDesc字段時出錯: {e}"
                    )
        logger.info(
            f"成功處理 {successfully_processed_addresses} 筆職缺的jobAddrNoDesc字段"
        )

    async def process_jobs_data(self, all_jobs: List[Dict]) -> None:
        """
        處理爬取到的職缺數據，包括合併、狀態處理、字段處理和保存

        參數:


            all_jobs: 所有爬取到的職缺數據列表

        實作細節:
            - 檢查是否有數據需要處理
            - 合併相同職缺的多個關鍵字，避免重複
            - 處理職缺上下架狀態，追蹤職缺生命週期
            - 處理職缺字段，包括link解析和地址分割
            - 將處理後的數據保存為多種格式（CSV、JSON、資料庫）
            - 提供詳細的日誌記錄，便於監控和調試
            - 整個過程是流水線式的，每一步都依賴前一步的結果
        """
        # 檢查是否有數據需要處理
        # 如果沒有數據，記錄警告並直接返回
        if not all_jobs:
            logger.warning("沒有職缺數據需要處理")
            return

        # 記錄處理開始，顯示數據量
        logger.info(f"=== 開始處理 {len(all_jobs)} 筆職缺數據 ===")

        # 步驟1: 合併相同職缺的多個關鍵字
        # 這一步將相同職缺但來自不同關鍵字搜索的結果合併，避免重複
        logger.info("步驟1: 合併相同職缺的多個關鍵字")
        logger.info(f"合併前職缺數量: {len(all_jobs)}")
        deduplicated_jobs = self._merge_job_keywords(all_jobs)
        logger.info(f"合併後職缺數量: {len(deduplicated_jobs)}")
        logger.info(f"減少了 {len(all_jobs) - len(deduplicated_jobs)} 筆重複職缺")

        # 步驟2: 處理職缺上下架狀態
        # 這一步檢查職缺是否已存在於資料庫中，並更新其狀態（上架/下架）
        logger.info("步驟2: 處理職缺上下架狀態")
        # _process_job_status方法會記錄詳細的處理日誌
        self._process_job_status(deduplicated_jobs)

        # 步驟3: 處理職缺字段（link和地址）
        # 這一步解析職缺的link字段和地址字段，提取有用信息
        logger.info("步驟3: 處理職缺字段（link和地址）")
        # _process_job_fields方法會記錄詳細的處理日誌
        self._process_job_fields(deduplicated_jobs)

        # 步驟4: 保存數據為不同格式
        # 這一步將處理後的數據保存為CSV、JSON和資料庫格式
        logger.info("步驟4: 保存數據為不同格式")
        # _save_jobs_data方法會記錄詳細的保存日誌
        self._save_jobs_data(deduplicated_jobs)

        # 記錄處理完成
        logger.info(
            f"=== 職缺數據處理完成，最終處理了 {len(deduplicated_jobs)} 筆職缺 ==="
        )

    def _save_jobs_data(self, jobs: List[Dict]) -> None:
        """
        保存職缺數據為不同格式

        參數:
            jobs: 職缺數據列表
        """
        if not jobs:
            return

        current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 保存為CSV文件
        logger.info("保存職缺數據為CSV格式")
        csv_file_path = self.save_to_csv(jobs, f"104_jobs_all_{current_timestamp}.csv")
        logger.info(f"CSV文件已保存至: {csv_file_path}")

        # 保存為JSON文件
        logger.info("保存職缺數據為JSON格式")
        json_file_path = self.save_to_json(
            jobs, f"104_jobs_all_{current_timestamp}.json"
        )
        logger.info(f"JSON文件已保存至: {json_file_path}")

        # 保存到資料庫
        logger.info("保存職缺數據到資料庫（結構化模式）")
        inserted_records_count = self.save_to_database(jobs)
        logger.info(f"成功將 {inserted_records_count} 筆數據保存到資料庫")

    def save_to_csv(self, jobs: List[Dict], filename: Optional[str] = None) -> str:
        """
        將職缺數據保存為CSV文件

        參數:
            jobs: 職缺數據列表
            filename: 文件名，默認為None（自動生成）

        返回:
            str: 保存文件的路徑

        實作細節:
            - 檢查是否有數據需要保存
            - 如未指定文件名，則自動生成包含時間戳的文件名
            - 將職缺數據轉換為DataFrame
            - 保存為CSV格式，使用utf-8-sig編碼支持中文
            - 返回保存文件的完整路徑
        """
        # 檢查是否有數據需要保存
        if not jobs:
            logger.warning("沒有職缺數據可保存")
            return ""

        # 如未指定文件名，則自動生成
        if not filename:
            current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"104_jobs_{current_timestamp}.csv"

        # 構建完整的文件路徑
        csv_file_path = self.output_dir / filename

        # 將職缺數據轉換為DataFrame
        jobs_dataframe = pd.DataFrame(jobs)

        # 保存為CSV文件，使用utf-8-sig編碼以支持中文
        jobs_dataframe.to_csv(csv_file_path, index=False, encoding="utf-8-sig")
        logger.info(f"已將 {len(jobs)} 筆職缺數據保存至 {str(csv_file_path)}")

        return str(csv_file_path)

    def save_to_json(self, jobs: List[Dict], filename: Optional[str] = None) -> str:
        """
        將職缺數據保存為JSON文件

        參數:
            jobs: 職缺數據列表
            filename: 文件名，默認為None（自動生成）

        返回:
            str: 保存文件的路徑

        實作細節:
            - 檢查是否有數據需要保存
            - 如未指定文件名，則自動生成包含時間戳的文件名
            - 使用json.dump保存數據，設置ensure_ascii=False以正確處理中文
            - 設置縮進以提高JSON文件的可讀性
            - 返回保存文件的完整路徑
        """
        # 檢查是否有數據需要保存
        if not jobs:
            logger.warning("沒有職缺數據可保存")
            return ""

        # 如未指定文件名，則自動生成
        if not filename:
            current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"104_jobs_{current_timestamp}.json"

        # 構建完整的文件路徑
        json_file_path = self.output_dir / filename

        # 保存為JSON文件
        with open(json_file_path, "w", encoding="utf-8") as f:
            # ensure_ascii=False確保中文字符正確保存，不會被轉換為Unicode轉義序列
            # indent=2設置縮進，使JSON文件更易讀
            json.dump(jobs, f, ensure_ascii=False, indent=2)
        logger.info(f"已將 {len(jobs)} 筆職缺數據保存至 {str(json_file_path)}")

        return str(json_file_path)

    def save_to_database(self, jobs: List[Dict]) -> int:
        """
        將職缺數據保存到資料庫

        參數:
            jobs: 職缺數據列表

        返回:
            int: 插入到資料庫的職缺數量

        實作細節:
            - 檢查是否有數據需要保存
            - 初始化 MongoDB 管理器
            - 使用 MongoDB 管理器的 insert_jobs 方法保存數據
            - 確保資料庫連接正確關閉
            - 返回成功插入的記錄數量

        注意:
            - 此方法會更新已存在職缺的所有欄位，包括狀態（status）、最後更新日期（last_update_date）和下架日期（delisted_date）
            - 職缺狀態可以是 'active'（上架）或 'inactive'（下架）
            - 當職缺狀態從 'active' 變為 'inactive' 時，會記錄下架日期（delisted_date）
            - 當職缺狀態從 'inactive' 變為 'active' 時，會清除下架日期（delisted_date）
        """
        # 檢查是否有數據需要保存
        if not jobs:
            logger.warning("沒有職缺數據可保存到資料庫")
            return 0

        # 初始化 MongoDB 管理器
        mongo_manager = MongoDBManager()

        try:
            # 使用 MongoDB 管理器的 insert_jobs 方法保存數據
            inserted_records_count = mongo_manager.insert_jobs(jobs)
            return inserted_records_count
        finally:
            # 確保資料庫連接正確關閉
            mongo_manager.close()
            logger.debug("MongoDB 連接已關閉")

    def run(self, keywords: List[str]) -> List[Dict]:
        """
        執行爬蟲的主入口方法

        參數:
            keywords: 要搜索的關鍵字列表

        返回:
            List[Dict]: 所有爬取到的職缺數據

        實作細節:
            - 使用asyncio.run啟動非同步爬蟲主函數
            - 將關鍵字列表傳遞給main方法進行處理
            - 處理整個爬蟲流程，包括搜索、爬取和數據處理
            - 返回處理後的職缺數據列表
            - 這是同步代碼與非同步代碼的橋接點
        """
        # 此方法是爬蟲的主入口點，由外部代碼調用
        # 例如：crawler.run(keywords=['Python', 'Java'])
        logger.info(f"開始執行爬蟲，搜索關鍵字: {keywords}")

        # 使用asyncio.run啟動非同步爬蟲主函數
        # asyncio.run會創建一個新的事件循環，運行main協程，並在完成後關閉循環
        crawled_jobs = asyncio.run(self.main(keywords))

        logger.info(f"爬蟲執行完成，共獲取 {len(crawled_jobs)} 筆職缺數據")
        return crawled_jobs


if __name__ == "__main__":
    crawler = Crawler()
    crawler.run(
        keywords=[
            "Python",
            "django",
            "fastapi",
            "flask",
            "DevOps",
            "SRE",
            "K8S",
            "JAVA",
        ]
    )
