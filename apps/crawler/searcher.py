"""
職缺搜索模組

此模組負責搜索104人力銀行網站上的職缺，包括構建搜索URL、發送請求和處理響應。
遵循單一職責原則 (SRP)，將搜索邏輯與爬蟲邏輯分離。
"""

import asyncio
import itertools
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Any
from urllib.parse import urlencode

from config.settings import logger
from src.utils.http_adapter import AsyncHttpAdapter
from src.utils.text_processing import extract_lowest_level_area_codes
from apps.crawler.constants import (
    SEARCH_API_URL,
    MAX_RETRIES,
    MIN_DELAY,
    MAX_DELAY,
    USER_AGENTS,
    DEFAULT_HEADERS,
    MAX_CONCURRENCY,
    TARGET_CITIES,
)
from apps.crawler.storage import JobStorage


class JobSearcher:
    """
    職缺搜索器

    負責搜索104人力銀行網站上的職缺，包括：
    1. 構建搜索URL
    2. 發送搜索請求
    3. 處理搜索響應
    4. 獲取所有頁面的URL
    """

    def __init__(self, storage: JobStorage):
        """
        初始化職缺搜索器

        參數:
            storage: 職缺資料儲存器
        """
        logger.info("初始化職缺搜索器")

        # 初始化HTTP請求適配器
        self.http_adapter = AsyncHttpAdapter(
            max_retries=MAX_RETRIES,
            min_retry_delay=MIN_DELAY,
            max_retry_delay=MAX_DELAY,
            user_agents=USER_AGENTS,
            rotate_user_agent=True,
            headers=DEFAULT_HEADERS,
        )

        # 設定儲存器
        self.storage = storage

        # 設定併發控制
        self.max_concurrency = MAX_CONCURRENCY
        self.semaphore = asyncio.Semaphore(self.max_concurrency)

        # 設定當前日期
        self.today = datetime.now().strftime("%Y-%m-%d")

    def build_url(
        self,
        keyword: str = "",
        page: int = 1,
        area: str = "",
        industry: str = "",
        job_category: str = "",
        experience: str = "",
        education: str = "",
    ) -> str:
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
            str: 完整的搜索URL
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

        # 構建完整URL
        complete_url = f"{SEARCH_API_URL}?{urlencode(params)}"
        logger.debug(f"構建搜索URL: {complete_url}")

        return complete_url

    async def search_jobs(self, url: str) -> Dict:
        """
        發送搜索請求並處理響應

        參數:
            url: 完整的搜索URL

        返回:
            Dict: 搜索結果字典
        """
        logger.debug(f"開始發送搜索請求: {url}")

        # 發送HTTP請求
        response_data = await self.http_adapter.get(url=url)

        # 檢查是否有錯誤
        if "error" in response_data:
            logger.warning(f"搜索請求返回錯誤: {response_data['error']}, URL: {url}")
            return response_data

        # 檢查響應是否包含預期的數據結構
        if "data" not in response_data:
            error_msg = "響應格式不正確，未找到'data'字段"
            logger.warning(f"{error_msg}, URL: {url}")
            return {"error": error_msg}

        # 記錄搜索結果統計信息
        if "totalPage" in response_data["data"]:
            total_pages = response_data["data"]["totalPage"]
            logger.debug(f"總頁數: {total_pages}, URL: {url}")

        if "totalCount" in response_data["data"]:
            total_jobs_count = response_data["data"]["totalCount"]
            logger.debug(f"總筆數: {total_jobs_count}, URL: {url}")

            if total_jobs_count > 0:
                logger.info(f"搜索結果: 找到 {total_jobs_count} 筆職缺, URL: {url}")
            else:
                logger.info(f"搜索結果: 未找到職缺, URL: {url}")

        # 檢查是否有職缺列表
        if "list" in response_data["data"]:
            page_jobs_count = len(response_data["data"]["list"])
            logger.debug(f"本頁職缺數: {page_jobs_count}, URL: {url}")
        else:
            logger.warning(f"響應中未找到'list'字段，可能是API結構變更, URL: {url}")

        return response_data

    def get_taiwan_area_codes(self) -> Dict[str, str]:
        """
        獲取台灣地區的所有最下層地區代碼

        返回:
            Dict[str, str]: 地區代碼與地區名稱的映射字典
        """
        # 使用台灣地區下的所有最下層地區代碼
        area_code_map = extract_lowest_level_area_codes()
        logger.info(
            f"從 area_codes.json 提取了 {len(area_code_map)} 個台灣地區下的最下層地區代碼"
        )

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
        """
        # 構建第一頁的URL
        first_page_url = self.build_url(keyword=keyword, area=area_code, page=1)

        # 檢查資料庫中是否已有今天的搜索結果
        cached_result = self.storage.get_search_result(
            {"today": self.today, "first_url": first_page_url}
        )

        if cached_result and cached_result.get("status") == "ok":
            logger.info(f"使用緩存的結果，跳過爬取: 關鍵字={keyword}, 地區={area_name}")
            return cached_result

        # 使用信號量控制併發請求數量
        async with self.semaphore:
            try:
                # 發送請求獲取第一頁搜索結果
                logger.info(f"開始爬取第一頁: 關鍵字={keyword}, 地區={area_name}")
                all_page_urls = []  # 用於存儲所有頁面的URL

                # 發送請求獲取第一頁結果
                first_page_response = await self.search_jobs(url=first_page_url)

                # 從第一頁結果中獲取總頁數，然後生成所有頁面的URL
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

                # 將結果封裝為標準格式
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

                # 保存搜索結果到儲存器
                self.storage.save_search_result(formatted_result)

                return formatted_result
            except Exception as e:
                # 記錄爬取失敗的錯誤
                logger.error(
                    f"爬取第一頁失敗: 關鍵字={keyword}, 地區={area_name}, 錯誤: {str(e)}"
                )

                # 即使失敗也返回標準格式的結果字典，只是狀態為"error"
                error_result = {
                    "today": self.today,
                    "first_url": first_page_url,
                    "crawl_url": first_page_url,
                    "keyword": keyword,
                    "area_code": area_code,
                    "area_name": area_name,
                    "status": "error",
                    "result": "",
                }

                # 保存錯誤結果到儲存器
                self.storage.save_search_result(error_result)

                return error_result

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
        """
        # 創建任務列表，用於並行執行
        first_page_tasks = []

        # 記錄開始搜索的關鍵字和地區數量
        logger.info(f"開始搜索關鍵字 '{keyword}' 在 {len(area_code_map)} 個地區的職缺")

        # 為每個地區創建一個任務，獲取第一頁結果
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

        # 分離成功和失敗的結果
        successful_results = [
            result
            for result in first_page_results
            if isinstance(result, dict) and result.get("status") == "ok"
        ]
        failed_results = [
            result
            for result in first_page_results
            if not isinstance(result, dict) or result.get("status") != "ok"
        ]

        # 記錄成功和失敗的數量
        logger.info(
            f"關鍵字 '{keyword}' 搜索結果: 成功 {len(successful_results)} 個, 失敗 {len(failed_results)} 個"
        )

        # 從成功結果中提取所有頁面的URL列表
        # 使用itertools.chain.from_iterable扁平化嵌套列表
        all_page_urls = list(
            itertools.chain.from_iterable(
                [
                    result["result"]
                    for result in successful_results
                    if result and "result" in result
                ]
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
            Dict: 搜索結果字典
        """
        # 檢查資料庫中是否已有今天的搜索結果
        cached_result = self.storage.get_search_result(
            {"today": self.today, "first_url": "", "crawl_url": url}
        )

        if cached_result and cached_result.get("status") == "ok":
            logger.info(f"使用緩存的結果，跳過爬取: {url}")
            return cached_result

        # 使用信號量控制併發請求數量
        async with self.semaphore:
            try:
                # 發送請求獲取搜索結果
                search_result = await self.search_jobs(url)

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

                # 保存搜索結果到儲存器
                self.storage.save_search_result(formatted_response)

                # 記錄成功爬取
                logger.info(f"成功爬取URL: {url}")
                return formatted_response
            except Exception as e:
                # 記錄爬取失敗的錯誤
                logger.error(f"爬取URL失敗: {url}, 錯誤: {str(e)}")

                # 即使失敗也返回標準格式的結果字典，只是狀態為"error"
                error_result = {
                    "today": self.today,
                    "url": url,
                    "first_url": "",
                    "crawl_url": url,
                    "keyword": keyword,
                    "status": "error",
                    "result": "",
                }

                # 保存錯誤結果到儲存器
                self.storage.save_search_result(error_result)

                return error_result

    async def search_all_keywords(self, keywords: List[str]) -> Dict[str, List[str]]:
        """
        搜索所有關鍵字的職缺

        參數:
            keywords: 關鍵字列表

        返回:
            Dict[str, List[str]]: 關鍵字到頁面URL列表的映射
        """
        # 獲取台灣地區代碼
        area_code_map = self.get_taiwan_area_codes()

        # 創建一個字典，用於存儲每個關鍵字對應的URL列表
        keyword_to_urls_mapping = {}

        # 為每個關鍵字在所有地區進行搜索
        for keyword in keywords:
            logger.info(f"處理關鍵字: {keyword}")
            page_urls = await self.search_keyword(keyword, area_code_map)
            keyword_to_urls_mapping[keyword] = page_urls
            logger.info(f"關鍵字 '{keyword}' 獲取到 {len(page_urls)} 個頁面URL")

        return keyword_to_urls_mapping

    async def fetch_all_pages(
        self, keyword_to_urls_mapping: Dict[str, List[str]]
    ) -> List[Dict]:
        """
        爬取所有頁面的職缺數據

        參數:
            keyword_to_urls_mapping: 關鍵字到頁面URL列表的映射

        返回:
            List[Dict]: 所有頁面的搜索結果
        """
        # 創建任務列表，準備並行爬取所有URL的職缺數據
        search_tasks = []
        for keyword, url_list in keyword_to_urls_mapping.items():
            for page_url in url_list:
                search_tasks.append(self.search_with_semaphore(keyword, page_url))

        logger.info(f"共創建 {len(search_tasks)} 個爬取任務")

        # 並行執行所有任務
        logger.info("開始並行執行爬取任務")
        search_results = await asyncio.gather(*search_tasks, return_exceptions=True)
        logger.info(f"所有任務執行完成，獲取 {len(search_results)} 個結果")

        # 分離成功和失敗的結果
        successful_results = [
            result
            for result in search_results
            if isinstance(result, dict) and result.get("status") == "ok"
        ]
        failed_count = sum(
            1
            for result in search_results
            if not isinstance(result, dict) or result.get("status") != "ok"
        )

        logger.info(f"成功: {len(successful_results)}個, 失敗: {failed_count}個")

        return successful_results

    def extract_jobs_from_results(
        self, search_results: List[Dict], keyword: str = ""
    ) -> List[Dict]:
        """
        從搜索結果中提取職缺數據

        參數:
            search_results: 搜索結果列表
            keyword: 搜索關鍵字，用於添加到職缺數據中

        返回:
            List[Dict]: 職缺數據列表
        """
        all_jobs = []

        for result in search_results:
            # 安全地訪問嵌套字典結構
            response_data = result.get("result", {})
            data = response_data.get("data", {})
            jobs_in_result = data.get("list", [])

            # 只有當jobs_in_result不為空時才處理
            if jobs_in_result:
                # 如果提供了關鍵字，添加到每個職缺中
                if keyword:
                    for job in jobs_in_result:
                        job["search_keyword"] = keyword

                all_jobs.extend(jobs_in_result)

        logger.info(f"從搜索結果中提取了 {len(all_jobs)} 筆職缺數據")

        return all_jobs
