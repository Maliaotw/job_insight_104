"""
爬蟲協調器模組

此模組負責協調整個爬蟲流程，包括搜索、處理和儲存職缺資料。
遵循開放/封閉原則 (OCP)，使系統可以輕鬆擴展支援不同的搜索和儲存策略。
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional

from apps.crawler.processor import JobDataProcessor
from apps.crawler.searcher import JobSearcher
from apps.crawler.storage import FileJobStorage, JobStorage, MongoDBJobStorage
from config.settings import logger


class CrawlerOrchestrator:
    """
    爬蟲協調器

    負責協調整個爬蟲流程，包括：
    1. 初始化搜索器、處理器和儲存器
    2. 執行搜索流程
    3. 處理搜索結果
    4. 儲存處理後的職缺資料
    """

    def __init__(
        self, storage: Optional[JobStorage] = None, output_dir: Optional[str] = None
    ):
        """
        初始化爬蟲協調器

        參數:
            storage: 職缺資料儲存器，如果為None則使用MongoDB儲存
            output_dir: 輸出目錄，用於檔案儲存
        """
        logger.info("初始化爬蟲協調器")

        # 初始化儲存器
        self.storage = storage or MongoDBJobStorage()

        # 如果提供了輸出目錄，也初始化檔案儲存器
        self.file_storage = None
        if output_dir:
            self.file_storage = FileJobStorage(output_dir)

        # 初始化搜索器和處理器
        self.searcher = JobSearcher(self.storage)
        self.processor = JobDataProcessor()

        # 設定當前時間戳，用於生成檔案名稱
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    async def crawl(self, keywords: List[str]) -> List[Dict]:
        """
        執行爬蟲的主流程

        參數:
            keywords: 要搜索的關鍵字列表

        返回:
            List[Dict]: 處理後的職缺資料列表
        """
        logger.info(f"開始執行爬蟲，搜索關鍵字: {keywords}")

        try:
            # 步驟1: 搜索所有關鍵字的職缺
            keyword_to_urls_mapping = await self.searcher.search_all_keywords(keywords)

            # 步驟2: 爬取所有頁面的職缺數據
            search_results = await self.searcher.fetch_all_pages(
                keyword_to_urls_mapping
            )

            # 步驟3: 從搜索結果中提取職缺數據
            all_jobs = []
            for keyword, urls in keyword_to_urls_mapping.items():
                # 過濾出屬於當前關鍵字的搜索結果
                keyword_results = [
                    result
                    for result in search_results
                    if result.get("keyword") == keyword
                    or any(url in result.get("url", "") for url in urls)
                ]

                # 提取職缺數據並添加關鍵字
                jobs = self.searcher.extract_jobs_from_results(keyword_results, keyword)

                # 添加元數據
                self.processor.add_metadata(jobs, keyword)

                # 添加到總職缺列表
                all_jobs.extend(jobs)

            # 步驟4: 獲取現有職缺ID和狀態
            existing_jobs = self.storage.get_existing_jobs()

            # 步驟5: 處理職缺數據
            processed_jobs = self.processor.process_jobs(all_jobs, existing_jobs)

            # 步驟6: 儲存處理後的職缺數據
            # 儲存到主儲存器
            saved_count = self.storage.save_jobs(processed_jobs)
            logger.info(f"已將 {saved_count} 筆職缺資料儲存到主儲存器")

            # 如果有檔案儲存器，也儲存到檔案
            if self.file_storage:
                file_saved_count = self.file_storage.save_jobs(processed_jobs)
                logger.info(f"已將 {file_saved_count} 筆職缺資料儲存到檔案")

            logger.info(f"爬蟲執行完成，共處理 {len(processed_jobs)} 筆職缺資料")
            return processed_jobs

        except Exception as e:
            logger.error(f"爬蟲執行過程中發生錯誤: {e}")
            raise
        finally:
            # 確保資源正確關閉
            self.close()

    def run(self, keywords: List[str]) -> List[Dict]:
        """
        執行爬蟲的同步入口方法

        參數:
            keywords: 要搜索的關鍵字列表

        返回:
            List[Dict]: 處理後的職缺資料列表
        """
        # 使用asyncio.run啟動非同步爬蟲主函數
        return asyncio.run(self.crawl(keywords))

    def close(self) -> None:
        """
        關閉爬蟲協調器，釋放資源
        """
        # 關閉儲存器
        if self.storage:
            self.storage.close()

        # 關閉檔案儲存器
        if self.file_storage:
            self.file_storage.close()

        logger.info("爬蟲協調器已關閉")


# 使用範例
if __name__ == "__main__":
    # 創建爬蟲協調器
    orchestrator = CrawlerOrchestrator()

    # 執行爬蟲
    jobs = orchestrator.run(keywords=["Python", "Java", "DevOps"])

    # 輸出結果
    print(f"共爬取到 {len(jobs)} 筆職缺資料")
