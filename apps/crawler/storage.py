"""
職缺資料儲存模組

此模組提供了一個抽象的資料儲存介面和具體實現，
遵循開放/封閉原則 (OCP)，使系統可以輕鬆擴展支援不同的儲存後端。
"""

import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd

from config.settings import logger, BASE_DIR
from src.database.mongodb_manager import MongoDBManager
from apps.crawler.constants import (
    JOB_STATUS_ACTIVE,
    JOB_STATUS_INACTIVE,
    MONGODB_COLLECTION_JOBS,
    MONGODB_COLLECTION_DAILY,
    CSV_ENCODING,
    JSON_INDENT,
)


class JobStorage(ABC):
    """
    職缺資料儲存抽象基類

    定義了儲存和檢索職缺資料的標準介面。
    所有具體的儲存實現都應該繼承此類並實現其抽象方法。
    """

    @abstractmethod
    def save_jobs(self, jobs: List[Dict]) -> int:
        """
        儲存職缺資料

        參數:
            jobs: 職缺資料列表

        返回:
            int: 成功儲存的記錄數量
        """
        pass

    @abstractmethod
    def get_jobs(self, filters: Optional[Dict] = None, limit: int = 1000) -> List[Dict]:
        """
        檢索職缺資料

        參數:
            filters: 過濾條件
            limit: 最大返回記錄數

        返回:
            List[Dict]: 職缺資料列表
        """
        pass

    @abstractmethod
    def update_job_status(self, current_job_ids: List[str]) -> int:
        """
        更新職缺狀態

        參數:
            current_job_ids: 當前爬取到的所有職缺ID列表

        返回:
            int: 更新的記錄數量
        """
        pass

    @abstractmethod
    def get_existing_jobs(self) -> Dict[str, str]:
        """
        獲取現有職缺ID和狀態

        返回:
            Dict[str, str]: 職缺ID到狀態的映射字典
        """
        pass

    @abstractmethod
    def reactivate_jobs(self, job_ids: List[str]) -> int:
        """
        重新激活下架的職缺

        參數:
            job_ids: 需要重新激活的職缺ID列表

        返回:
            int: 更新的記錄數量
        """
        pass

    @abstractmethod
    def save_search_result(self, result: Dict) -> bool:
        """
        儲存搜索結果

        參數:
            result: 搜索結果字典

        返回:
            bool: 是否成功儲存
        """
        pass

    @abstractmethod
    def get_search_result(self, filters: Dict) -> Optional[Dict]:
        """
        檢索搜索結果

        參數:
            filters: 過濾條件

        返回:
            Optional[Dict]: 搜索結果字典，如果不存在則返回None
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        關閉儲存連接
        """
        pass


class MongoDBJobStorage(JobStorage):
    """
    MongoDB職缺資料儲存實現

    使用MongoDB作為後端儲存職缺資料。
    """

    def __init__(
        self, connection_string: Optional[str] = None, db_name: Optional[str] = None
    ):
        """
        初始化MongoDB儲存

        參數:
            connection_string: MongoDB連接字符串，默認從配置中獲取
            db_name: 數據庫名稱，默認從配置中獲取
        """
        self.mongo_manager = MongoDBManager(connection_string, db_name)
        logger.info("MongoDB職缺資料儲存已初始化")

    def save_jobs(self, jobs: List[Dict]) -> int:
        """
        將職缺資料儲存到MongoDB

        參數:
            jobs: 職缺資料列表

        返回:
            int: 成功儲存的記錄數量
        """
        if not jobs:
            logger.warning("沒有職缺資料可儲存")
            return 0

        # 使用MongoDB管理器的insert_jobs方法儲存資料
        return self.mongo_manager.insert_jobs(jobs)

    def get_jobs(self, filters: Optional[Dict] = None, limit: int = 1000) -> List[Dict]:
        """
        從MongoDB檢索職缺資料

        參數:
            filters: 過濾條件
            limit: 最大返回記錄數

        返回:
            List[Dict]: 職缺資料列表
        """
        return self.mongo_manager.get_jobs(filters, limit)

    def update_job_status(self, current_job_ids: List[str]) -> int:
        """
        更新職缺狀態

        參數:
            current_job_ids: 當前爬取到的所有職缺ID列表

        返回:
            int: 更新的記錄數量
        """
        return self.mongo_manager.update_job_status(current_job_ids)

    def get_existing_jobs(self) -> Dict[str, str]:
        """
        獲取現有職缺ID和狀態

        返回:
            Dict[str, str]: 職缺ID到狀態的映射字典
        """
        return self.mongo_manager.get_existing_jobs()

    def reactivate_jobs(self, job_ids: List[str]) -> int:
        """
        重新激活下架的職缺

        參數:
            job_ids: 需要重新激活的職缺ID列表

        返回:
            int: 更新的記錄數量
        """
        return self.mongo_manager.reactivate_jobs(job_ids)

    def save_search_result(self, result: Dict) -> bool:
        """
        儲存搜索結果到MongoDB

        參數:
            result: 搜索結果字典

        返回:
            bool: 是否成功儲存
        """
        try:
            # 使用upsert=True確保不會重複插入相同的記錄
            self.mongo_manager.db[MONGODB_COLLECTION_DAILY].update_one(
                {
                    "today": result["today"],
                    "keyword": result.get("keyword", ""),
                    "first_url": result["first_url"],
                    "crawl_url": result["crawl_url"],
                },
                {"$set": result},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error(f"儲存搜索結果時發生錯誤: {e}")
            return False

    def get_search_result(self, filters: Dict) -> Optional[Dict]:
        """
        從MongoDB檢索搜索結果

        參數:
            filters: 過濾條件

        返回:
            Optional[Dict]: 搜索結果字典，如果不存在則返回None
        """
        try:
            result = self.mongo_manager.db[MONGODB_COLLECTION_DAILY].find_one(filters)
            return result
        except Exception as e:
            logger.error(f"檢索搜索結果時發生錯誤: {e}")
            return None

    def close(self) -> None:
        """
        關閉MongoDB連接
        """
        self.mongo_manager.close()
        logger.debug("MongoDB連接已關閉")


class FileJobStorage(JobStorage):
    """
    檔案系統職缺資料儲存實現

    使用檔案系統（CSV和JSON）作為後端儲存職缺資料。
    主要用於備份和離線分析。
    """

    def __init__(self, output_dir: Union[str, Path] = None):
        """
        初始化檔案儲存

        參數:
            output_dir: 輸出目錄，默認為None（使用預設目錄）
        """
        if output_dir:
            self.output_dir = (
                Path(output_dir) if isinstance(output_dir, str) else output_dir
            )
        else:
            self.output_dir = BASE_DIR / "data" / "raw_data"

        # 確保輸出目錄存在
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 用於記憶體中暫存的職缺資料
        self._jobs_cache = {}
        self._search_results_cache = {}

        logger.info(f"檔案系統職缺資料儲存已初始化，輸出目錄: {self.output_dir}")

    def save_jobs(self, jobs: List[Dict]) -> int:
        """
        將職缺資料儲存到檔案系統（CSV和JSON）

        參數:
            jobs: 職缺資料列表

        返回:
            int: 成功儲存的記錄數量
        """
        if not jobs:
            logger.warning("沒有職缺資料可儲存")
            return 0

        # 生成檔案名稱
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"104_jobs_{timestamp}.csv"
        json_filename = f"104_jobs_{timestamp}.json"

        # 儲存為CSV
        csv_path = self.output_dir / csv_filename
        pd.DataFrame(jobs).to_csv(csv_path, index=False, encoding=CSV_ENCODING)

        # 儲存為JSON
        json_path = self.output_dir / json_filename
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(jobs, f, ensure_ascii=False, indent=JSON_INDENT)

        # 更新記憶體快取
        for job in jobs:
            if "jobNo" in job:
                self._jobs_cache[job["jobNo"]] = job

        logger.info(f"已將 {len(jobs)} 筆職缺資料儲存至檔案系統")
        logger.info(f"CSV檔案: {csv_path}")
        logger.info(f"JSON檔案: {json_path}")

        return len(jobs)

    def get_jobs(self, filters: Optional[Dict] = None, limit: int = 1000) -> List[Dict]:
        """
        從記憶體快取中檢索職缺資料

        參數:
            filters: 過濾條件
            limit: 最大返回記錄數

        返回:
            List[Dict]: 職缺資料列表
        """
        # 簡單實現，僅支援jobNo過濾
        if filters and "jobNo" in filters:
            job_no = filters["jobNo"]
            if job_no in self._jobs_cache:
                return [self._jobs_cache[job_no]]
            return []

        # 返回所有職缺，受limit限制
        return list(self._jobs_cache.values())[:limit]

    def update_job_status(self, current_job_ids: List[str]) -> int:
        """
        更新職缺狀態

        參數:
            current_job_ids: 當前爬取到的所有職缺ID列表

        返回:
            int: 更新的記錄數量
        """
        if not current_job_ids or not self._jobs_cache:
            return 0

        today = datetime.now().strftime("%Y-%m-%d")
        updated_count = 0

        # 將不在current_job_ids中的職缺標記為下架
        for job_id, job in self._jobs_cache.items():
            if job_id not in current_job_ids and job.get("status") == JOB_STATUS_ACTIVE:
                job["status"] = JOB_STATUS_INACTIVE
                job["last_update_date"] = today
                job["delisted_date"] = today
                updated_count += 1

        logger.info(f"檔案儲存: 更新了 {updated_count} 筆職缺狀態為下架")
        return updated_count

    def get_existing_jobs(self) -> Dict[str, str]:
        """
        獲取現有職缺ID和狀態

        返回:
            Dict[str, str]: 職缺ID到狀態的映射字典
        """
        # 從記憶體快取中提取職缺ID和狀態
        result = {}
        for job_id, job in self._jobs_cache.items():
            result[job_id] = job.get("status", JOB_STATUS_ACTIVE)

        return result

    def reactivate_jobs(self, job_ids: List[str]) -> int:
        """
        重新激活下架的職缺

        參數:
            job_ids: 需要重新激活的職缺ID列表

        返回:
            int: 更新的記錄數量
        """
        if not job_ids:
            return 0

        today = datetime.now().strftime("%Y-%m-%d")
        reactivated_count = 0

        # 重新激活指定的職缺
        for job_id in job_ids:
            if (
                job_id in self._jobs_cache
                and self._jobs_cache[job_id].get("status") == JOB_STATUS_INACTIVE
            ):
                self._jobs_cache[job_id]["status"] = JOB_STATUS_ACTIVE
                self._jobs_cache[job_id]["last_update_date"] = today
                if "delisted_date" in self._jobs_cache[job_id]:
                    del self._jobs_cache[job_id]["delisted_date"]
                reactivated_count += 1

        logger.info(f"檔案儲存: 已將 {reactivated_count} 筆職缺狀態更新為上架")
        return reactivated_count

    def save_search_result(self, result: Dict) -> bool:
        """
        儲存搜索結果到記憶體快取

        參數:
            result: 搜索結果字典

        返回:
            bool: 是否成功儲存
        """
        try:
            # 生成唯一鍵
            key = (
                result["today"],
                result.get("keyword", ""),
                result["first_url"],
                result["crawl_url"],
            )

            # 儲存到記憶體快取
            self._search_results_cache[key] = result
            return True
        except Exception as e:
            logger.error(f"儲存搜索結果時發生錯誤: {e}")
            return False

    def get_search_result(self, filters: Dict) -> Optional[Dict]:
        """
        從記憶體快取中檢索搜索結果

        參數:
            filters: 過濾條件

        返回:
            Optional[Dict]: 搜索結果字典，如果不存在則返回None
        """
        try:
            # 嘗試構建鍵
            key = (
                filters.get("today", ""),
                filters.get("keyword", ""),
                filters.get("first_url", ""),
                filters.get("crawl_url", ""),
            )

            # 從記憶體快取中檢索
            return self._search_results_cache.get(key)
        except Exception as e:
            logger.error(f"檢索搜索結果時發生錯誤: {e}")
            return None

    def close(self) -> None:
        """
        關閉檔案儲存（無需特殊操作）
        """
        logger.debug("檔案儲存已關閉")
