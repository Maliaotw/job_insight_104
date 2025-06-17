"""
MongoDB 數據管理器：用於存儲和檢索爬蟲數據
"""

import json
from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database

from config.settings import (
    BASE_DIR, 
    logger, 
    MONGODB_CONNECTION_STRING,
    MONGODB_DB_NAME,
    MONGODB_AUTH_SOURCE
)


class MongoDBManager:
    """
    MongoDB 數據管理器類：用於管理與 MongoDB 的連接和操作
    """

    def __init__(self, connection_string: Optional[str] = None, db_name: Optional[str] = None, auth_source: Optional[str] = None):
        """
        初始化 MongoDB 管理器

        參數:
            connection_string: MongoDB 連接字符串，默認從配置中獲取
            db_name: 數據庫名稱，默認從配置中獲取
            auth_source: 身份驗證數據庫，默認從配置中獲取
        """
        # 從配置中獲取連接信息
        self.connection_string = connection_string or MONGODB_CONNECTION_STRING
        self.db_name = db_name or MONGODB_DB_NAME
        self.auth_source = auth_source or MONGODB_AUTH_SOURCE

        # 初始化連接
        logger.info(f"連接到 MongoDB: {self.connection_string}, 數據庫: {self.db_name}, 身份驗證數據庫: {self.auth_source}")

        try:
            # 創建客戶端連接，指定身份驗證數據庫
            self.client = MongoClient(self.connection_string, authSource=self.auth_source)

            # 測試連接是否成功
            # 執行一個簡單的命令來驗證連接和身份驗證
            self.client.admin.command('ping')

            self.db = self.client[self.db_name]

            # 確保索引存在
            self._ensure_indexes()
            self._daily_indexes()

            logger.info("MongoDB 管理器初始化完成")
        except Exception as e:
            error_msg = str(e)
            logger.error(f"MongoDB 連接失敗: {error_msg}")

            # 檢查是否是身份驗證錯誤
            if "Authentication failed" in error_msg:
                logger.warning("嘗試使用無身份驗證方式連接...")
                try:
                    # 嘗試無身份驗證連接
                    no_auth_connection = f"mongodb://localhost:27017/"
                    self.client = MongoClient(no_auth_connection)
                    self.client.admin.command('ping')
                    self.db = self.client[self.db_name]
                    self._ensure_indexes()
                    logger.info("使用無身份驗證方式連接成功")
                    return
                except Exception as no_auth_error:
                    logger.error(f"無身份驗證連接也失敗: {str(no_auth_error)}")

            # 提供詳細的錯誤信息和解決方案
            logger.error("請檢查以下可能的問題:")
            logger.error("1. 確保MongoDB服務正在運行")
            logger.error("2. 檢查連接字符串中的用戶名和密碼是否正確")
            logger.error("3. 確認身份驗證數據庫設置是否正確（通常為'admin'）")
            logger.error("4. 確認MongoDB用戶是否有權限訪問指定的數據庫")
            logger.error("5. 創建.env文件並設置正確的MongoDB連接信息")

            # 重新拋出異常，讓調用者知道初始化失敗
            raise


    def _daily_indexes(self):
        self.db.daily.create_index(
            [('today', 1), ('crawl_url', 1), ('first_url', 1), ('keyword', 1)],
            unique=True
        )
        logger.info("MongoDB _daily_indexes 索引檢查完成")

    def _ensure_indexes(self):
        """確保必要的索引存在"""
        # 在 jobs 集合上創建 jobNo 索引
        self.db.jobs.create_index("jobNo", unique=True)
        # 在 jobs 集合上創建爬取日期索引
        self.db.jobs.create_index("crawl_date")
        # 在 jobs 集合上創建狀態索引
        self.db.jobs.create_index("status")

        logger.info("MongoDB _ensure_indexes 索引檢查完成")

    def list_indexes(self,collection):
        return list(self.db[collection].list_indexes())

    def insert_jobs(self, jobs: List[Dict]) -> int:
        """
        將職缺數據保存到 MongoDB

        參數:
            jobs: 職缺數據列表

        返回:
            int: 插入的記錄數量
        """
        if not jobs:
            logger.warning("沒有職缺數據可保存到 MongoDB")
            return 0

        # 獲取當前日期時間
        now = datetime.now()

        # 為每個職缺添加更新時間
        for job in jobs:
            job['mongodb_update_time'] = now

        today = datetime.now().strftime("%Y-%m-%d")

        # 批量插入或更新數據
        operations = []
        for job in jobs:
            operations.append(
                UpdateOne(
                    {'jobNo': job['jobNo']},
                    {
                        '$set': job,
                        '$setOnInsert': {
                            'discovery_date': today  # 只在插入时设置
                        }
                    },
                    upsert=True,
                )
            )

        result = self.db.jobs.bulk_write(operations)

        # 獲取插入和更新的數量
        upserted_count = len(result.upserted_ids)
        modified_count = result.modified_count
        total_count = upserted_count + modified_count

        logger.info(f"MongoDB: 插入 {upserted_count} 筆新職缺，更新 {modified_count} 筆現有職缺")

        return total_count

    def get_jobs(self, filters: Dict = None, limit: int = 1000) -> List[Dict]:
        """
        從 MongoDB 獲取職缺數據

        參數:
            filters: 過濾條件
            limit: 最大返回記錄數

        返回:
            List[Dict]: 職缺數據列表
        """
        filters = filters or {}

        # 執行查詢
        cursor = self.db.jobs.find(filters).limit(limit)

        # 將結果轉換為列表
        jobs = list(cursor)

        # 移除 MongoDB 的 _id 字段
        for job in jobs:
            if '_id' in job:
                del job['_id']

        logger.info(f"從 MongoDB 獲取了 {len(jobs)} 筆職缺數據")

        return jobs

    def get_jobs_dataframe(self, filters: Dict = None, limit: int = 1000) -> pd.DataFrame:
        """
        從 MongoDB 獲取職缺數據並轉換為 DataFrame

        參數:
            filters: 過濾條件
            limit: 最大返回記錄數

        返回:
            pd.DataFrame: 職缺數據 DataFrame
        """
        jobs = self.get_jobs(filters, limit)

        if not jobs:
            return pd.DataFrame()

        return pd.DataFrame(jobs)

    def update_job_status(self, current_job_ids: List[str]) -> int:
        """
        更新職缺的上下架狀態

        參數:
            current_job_ids: 當前爬取到的所有職缺ID列表

        返回:
            int: 更新的記錄數量
        """
        if not current_job_ids:
            logger.warning("沒有當前職缺ID列表，無法更新狀態")
            return 0

        # 獲取當前日期
        today = datetime.now().strftime("%Y-%m-%d")

        # 將資料庫中存在但當前未爬取到的職缺狀態更新為下架
        result = self.db.jobs.update_many(
            {
                'jobNo': {'$nin': current_job_ids},
                'status': 'active'
            },
            {
                '$set': {
                    'status': 'inactive',
                    'last_update_date': today,
                    'delisted_date': today
                }
            }
        )

        modified_count = result.modified_count
        logger.info(f"MongoDB: 更新了 {modified_count} 筆職缺狀態為下架")

        return modified_count

    def get_existing_jobs(self) -> Dict[str, str]:
        """
        獲取資料庫中所有現有職缺的ID和狀態。

        返回:
            Dict[str, str]: 職缺ID到狀態的映射字典
        """
        try:
            # 查詢資料庫中的所有職缺ID和狀態
            cursor = self.db.jobs.find({}, {'jobNo': 1, 'status': 1})

            # 將結果轉換為字典
            existing_job_dict = {}
            for job in cursor:
                if 'jobNo' in job and 'status' in job:
                    existing_job_dict[job['jobNo']] = job['status']
                elif 'jobNo' in job:
                    # 如果沒有status欄位，默認為active
                    existing_job_dict[job['jobNo']] = 'active'

            logger.info(f"從 MongoDB 獲取了 {len(existing_job_dict)} 筆現有職缺記錄")
            return existing_job_dict

        except Exception as e:
            logger.error(f"從 MongoDB 獲取現有職缺時發生錯誤: {e}")
            return {}

    def reactivate_jobs(self, job_ids: List[str]) -> int:
        """
        重新激活之前標記為下架的職缺。

        參數:
            job_ids: 需要重新激活的職缺ID列表

        返回:
            int: 更新的記錄數量
        """
        if not job_ids:
            logger.warning("沒有需要重新激活的職缺ID列表")
            return 0

        try:
            # 獲取當前日期
            today = datetime.now().strftime("%Y-%m-%d")

            # 將指定ID的職缺狀態更新為上架並清除下架日期
            result = self.db.jobs.update_many(
                {
                    'jobNo': {'$in': job_ids},
                    'status': 'inactive'
                },
                {
                    '$set': {
                        'status': 'active',
                        'last_update_date': today
                    },
                    '$unset': {
                        'delisted_date': ""
                    }
                }
            )

            modified_count = result.modified_count
            logger.info(f"MongoDB: 已將 {modified_count} 筆職缺狀態更新為上架")

            return modified_count

        except Exception as e:
            logger.error(f"MongoDB: 重新激活職缺時發生錯誤: {e}")
            return 0

    def close(self):
        """關閉 MongoDB 連接"""
        if hasattr(self, 'client') and self.client:
            self.client.close()
            logger.debug("MongoDB 連接已關閉")
