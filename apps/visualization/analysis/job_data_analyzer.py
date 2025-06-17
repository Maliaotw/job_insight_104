"""
此模組提供分析104人力銀行職缺數據的功能。
它集中了原本分散在視覺化應用程序中的pandas計算。
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Union, Tuple
from datetime import datetime, timedelta

from src.database.duckdb_manager import DuckDBManager
from apps.visualization.analysis.df_utils import (
    prepare_jobs_analysis_df,
    extract_application_counts,
    extract_salary_range,
    analyze_industry_distribution,
    get_job_display_columns
)
from config.settings import logger


class JobDataAnalyzer:
    """
    用於分析104人力銀行職缺數據的類別。

    此類別集中了原本分散在視覺化應用程序中的pandas計算。它提供了過濾職缺、
    分析職缺數據以及準備視覺化數據的方法。
    """

    def __init__(self):
        """
        使用數據庫管理器初始化JobDataAnalyzer。

        參數:
            db_manager: 用於數據庫操作的DuckDBManager實例。
                        如果為None，將創建一個新的實例。
        """

        self.db_manager = DuckDBManager()
        self.should_close_db = True


        logger.info("JobDataAnalyzer 初始化完成")

    def get_jobs(self, limit = 10000, months: int = None, keywords: List[str] = None, city: str = None, district: str = None, include_inactive: bool = False) -> pd.DataFrame:
        """
        從數據庫獲取職缺，並可選擇性地進行過濾。

        參數:
            limit: 最大獲取職缺數量，如果是"無限制"則不限制數量
            months: 如果提供，只獲取最近N個月的職缺
            keywords: 用於過濾職缺的關鍵詞列表
            city: 用於過濾職缺的城市
            district: 用於過濾職缺的地區
            include_inactive: 是否包含已下架的職缺。默認為False。

        返回:
            包含過濾後職缺的DataFrame
        """
        # 處理"無限制"選項
        if limit == "無限制":
            # 使用一個非常大的數字作為實際限制，相當於無限制
            db_limit = 1000000
        else:
            db_limit = limit

        # 從數據庫獲取指定數量的職缺
        jobs_df = self.db_manager.get_jobs(limit=db_limit, include_inactive=include_inactive)

        if jobs_df.empty:
            logger.warning("數據庫中沒有找到職缺")
            return jobs_df

        # 如果提供了月份參數，按日期過濾
        if months is not None and 'appearDate' in jobs_df.columns:
            logger.info(f"過濾最近 {months} 個月的職缺")
            # 如果appearDate不是datetime類型，則轉換
            if not pd.api.types.is_datetime64_dtype(jobs_df['appearDate']):
                jobs_df['appearDate'] = pd.to_datetime(jobs_df['appearDate'], format='%Y%m%d')

            # 計算截止日期
            cutoff_date = datetime.now() - timedelta(days=30*months)

            # 過濾職缺
            jobs_df = jobs_df[jobs_df['appearDate'] >= cutoff_date]

        # 如果提供了關鍵詞、城市或地區，進行過濾
        if keywords or city or district:
            jobs_df = self.filter_jobs_by_keywords(jobs_df, keywords, city, district)

        return jobs_df

    def filter_jobs_by_keywords(self, jobs_df: pd.DataFrame, keywords: List[str] = None, 
                               city: str = None, district: str = None) -> pd.DataFrame:
        """
        根據關鍵詞、城市和地區過濾職缺。

        參數:
            jobs_df: 包含要過濾的職缺的DataFrame
            keywords: 用於過濾的關鍵詞列表
            city: 用於過濾的城市
            district: 用於過濾的地區

        返回:
            過濾後的DataFrame
        """
        filtered_df = jobs_df.copy()

        # 如果提供了關鍵詞，進行過濾
        if keywords and len(keywords) > 0:
            logger.info(f"根據關鍵詞過濾職缺: {keywords}")

            # 檢查是否有search_keyword欄位
            if 'search_keyword' in filtered_df.columns:
                # 使用search_keyword欄位進行過濾
                logger.info("使用search_keyword欄位進行過濾")

                # 創建一個臨時列來存儲搜索文本
                filtered_df['search_text_temp'] = filtered_df['search_keyword'].apply(
                    lambda x: ' '.join(x) if isinstance(x, list) else str(x)
                )

                # 將空值填充為空字符串，並轉換為小寫
                filtered_df['search_text_temp'] = filtered_df['search_text_temp'].fillna('').str.lower()

                # 按每個關鍵詞過濾
                for keyword in keywords:
                    keyword = keyword.lower()
                    filtered_df = filtered_df[filtered_df['search_text_temp'].str.contains(keyword, na=False)]

                # 刪除臨時列
                filtered_df = filtered_df.drop('search_text_temp', axis=1)
            else:
                # 如果沒有search_keyword欄位，則使用原來的方法
                logger.info("search_keyword欄位不存在，使用組合文本進行過濾")
                # 創建用於搜索的組合文本字段
                if 'jobDetail' in filtered_df.columns and 'jobName' in filtered_df.columns:
                    filtered_df['search_text'] = filtered_df['jobName'] + ' ' + filtered_df['jobDetail'].fillna('')
                elif 'jobName' in filtered_df.columns:
                    filtered_df['search_text'] = filtered_df['jobName']
                else:
                    # 如果兩個列都不存在，創建一個空列
                    filtered_df['search_text'] = ''

                # 如果有公司名稱，添加到搜索文本中
                if 'custName' in filtered_df.columns:
                    filtered_df['search_text'] = filtered_df['search_text'] + ' ' + filtered_df['custName'].fillna('')

                # 將搜索文本轉換為小寫，以便進行不區分大小寫的搜索
                filtered_df['search_text'] = filtered_df['search_text'].str.lower()

                # 按每個關鍵詞過濾
                for keyword in keywords:
                    keyword = keyword.lower()
                    filtered_df = filtered_df[filtered_df['search_text'].str.contains(keyword, na=False)]

                # 刪除臨時搜索文本列
                filtered_df = filtered_df.drop('search_text', axis=1)

        # 如果提供了城市，進行過濾
        if city and 'city' in filtered_df.columns:
            logger.info(f"根據城市過濾職缺: {city}")
            filtered_df = filtered_df[filtered_df['city'] == city]

        # 如果提供了地區，進行過濾
        if district and 'district' in filtered_df.columns:
            logger.info(f"根據地區過濾職缺: {district}")
            filtered_df = filtered_df[filtered_df['district'] == district]

        logger.info(f"過濾後的職缺: {len(filtered_df)} 個，共 {len(jobs_df)} 個")
        return filtered_df

    def prepare_jobs_analysis_df(self, jobs_df: pd.DataFrame) -> pd.DataFrame:
        """
        準備具有標準化列名和計算字段的職缺分析DataFrame。

        參數:
            jobs_df: 包含原始職缺數據的DataFrame

        返回:
            具有標準化列用於分析的DataFrame
        """
        return prepare_jobs_analysis_df(jobs_df)

    def extract_application_counts(self, jobs_analysis: pd.DataFrame) -> pd.DataFrame:
        """
        從應徵描述中提取應徵人數。

        參數:
            jobs_analysis: 包含職缺分析數據的DataFrame

        返回:
            添加了應徵人數列的DataFrame
        """
        return extract_application_counts(jobs_analysis)

    def extract_salary_range(self, jobs_analysis: pd.DataFrame) -> pd.DataFrame:
        """
        從薪資描述中提取薪資範圍。

        參數:
            jobs_analysis: 包含職缺分析數據的DataFrame

        返回:
            添加了薪資範圍列的DataFrame
        """
        return extract_salary_range(jobs_analysis)

    def analyze_industry_distribution(self, jobs_df: pd.DataFrame) -> pd.DataFrame:
        """
        分析職缺按產業的分佈情況。

        參數:
            jobs_df: 包含職缺數據的DataFrame

        返回:
            包含產業分佈統計的DataFrame
        """
        return analyze_industry_distribution(jobs_df)

    def get_job_display_columns(self, df: pd.DataFrame = None) -> List[str]:
        """
        獲取標準的職缺顯示欄位列表。

        參數:
            df: 可選的DataFrame，如果提供，將只返回該DataFrame中存在的欄位

        返回:
            用於顯示的欄位名稱列表
        """
        return get_job_display_columns(df)

    def close(self):
        """如果數據庫連接是由此實例創建的，則關閉它。"""
        if self.should_close_db:
            self.db_manager.close()
