"""
趨勢分析模組

此模組負責分析職位數據中的趨勢。
它提供了識別職位發布隨時間變化的趨勢、
分析關鍵詞流行度和計算增長率的功能。

使用時機與場景:
1. 市場趨勢監控: 當需要監控就業市場的整體趨勢，了解職位發布數量的變化。
2. 技能需求分析: 當需要分析特定技能或關鍵詞在就業市場中的需求變化。
3. 熱門技能識別: 當需要識別當前市場上快速增長的熱門技能和關鍵詞。
4. 行業比較分析: 當需要比較不同行業的就業趨勢和增長情況。
5. 市場預測: 當需要基於歷史數據預測未來的就業市場趨勢。

使用方式:
1. 日常趨勢分析: 使用get_daily_job_trends()方法分析每日職位發布趨勢。
2. 關鍵詞趨勢分析: 使用get_keyword_trends()方法分析特定關鍵詞隨時間的變化趨勢。
3. 增長率計算: 使用calculate_growth_rates()方法計算不同時間段的增長率。
4. 熱門關鍵詞識別: 使用identify_trending_keywords()方法識別當前市場上增長最快的關鍵詞。
5. 行業比較: 使用compare_industries()方法比較不同行業的就業趨勢。
6. 數據可視化: 使用plot_daily_trends()和plot_keyword_trends()方法將分析結果可視化。

示例:
```python
# 初始化分析器
analyzer = TrendAnalyzer()

# 獲取每日職位趨勢
daily_trends = analyzer.get_daily_job_trends()
analyzer.plot_daily_trends(daily_trends)

# 獲取關鍵詞趨勢
keyword_trends = analyzer.get_keyword_trends(['Python', 'Java', 'JavaScript'])
analyzer.plot_keyword_trends(keyword_trends, ['Python', 'Java', 'JavaScript'])

# 識別熱門關鍵詞
trending_keywords = analyzer.identify_trending_keywords()
print(trending_keywords)

# 關閉數據庫連接
analyzer.close()
```
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Union, Tuple
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta


from config.settings import logger


class TrendAnalyzer:
    """
    用於分析職位數據趨勢的類。

    此類提供方法來分析職位發布隨時間的變化趨勢，
    識別熱門關鍵詞，以及計算增長率。
    """

    def __init__(self):
        pass

    def create_job_trend_chart(self, jobs_df):
        """職缺趨勢圖表

        :param jobs_df:
        :return:
        """
        logger.info("計算每日職缺變化")
        stats_df = self.analyze_daily_job_changes(jobs_df)

        if stats_df.empty:
            logger.warning("無法計算每日職缺變化，可能是因為數據不足")
            return

        logger.info(f"計算了 {len(stats_df)} 天的職缺變化")

        #
        # # 準備要插入的數據
        stats_df = stats_df[["appear_date", "jobNo", "new_jobs", "removed_jobs"]].copy()
        stats_df.columns = ["date", "total_jobs", "new_jobs", "removed_jobs"]
        #
        # # 確保removed_jobs是正數（因為在analyze_daily_job_changes中它可能是負數）
        stats_df["removed_jobs"] = stats_df["removed_jobs"].abs()
        #
        # # 計算淨變化（新增職缺減去減少職缺）
        stats_df["net_change"] = stats_df["new_jobs"] - stats_df["removed_jobs"]
        #
        # # 將NaN值替換為0
        stats_df = stats_df.fillna(0)
        #
        logger.debug(
            f"獲取到 {len(stats_df) if not stats_df.empty else 0} 條每日統計數據"
        )
        return stats_df

    def analyze_daily_job_changes(
        self,
        jobs_df: pd.DataFrame,
        date_column: str = "appearDate",
        date_format: str = "%Y%m%d",
    ) -> pd.DataFrame:
        """
        分析每日職缺新增和減少情況。

        參數:
            jobs_df: 包含職缺數據的DataFrame。
            date_column: 日期列的名稱。默認為'appearDate'。
            date_format: 日期格式。默認為'%Y%m%d'。

        返回:
            pd.DataFrame: 包含每日職缺變化分析的DataFrame。
        """
        logger.info("開始分析每日職缺變化")

        # 將日期欄位轉換為日期格式
        jobs_df["appear_date"] = pd.to_datetime(
            jobs_df[date_column], format=date_format
        )

        # 創建日期範圍（從最早的appear_date到最晚的appear_date或delisted_date）
        min_date = jobs_df["appear_date"].min()

        # 如果存在delisted_date欄位，也考慮它的日期範圍
        if "delisted_date" in jobs_df.columns:
            # 將delisted_date轉換為日期格式，忽略空值
            jobs_df["delisted_date"] = pd.to_datetime(
                jobs_df["delisted_date"], errors="coerce"
            )
            max_delisted = jobs_df["delisted_date"].max()
            max_date = (
                max(jobs_df["appear_date"].max(), max_delisted)
                if not pd.isna(max_delisted)
                else jobs_df["appear_date"].max()
            )
        else:
            max_date = jobs_df["appear_date"].max()

        # 創建包含所有日期的DataFrame
        date_range = pd.date_range(start=min_date, end=max_date)
        daily_jobs = pd.DataFrame({"appear_date": date_range})

        # 計算每天新增的職缺數量（以appearDate為準）
        new_jobs_by_date = (
            jobs_df.groupby("appear_date").size().reset_index(name="new_jobs")
        )
        daily_jobs = daily_jobs.merge(new_jobs_by_date, on="appear_date", how="left")

        # 計算每天減少的職缺數量（以delisted_date為準）
        if "delisted_date" in jobs_df.columns:
            # 只考慮有效的delisted_date
            valid_delisted = jobs_df[jobs_df["delisted_date"].notna()]
            if not valid_delisted.empty:
                removed_jobs_by_date = (
                    valid_delisted.groupby("delisted_date")
                    .size()
                    .reset_index(name="removed_jobs")
                )
                daily_jobs = daily_jobs.merge(
                    removed_jobs_by_date,
                    left_on="appear_date",
                    right_on="delisted_date",
                    how="left",
                )
                daily_jobs.drop("delisted_date", axis=1, inplace=True, errors="ignore")

        # 填充缺失值為0
        daily_jobs["new_jobs"] = daily_jobs["new_jobs"].fillna(0)
        if "removed_jobs" not in daily_jobs.columns:
            daily_jobs["removed_jobs"] = 0
        else:
            daily_jobs["removed_jobs"] = daily_jobs["removed_jobs"].fillna(0)

        # 按日期排序
        daily_jobs = daily_jobs.sort_values("appear_date")

        # 計算每天的職缺總數（累計新增減去累計減少）
        daily_jobs["jobNo"] = (
            daily_jobs["new_jobs"].cumsum() - daily_jobs["removed_jobs"].cumsum()
        )

        # 計算變化率
        daily_jobs["new_delta"] = daily_jobs["new_jobs"].diff()
        daily_jobs["removed_delta"] = daily_jobs["removed_jobs"].diff()

        logger.info(f"成功分析每日職缺變化，共{len(daily_jobs)}天的數據")
        return daily_jobs

    def analyze_job_details_by_date(
        self,
        jobs_df: pd.DataFrame,
        date_column: str = "appearDate",
        date_format: str = "%Y%m%d",
    ) -> pd.DataFrame:
        """
        按日期分析職缺詳細信息，包括新增和減少的具體職缺。

        參數:
            jobs_df: 包含職缺數據的DataFrame。
            date_column: 日期列的名稱。默認為'appearDate'。
            date_format: 日期格式。默認為'%Y%m%d'。

        返回:
            pd.DataFrame: 包含每日職缺詳細變化的DataFrame。
        """
        logger.info("開始按日期分析職缺詳細信息")

        # 將日期欄位轉換為日期格式
        jobs_df["appear_date"] = pd.to_datetime(
            jobs_df[date_column], format=date_format
        )

        # 準備要聚合的列
        agg_dict = {
            "jobNo": "count",
            "jobName": list,
            "custName": list,
            "link": list,
            "job": list,
            "search_keyword": list,
        }

        # 如果存在城市和地區列，也進行聚合
        if "city" in jobs_df.columns:
            agg_dict["city"] = list
        if "district" in jobs_df.columns:
            agg_dict["district"] = list

        # 按日期分組計算每天的職缺數，並保存職位名稱、公司名稱、URL、城市和地區
        daily_jobs = jobs_df.groupby("appear_date").agg(agg_dict).reset_index()

        daily_jobs = daily_jobs.sort_values("appear_date")

        # 計算每天的總職缺數和變化
        daily_jobs["total_count"] = daily_jobs["jobNo"]
        daily_jobs["count_diff"] = daily_jobs["jobNo"].diff()

        logger.info(f"成功分析每日職缺詳細信息，共{len(daily_jobs)}天的數據")
        return daily_jobs

    def analyze_industry_distribution(
        self, jobs_df: pd.DataFrame, industry_column: str = "coIndustryDesc"
    ) -> pd.DataFrame:
        """
        分析產業職缺分佈統計。

        參數:
            jobs_df: 包含職缺數據的DataFrame。
            industry_column: 產業類別列的名稱。默認為'coIndustryDesc'。

        返回:
            pd.DataFrame: 包含產業職缺分佈統計的DataFrame。
        """
        logger.info("開始分析產業職缺分佈統計")

        # 計算產業分佈統計
        industry_dist = (
            jobs_df.groupby(industry_column)
            .agg({"jobNo": "count", "custName": "nunique", "jobName": list})
            .reset_index()
        )

        industry_dist.columns = ["產業類別", "職缺數", "公司數", "職缺列表"]
        industry_dist = industry_dist.sort_values("職缺數", ascending=False)

        logger.info(f"成功分析產業職缺分佈，共{len(industry_dist)}個產業類別")
        return industry_dist

    def analyze_industry_trends(
        self,
        jobs_df: pd.DataFrame,
        date_column: str = "appearDate",
        industry_column: str = "coIndustryDesc",
        date_format: str = "%Y%m%d",
    ) -> pd.DataFrame:
        """
        按產業和月份分析職缺趨勢。

        參數:
            jobs_df: 包含職缺數據的DataFrame。
            date_column: 日期列的名稱。默認為'appearDate'。
            industry_column: 產業類別列的名稱。默認為'coIndustryDesc'。
            date_format: 日期格式。默認為'%Y%m%d'。

        返回:
            pd.DataFrame: 包含產業月度職缺趨勢的DataFrame。
        """
        logger.info("開始按產業和月份分析職缺趨勢")

        # 將日期轉換為月份格式
        jobs_df["month"] = pd.to_datetime(
            jobs_df[date_column], format=date_format
        ).dt.strftime("%Y-%m")

        # 按產業和月份統計職缺
        monthly_industry_stats = jobs_df.pivot_table(
            values="jobNo",
            index=industry_column,
            columns="month",
            aggfunc="count",
            fill_value=0,
        ).reset_index()

        monthly_industry_stats.columns.name = None
        monthly_industry_stats = monthly_industry_stats.rename(
            columns={industry_column: "產業類別"}
        )

        # 計算增長量和增長率
        month_cols = [
            col for col in monthly_industry_stats.columns if col != "產業類別"
        ]
        if len(month_cols) >= 2:
            monthly_industry_stats["增長量"] = (
                monthly_industry_stats[month_cols].iloc[:, -1]
                - monthly_industry_stats[month_cols].iloc[:, 0]
            )

            # 處理除以0的情況
            monthly_industry_stats["增長率"] = np.where(
                monthly_industry_stats[month_cols].iloc[:, 0] == 0,
                np.where(monthly_industry_stats["增長量"] > 0, "首次出現", "0%"),
                (
                    monthly_industry_stats["增長量"]
                    / monthly_industry_stats[month_cols].iloc[:, 0]
                    * 100
                )
                .round(2)
                .astype(str)
                + "%",
            )

        logger.info(
            f"成功分析產業月度職缺趨勢，共{len(monthly_industry_stats)}個產業類別"
        )
        return monthly_industry_stats


# 使用示例
if __name__ == "__main__":
    analyzer = TrendAnalyzer()
