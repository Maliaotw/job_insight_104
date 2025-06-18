"""
此模組提供用於處理和分析職缺數據的DataFrame工具函數。
這些函數不依賴於數據庫操作，可以獨立使用於任何pandas DataFrame。
"""

from typing import List

import pandas as pd

from config.settings import logger


def prepare_jobs_analysis_df(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """
    準備具有標準化列名和計算字段的職缺分析DataFrame。

    參數:
        jobs_df: 包含原始職缺數據的DataFrame

    返回:
        具有標準化列用於分析的DataFrame
    """
    # 創建職缺分析DataFrame
    jobs_analysis = pd.DataFrame()

    # 添加基本職缺信息
    jobs_analysis["職缺編號"] = (
        jobs_df["jobNo"] if "jobNo" in jobs_df.columns else range(len(jobs_df))
    )
    jobs_analysis["職稱"] = (
        jobs_df["jobName"] if "jobName" in jobs_df.columns else "未知"
    )
    jobs_analysis["公司名稱"] = (
        jobs_df["custName"] if "custName" in jobs_df.columns else "未知"
    )
    jobs_analysis["城市"] = jobs_df["city"]
    jobs_analysis["地區"] = jobs_df["district"]
    jobs_analysis["關鍵字"] = jobs_df["search_keyword"]
    jobs_analysis["連結"] = jobs_df["job"]

    # 如果有應徵信息，添加應徵人數範圍
    if "applyDesc" in jobs_df.columns:
        jobs_analysis["應徵人數範圍"] = jobs_df["applyDesc"]

    # 如果有薪資信息，添加薪資範圍
    if "salaryDesc" in jobs_df.columns:
        jobs_analysis["薪資範圍"] = jobs_df["salaryDesc"]

    # 如果有工作經驗和教育程度信息，添加相應欄位
    if "periodDesc" in jobs_df.columns:
        jobs_analysis["工作經驗"] = jobs_df["periodDesc"]

    if "optionEdu" in jobs_df.columns:
        jobs_analysis["教育程度"] = jobs_df["optionEdu"]

    # 如果有日期信息，計算職缺在架時間
    if "appearDate" in jobs_df.columns:
        jobs_analysis["上架日期"] = pd.to_datetime(
            jobs_df["appearDate"], format="%Y%m%d"
        )
        jobs_analysis["在架天數"] = (
            pd.Timestamp.now() - jobs_analysis["上架日期"]
        ).dt.days

        # 識別長期未招滿的職缺（在架超過30天）
        jobs_analysis["是否長期未招滿"] = jobs_analysis["在架天數"] > 30

        # 剛發布的職缺
        jobs_analysis["近期發布的職缺"] = jobs_analysis["在架天數"] < 30

    # 保留職缺狀態和下架日期信息（用於分析已下架職缺）
    if "status" in jobs_df.columns:
        jobs_analysis["status"] = jobs_df["status"]

    if "delisted_date" in jobs_df.columns:
        jobs_analysis["delisted_date"] = jobs_df["delisted_date"]
        jobs_analysis["下架日期"] = jobs_df["delisted_date"]

    return jobs_analysis


def extract_application_counts(jobs_analysis: pd.DataFrame) -> pd.DataFrame:
    """
    從應徵描述中提取應徵人數。

    參數:
        jobs_analysis: 包含職缺分析數據的DataFrame

    返回:
        添加了應徵人數列的DataFrame
    """
    if "應徵人數範圍" not in jobs_analysis.columns:
        logger.warning("在jobs_analysis中找不到應徵人數列")
        return jobs_analysis

    result_df = jobs_analysis.copy()

    # 提取應徵人數
    def extract_apply_count(apply_desc):
        if pd.isna(apply_desc) or apply_desc == "":
            return 0, 0

        if "人" in apply_desc:
            parts = apply_desc.split("人")[0].strip()
            if "~" in parts:
                try:
                    min_count, max_count = parts.split("~")
                    return int(min_count), int(max_count)
                except:
                    pass
            else:
                try:
                    count = int(parts)
                    return count, count
                except:
                    pass
        return 0, 0

    # 應用提取函數
    result_df[["最少應徵人數", "最多應徵人數"]] = result_df["應徵人數範圍"].apply(
        lambda x: pd.Series(extract_apply_count(x))
    )

    # 計算平均應徵人數
    result_df["平均應徵人數"] = (
        result_df["最少應徵人數"] + result_df["最多應徵人數"]
    ) / 2

    return result_df


def extract_salary_range(jobs_analysis: pd.DataFrame) -> pd.DataFrame:
    """
    從薪資描述中提取薪資範圍。

    參數:
        jobs_analysis: 包含職缺分析數據的DataFrame

    返回:
        添加了薪資範圍列的DataFrame
    """
    if "薪資範圍" not in jobs_analysis.columns:
        logger.warning("在jobs_analysis中找不到薪資列")
        return jobs_analysis

    result_df = jobs_analysis.copy()

    # 提取薪資範圍
    def extract_salary(salary_desc):
        if pd.isna(salary_desc) or salary_desc == "":
            return 0, 0

        # 處理月薪
        if "月薪" in salary_desc:
            parts = salary_desc.split("月薪")[1].strip()
            if "~" in parts:
                try:
                    min_salary, max_salary = parts.split("~")
                    min_salary = min_salary.replace("元", "").replace(",", "").strip()
                    max_salary = max_salary.replace("元", "").replace(",", "").strip()
                    return int(min_salary), int(max_salary)
                except:
                    pass
            else:
                try:
                    salary = parts.replace("元", "").replace(",", "").strip()
                    return int(salary), int(salary)
                except:
                    pass

        # 處理時薪
        if "時薪" in salary_desc:
            parts = salary_desc.split("時薪")[1].strip()
            try:
                hourly = parts.replace("元", "").replace(",", "").strip()
                # 轉換為月薪（假設每月160小時）
                monthly = int(hourly) * 160
                return monthly, monthly
            except:
                pass

        return 0, 0

    # 應用提取函數
    result_df[["最低薪資", "最高薪資"]] = result_df["薪資範圍"].apply(
        lambda x: pd.Series(extract_salary(x))
    )

    # 計算平均薪資
    result_df["平均薪資"] = (result_df["最低薪資"] + result_df["最高薪資"]) / 2

    return result_df


def analyze_industry_distribution(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """
    分析職缺按產業的分佈情況。

    參數:
        jobs_df: 包含職缺數據的DataFrame

    返回:
        包含產業分佈統計的DataFrame
    """
    if "coIndustryDesc" not in jobs_df.columns:
        logger.warning("在jobs_df中找不到產業列")
        return pd.DataFrame()

    # 按產業統計職缺數量
    industry_counts = jobs_df["coIndustryDesc"].value_counts().reset_index()
    industry_counts.columns = ["產業", "職缺數量"]

    # 計算百分比
    total_jobs = len(jobs_df)
    industry_counts["佔比"] = industry_counts["職缺數量"] / total_jobs * 100

    # 按職缺數量排序
    industry_counts = industry_counts.sort_values("職缺數量", ascending=False)

    return industry_counts


def get_job_display_columns(df: pd.DataFrame = None) -> List[str]:
    """
    獲取標準的職缺顯示欄位列表。

    參數:
        df: 可選的DataFrame，如果提供，將只返回該DataFrame中存在的欄位

    返回:
        用於顯示的欄位名稱列表
    """
    # 定義標準顯示欄位
    standard_cols = [
        "城市",
        "地區",
        "職稱",
        "公司名稱",
        "在架天數",
        "上架日期",
        "連結",
        "薪資範圍",
        "應徵人數範圍",
        "工作經驗",
        "教育程度",
        "關鍵字",
    ]

    # 如果提供了DataFrame，只返回存在的欄位
    if df is not None:
        return [col for col in standard_cols if col in df.columns]

    return standard_cols
