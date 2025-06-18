"""
Hiring efficiency page for the 104 Job Insight visualization app.
This page analyzes the efficiency of hiring processes across different job listings.

This module follows the Single Responsibility Principle by separating:
1. Data processing (HiringEfficiencyDataProcessor)
2. UI rendering (HiringEfficiencyPageRenderer)
3. Page controller (show_hiring_efficiency_page)
"""

from typing import Optional, List

import pandas as pd
import plotly.express as px
import streamlit as st

from apps.visualization.analysis.df_utils import (
    prepare_jobs_analysis_df,
    extract_application_counts,
)
from apps.visualization.analysis.job_data_analyzer import JobDataAnalyzer
from apps.visualization.components import display_filter_info
from config.settings import logger


# Component functions integrated from separate files
def display_job_duration_distribution(jobs_analysis):
    """
    顯示職缺在架時間分布

    參數:
        jobs_analysis: 職缺分析數據DataFrame
    """
    # 檢查是否有在架天數數據
    if "在架天數" in jobs_analysis.columns:
        # 記錄顯示職缺在架時間分布
        logger.debug("顯示職缺在架時間分布區塊")
        st.subheader("職缺在架時間分布")
        logger.info("創建職缺在架時間分布圖表")

        # 創建直方圖
        fig = px.histogram(
            jobs_analysis,
            x="在架天數",
            nbins=20,
            title="職缺在架時間分布",
            color_discrete_sequence=["skyblue"],
        )
        st.plotly_chart(fig, use_container_width=True)
        logger.info("職缺在架時間分布圖表顯示完成")


def display_experience_application_relationship(jobs_analysis):
    """
    顯示工作經驗與應徵人數關係

    參數:
        jobs_analysis: 職缺分析數據DataFrame
    """
    # 檢查是否有工作經驗和應徵人數範圍數據
    if all(col in jobs_analysis.columns for col in ["應徵人數範圍", "工作經驗"]):
        # 記錄顯示工作經驗與應徵人數關係
        logger.debug("顯示工作經驗與應徵人數關係區塊")
        st.subheader("工作經驗與應徵人數關係")
        logger.info("分析工作經驗與應徵人數關係")

        # 創建交叉表
        exp_apply_cross = pd.crosstab(
            jobs_analysis["工作經驗"], jobs_analysis["應徵人數範圍"]
        )
        logger.debug(f"交叉表大小: {exp_apply_cross.shape}")

        # 創建堆疊條形圖
        fig = px.bar(exp_apply_cross, barmode="stack", title="工作經驗與應徵人數關係")
        fig.update_layout(
            xaxis_title="工作經驗要求", yaxis_title="職缺數量", xaxis_tickangle=-45
        )
        st.plotly_chart(fig, use_container_width=True)
        logger.info("工作經驗與應徵人數關係圖表顯示完成")


def display_job_competition_distribution(jobs_analysis):
    """
    顯示職缺競爭度分布

    參數:
        jobs_analysis: 職缺分析數據DataFrame
    """
    # 檢查是否有職缺競爭度數據
    if "職缺競爭度" in jobs_analysis.columns:
        # 記錄顯示職缺競爭度分布
        logger.debug("顯示職缺競爭度分布區塊")
        st.subheader("職缺競爭度分布")
        logger.info("創建職缺競爭度分布圖表")

        # 創建餅圖
        competition_counts = jobs_analysis["職缺競爭度"].value_counts()
        fig = px.pie(
            values=competition_counts.values,
            names=competition_counts.index,
            title="職缺競爭度分布",
        )
        st.plotly_chart(fig, use_container_width=True)
        logger.info("職缺競爭度分布圖表顯示完成")


def display_education_salary_relationship(jobs_analysis):
    """
    顯示教育程度與薪資範圍關係

    參數:
        jobs_analysis: 職缺分析數據DataFrame
    """
    # 檢查是否有教育程度和薪資範圍數據
    if all(col in jobs_analysis.columns for col in ["教育程度", "薪資範圍"]):
        # 記錄顯示教育程度與薪資範圍關係
        logger.debug("顯示教育程度與薪資範圍關係區塊")
        st.subheader("教育程度與薪資範圍關係")
        logger.info("分析教育程度與薪資範圍關係")

        # 創建交叉表
        edu_salary_cross = pd.crosstab(
            jobs_analysis["教育程度"], jobs_analysis["薪資範圍"]
        )
        logger.debug(f"交叉表大小: {edu_salary_cross.shape}")

        # 創建堆疊條形圖
        fig = px.bar(edu_salary_cross, barmode="stack", title="教育程度與薪資範圍關係")
        fig.update_layout(
            xaxis_title="教育程度要求", yaxis_title="職缺數量", xaxis_tickangle=-45
        )
        st.plotly_chart(fig, use_container_width=True)
        logger.info("教育程度與薪資範圍關係圖表顯示完成")


def display_hiring_efficiency_long_unfilled_jobs(job_data_analyzer, jobs_analysis):
    """
    顯示長期未招滿職缺分析（招聘效率分析頁面專用）

    參數:
        job_data_analyzer: JobDataAnalyzer實例
        jobs_analysis: 職缺分析數據DataFrame
    """
    # 檢查是否有長期未招滿數據
    if "是否長期未招滿" in jobs_analysis.columns:
        # 記錄顯示長期未招滿職缺分析
        logger.debug("顯示長期未招滿職缺分析區塊")
        st.subheader("長期未招滿職缺分析")
        logger.info("分析長期未招滿職缺")

        # 篩選長期未招滿職缺
        long_term_unfilled = jobs_analysis[jobs_analysis["是否長期未招滿"]]
        logger.debug(f"長期未招滿職缺數量: {len(long_term_unfilled)}")
        st.write(f"長期未招滿職缺數量: {len(long_term_unfilled)}")

        if not long_term_unfilled.empty:
            # 顯示長期未招滿職缺表格
            display_cols = job_data_analyzer.get_job_display_columns(long_term_unfilled)
            st.dataframe(
                long_term_unfilled[display_cols].sort_values(
                    "在架天數", ascending=False
                ),
                use_container_width=True,
            )
            logger.info("長期未招滿職缺表格顯示完成")
        else:
            logger.info("沒有發現長期未招滿的職缺")
            st.info("沒有發現長期未招滿的職缺")


def display_hiring_efficiency_recent_jobs(job_data_analyzer, jobs_analysis):
    """
    顯示剛開始招募的職缺列表（招聘效率分析頁面專用）

    參數:
        job_data_analyzer: JobDataAnalyzer實例
        jobs_analysis: 職缺分析數據DataFrame
    """
    # 檢查是否有在架天數數據
    if "在架天數" in jobs_analysis.columns:
        # 記錄顯示剛開始招募的職缺列表
        logger.debug("顯示剛開始招募的職缺列表區塊")
        st.subheader("剛開始招募的職缺列表")
        st.markdown("顯示在架天數少於30天的職缺")
        logger.info("分析剛開始招募的職缺")

        # 篩選剛開始招募的職缺
        recent_jobs = jobs_analysis[jobs_analysis["在架天數"] < 30]
        logger.debug(f"剛開始招募的職缺數量: {len(recent_jobs)}")
        st.write(f"剛開始招募的職缺數量: {len(recent_jobs)}")

        if not recent_jobs.empty:
            # 顯示剛開始招募的職缺表格
            display_cols = job_data_analyzer.get_job_display_columns(recent_jobs)
            st.dataframe(
                recent_jobs[display_cols].sort_values("在架天數", ascending=True),
                use_container_width=True,
            )
            logger.info("剛開始招募的職缺表格顯示完成")
        else:
            logger.info("沒有發現剛開始招募的職缺")
            st.info("沒有發現剛開始招募的職缺")


# Constants
MAX_JOBS_LIMIT = 10000
REQUIRED_COLUMNS = [
    "jobNo",
    "jobName",
    "custName",
    "applyDesc",
    "salaryDesc",
    "periodDesc",
    "optionEdu",
    "appearDate",
]
LOW_COMPETITION_RANGES = ["0~5人應徵"]
HIGH_COMPETITION_RANGES = ["大於30人應徵"]


class HiringEfficiencyDataProcessor:
    """
    負責處理招聘效率分析所需的數據處理邏輯。
    遵循單一職責原則，專注於數據處理，不包含UI渲染邏輯。
    """

    def __init__(self, job_data_analyzer):
        """
        初始化數據處理器。

        Args:
            job_data_analyzer: JobDataAnalyzer instance for data processing
        """
        self.job_data_analyzer: JobDataAnalyzer = job_data_analyzer

    def load_jobs_data(
        self,
        keywords: Optional[List[str]] = None,
        city: Optional[str] = None,
        district: Optional[str] = None,
        limit: int = MAX_JOBS_LIMIT,
        months: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        從數據庫載入職缺數據。

        Args:
            keywords: 用於過濾職缺的關鍵詞列表
            city: 用於過濾職缺的城市
            district: 用於過濾職缺的地區
            limit: 最大獲取職缺數量
            months: 如果提供，只獲取最近N個月的職缺

        Returns:
            職缺數據DataFrame
        """
        logger.info("從數據庫獲取職缺數據")
        jobs_df = self.job_data_analyzer.get_jobs(
            limit=limit, months=months, keywords=keywords, city=city, district=district
        )
        logger.debug(f"獲取到 {len(jobs_df)} 條職缺數據")
        return jobs_df

    def check_required_columns(self, jobs_df: pd.DataFrame) -> List[str]:
        """
        檢查職缺數據是否包含所需列。

        Args:
            jobs_df: 職缺數據DataFrame

        Returns:
            缺少的列名列表
        """
        logger.debug("檢查職缺數據是否包含所需列")
        return [col for col in REQUIRED_COLUMNS if col not in jobs_df.columns]

    def prepare_analysis_data(self, jobs_df: pd.DataFrame) -> pd.DataFrame:
        """
        準備職缺分析數據。

        Args:
            jobs_df: 職缺數據DataFrame

        Returns:
            職缺分析數據DataFrame
        """
        logger.info("準備職缺分析數據")
        jobs_analysis = prepare_jobs_analysis_df(jobs_df)
        logger.debug(f"準備了 {len(jobs_analysis)} 條職缺分析數據")
        return jobs_analysis

    def process_application_data(self, jobs_analysis: pd.DataFrame) -> pd.DataFrame:
        """
        處理應徵人數數據並計算競爭度。

        Args:
            jobs_analysis: 職缺分析數據DataFrame

        Returns:
            更新後的職缺分析數據DataFrame
        """
        if "應徵人數範圍" in jobs_analysis.columns:
            # 提取應徵人數數據
            logger.info("提取應徵人數數據")
            jobs_analysis = extract_application_counts(jobs_analysis)
            logger.debug("應徵人數數據提取完成")

            # 計算職缺競爭度
            logger.info("計算職缺競爭度")
            jobs_analysis["職缺競爭度"] = "中等"
            jobs_analysis.loc[
                jobs_analysis["應徵人數範圍"].isin(LOW_COMPETITION_RANGES), "職缺競爭度"
            ] = "低"
            jobs_analysis.loc[
                jobs_analysis["應徵人數範圍"].isin(HIGH_COMPETITION_RANGES),
                "職缺競爭度",
            ] = "高"
            logger.debug("職缺競爭度計算完成")

        return jobs_analysis


class HiringEfficiencyPageRenderer:
    """
    負責招聘效率分析頁面的UI渲染邏輯。
    遵循單一職責原則，專注於UI渲染，不包含數據處理邏輯。
    """

    def render_page_header(self):
        """渲染頁面標題和說明。"""
        st.header("招聘效率分析")
        st.markdown("分析不同職位的招聘效率，包括應徵人數、在架時間等指標。")

    def render_filter_info(self, keywords, city, district, months):
        """渲染過濾條件信息。"""
        display_filter_info(keywords, city, district, months)

    def render_data_loading_status(self):
        """渲染數據載入狀態區塊。"""
        st.subheader("數據載入狀態")

    def render_empty_data_warning(self):
        """渲染數據為空的警告。"""
        st.warning("數據庫中沒有符合條件的職缺數據。請調整篩選條件或先爬取更多數據。")

    def render_jobs_count(self, count: int):
        """渲染職缺數量。"""
        st.write(f"找到 {count} 個符合條件的職缺")

    def render_missing_columns_info(self, missing_cols: List[str]):
        """渲染缺少列的信息。"""
        if missing_cols:
            st.info(
                f"職缺數據中缺少以下列: {', '.join(missing_cols)}，無法進行完整分析"
            )

    def render_analysis_charts(self, job_data_analyzer, jobs_analysis: pd.DataFrame):
        """渲染分析圖表。"""
        # 顯示職缺在架時間分布
        display_job_duration_distribution(jobs_analysis)

        # 顯示工作經驗與應徵人數關係
        display_experience_application_relationship(jobs_analysis)

        # 顯示職缺競爭度分布
        display_job_competition_distribution(jobs_analysis)

        # 顯示教育程度與薪資範圍關係
        display_education_salary_relationship(jobs_analysis)

        # 顯示長期未招滿職缺分析
        display_hiring_efficiency_long_unfilled_jobs(job_data_analyzer, jobs_analysis)

        # 顯示剛開始招募的職缺列表
        display_hiring_efficiency_recent_jobs(job_data_analyzer, jobs_analysis)

    def render_error_message(self, error_msg: str):
        """渲染錯誤信息。"""
        st.error(f"分析招聘效率時發生錯誤: {error_msg}")
        st.info("無法分析招聘效率，請確保數據格式正確。")


def show_hiring_efficiency_page(
    job_data_analyzer,
    keywords=None,
    city=None,
    district=None,
    limit=MAX_JOBS_LIMIT,
    months=None,
):
    """
    顯示招聘效率分析頁面，分析不同職位的招聘效率。

    此函數作為頁面控制器，協調數據處理和UI渲染。

    Args:
        job_data_analyzer: JobDataAnalyzer instance for data processing
        keywords: 用於過濾職缺的關鍵詞列表，默認為None
        city: 用於過濾職缺的城市，默認為None
        district: 用於過濾職缺的地區，默認為None
        limit: 最大獲取職缺數量，默認為10000
        months: 如果提供，只獲取最近N個月的職缺，默認為None
    """
    # 記錄頁面載入開始
    logger.info("顯示招聘效率分析頁面")

    # 初始化數據處理器和頁面渲染器
    data_processor = HiringEfficiencyDataProcessor(job_data_analyzer)
    page_renderer = HiringEfficiencyPageRenderer()

    # 渲染頁面標題和過濾條件
    page_renderer.render_page_header()
    page_renderer.render_filter_info(keywords, city, district, months)

    try:
        # 渲染數據載入狀態
        page_renderer.render_data_loading_status()

        # 載入職缺數據
        jobs_df = data_processor.load_jobs_data(keywords, city, district, limit, months)

        # 檢查數據是否為空
        if jobs_df.empty:
            logger.warning("數據庫中沒有符合條件的職缺數據")
            page_renderer.render_empty_data_warning()
            return

        # 渲染職缺數量
        page_renderer.render_jobs_count(len(jobs_df))

        # 檢查必要的列
        missing_cols = data_processor.check_required_columns(jobs_df)
        page_renderer.render_missing_columns_info(missing_cols)

        # 準備分析數據
        jobs_analysis = data_processor.prepare_analysis_data(jobs_df)

        # 處理應徵人數數據
        jobs_analysis = data_processor.process_application_data(jobs_analysis)

        # 渲染分析圖表
        page_renderer.render_analysis_charts(job_data_analyzer, jobs_analysis)

    except Exception as e:
        # 記錄錯誤信息
        logger.error(f"顯示招聘效率分析頁面時發生錯誤: {str(e)}", exc_info=True)
        page_renderer.render_error_message(str(e))
