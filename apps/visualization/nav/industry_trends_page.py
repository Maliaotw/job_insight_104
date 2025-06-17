"""
Industry trends page for the 104 Job Insight visualization app.
This page analyzes the distribution and trends of jobs across different industries.

This module follows the Single Responsibility Principle by separating:
1. Data processing (IndustryDataProcessor)
2. UI rendering (IndustryTrendsPageRenderer)
3. Page coordination (IndustryTrendsPage)

Author: Job Insight 104 Team
Version: 2.0.0
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional, Dict, List, Any

from apps.visualization.components.filter_info import display_filter_info
from config.settings import logger


# Component functions integrated directly into this file
def display_industry_distribution_chart(industry_dist):
    """
    顯示產業職缺分佈圖表

    參數:
        industry_dist: 產業分佈數據DataFrame
    """
    # 記錄顯示產業分佈圖表
    logger.debug("顯示產業職缺分佈圖表區塊")
    st.subheader("產業職缺分佈")
    logger.info("創建產業職缺分佈圖表")

    # 創建條形圖
    fig = px.bar(
        industry_dist, 
        x='產業類別', 
        y='職缺數',
        title='產業職缺分佈',
        color='產業類別'
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)
    logger.info("產業職缺分佈圖表顯示完成")


def display_industry_distribution_details(industry_dist):
    """
    顯示產業職缺分佈詳情表格

    參數:
        industry_dist: 產業分佈數據DataFrame
    """
    # 記錄顯示產業分佈詳情
    logger.debug("顯示產業職缺分佈詳情區塊")
    st.subheader("產業職缺分佈詳情")
    st.dataframe(industry_dist[['產業類別', '職缺數', '公司數']], use_container_width=True)
    logger.info("產業職缺分佈詳情表格顯示完成")


def display_industry_trends_chart(monthly_industry_stats):
    """
    顯示產業月度職缺趨勢圖表

    參數:
        monthly_industry_stats: 產業月度統計數據DataFrame
    """
    # 記錄顯示產業趨勢圖表
    logger.debug("顯示產業月度職缺趨勢圖表區塊")
    st.subheader("產業月度職缺趨勢")
    logger.info("創建產業月度職缺趨勢圖表")

    # 獲取月份列
    month_cols = [col for col in monthly_industry_stats.columns 
                 if col not in ['產業類別', '增長量', '增長率']]
    logger.debug(f"月份列: {month_cols}")

    # 允許用戶選擇要顯示的產業
    default_industries = monthly_industry_stats.sort_values(month_cols[-1], ascending=False)['產業類別'].head(5).tolist()
    logger.debug(f"默認顯示的產業: {default_industries}")

    selected_industries = st.multiselect(
        "選擇要顯示的產業",
        options=monthly_industry_stats['產業類別'].tolist(),
        default=default_industries
    )
    logger.debug(f"用戶選擇的產業: {selected_industries}")

    if not selected_industries:
        logger.warning("用戶未選擇任何產業")
        st.info("請選擇至少一個產業來顯示趨勢")
        return

    # 過濾選定的產業數據
    filtered_data = monthly_industry_stats[monthly_industry_stats['產業類別'].isin(selected_industries)]
    logger.debug(f"過濾後的產業數據: {len(filtered_data)} 條")

    # 創建趨勢線圖
    create_industry_trend_line_chart(filtered_data, month_cols)


def create_industry_trend_line_chart(filtered_data, month_cols):
    """
    創建產業趨勢線圖

    參數:
        filtered_data: 過濾後的產業數據DataFrame
        month_cols: 月份列名列表
    """
    # 記錄創建趨勢線圖
    logger.debug("創建產業趨勢線圖")
    fig = go.Figure()
    for _, industry in filtered_data.iterrows():
        fig.add_trace(go.Scatter(
            x=month_cols,
            y=industry[month_cols].values,
            name=industry['產業類別'],
            mode='lines+markers'
        ))

    fig.update_layout(
        title='產業月度職缺趨勢',
        xaxis_title='月份',
        yaxis_title='職缺數量',
        hovermode="x unified"
    )
    logger.debug("產業月度職缺趨勢圖表配置完成")

    st.plotly_chart(fig, use_container_width=True)
    logger.info("產業月度職缺趨勢圖表顯示完成")


def display_industry_growth_table(filtered_data):
    """
    顯示產業職缺增長情況表格

    參數:
        filtered_data: 過濾後的產業數據DataFrame
    """
    # 記錄顯示產業增長情況
    logger.debug("顯示產業職缺增長情況區塊")
    st.subheader("產業職缺增長情況")
    logger.info("顯示產業職缺增長情況")

    # 創建增長表格
    growth_data = filtered_data[['產業類別', '增長量', '增長率']].sort_values('增長量', ascending=False)
    st.dataframe(growth_data, use_container_width=True)
    logger.info("產業職缺增長情況表格顯示完成")


# Constants
DEFAULT_JOB_LIMIT = 10000
INDUSTRY_COLUMN = 'coIndustryDesc'
DATE_COLUMN = 'appearDate'


class IndustryDataProcessor:
    """
    負責處理產業數據的類別，將數據處理邏輯與UI渲染分離。

    此類別遵循單一職責原則(SRP)，專注於數據處理相關操作。
    """

    def __init__(self, job_data_analyzer, trend_analyzer):
        """
        初始化產業數據處理器

        Args:
            job_data_analyzer: 職缺數據分析器實例
            trend_analyzer: 趨勢分析器實例
        """
        self.job_data_analyzer = job_data_analyzer
        self.trend_analyzer = trend_analyzer

    def load_job_data(self, keywords=None, city=None, district=None, 
                     limit=DEFAULT_JOB_LIMIT, months=None) -> Optional[pd.DataFrame]:
        """
        從數據庫載入職缺數據

        Args:
            keywords: 關鍵詞列表
            city: 城市名稱
            district: 地區名稱
            limit: 最大獲取職缺數量
            months: 月份數量

        Returns:
            職缺數據DataFrame，如果沒有數據則返回None
        """
        logger.info("從數據庫獲取職缺數據")
        jobs_df = self.job_data_analyzer.get_jobs(
            limit=limit, 
            months=months, 
            keywords=keywords, 
            city=city, 
            district=district
        )
        logger.debug(f"獲取到 {len(jobs_df)} 條職缺數據")

        if jobs_df.empty:
            logger.warning("數據庫中沒有符合條件的職缺數據")
            return None

        return jobs_df

    def analyze_industry_distribution(self, jobs_df: pd.DataFrame) -> pd.DataFrame:
        """
        分析產業職缺分佈

        Args:
            jobs_df: 職缺數據DataFrame

        Returns:
            產業分佈數據DataFrame
        """
        logger.info("分析產業職缺分佈")
        industry_dist = self.trend_analyzer.analyze_industry_distribution(jobs_df)
        logger.debug(f"獲取到 {len(industry_dist) if not industry_dist.empty else 0} 條產業分佈數據")
        return industry_dist

    def analyze_industry_trends(self, jobs_df: pd.DataFrame) -> pd.DataFrame:
        """
        分析產業月度職缺趨勢

        Args:
            jobs_df: 職缺數據DataFrame

        Returns:
            產業月度趨勢數據DataFrame
        """
        logger.info("分析產業月度職缺趨勢")
        monthly_industry_stats = self.trend_analyzer.analyze_industry_trends(jobs_df)
        logger.debug(f"獲取到 {len(monthly_industry_stats) if not monthly_industry_stats.empty else 0} 條產業趨勢數據")
        return monthly_industry_stats

    def has_required_columns(self, jobs_df: pd.DataFrame) -> Dict[str, bool]:
        """
        檢查數據框是否包含所需的列

        Args:
            jobs_df: 職缺數據DataFrame

        Returns:
            包含列檢查結果的字典
        """
        return {
            'industry': INDUSTRY_COLUMN in jobs_df.columns,
            'date': DATE_COLUMN in jobs_df.columns
        }


class IndustryTrendsPageRenderer:
    """
    負責渲染產業趨勢頁面UI的類別，將UI渲染邏輯與數據處理分離。

    此類別遵循單一職責原則(SRP)，專注於UI渲染相關操作。
    """

    def render_page_header(self):
        """渲染頁面標題"""
        st.header("產業職缺分佈與趨勢")
        st.markdown("分析不同產業的職缺分佈情況和隨時間的變化趨勢。")

    def render_filter_info(self, keywords, city, district, months):
        """渲染過濾條件信息"""
        display_filter_info(keywords, city, district, months)

    def render_data_loading_status(self, jobs_count: int):
        """
        渲染數據載入狀態

        Args:
            jobs_count: 職缺數量
        """
        st.subheader("數據載入狀態")
        st.write(f"找到 {jobs_count} 個符合條件的職缺")

    def render_no_data_warning(self):
        """渲染無數據警告"""
        st.warning("數據庫中沒有符合條件的職缺數據。請調整篩選條件或先爬取更多數據。")

    def render_missing_column_info(self, column_type: str):
        """
        渲染缺少列信息

        Args:
            column_type: 列類型 ('industry' 或 'date')
        """
        message = "職缺數據中沒有產業類別信息" if column_type == 'industry' else "職缺數據中沒有日期信息，無法分析產業趨勢"
        st.info(message)

    def render_analysis_error(self, error_message: str):
        """
        渲染分析錯誤信息

        Args:
            error_message: 錯誤信息
        """
        st.error(f"分析產業職缺分佈與趨勢時發生錯誤: {error_message}")
        st.info("無法分析產業職缺分佈與趨勢，請確保數據格式正確。")

    def render_empty_analysis_info(self, analysis_type: str):
        """
        渲染空分析信息

        Args:
            analysis_type: 分析類型 ('distribution' 或 'trends')
        """
        message = "無法分析產業分佈，可能是數據不足" if analysis_type == 'distribution' else "無法分析產業趨勢，可能是數據不足"
        st.info(message)

    def render_industry_distribution(self, industry_dist: pd.DataFrame):
        """
        渲染產業分佈

        Args:
            industry_dist: 產業分佈數據DataFrame
        """
        display_industry_distribution_chart(industry_dist)
        display_industry_distribution_details(industry_dist)

    def render_industry_trends(self, monthly_industry_stats: pd.DataFrame):
        """
        渲染產業趨勢

        Args:
            monthly_industry_stats: 產業月度趨勢數據DataFrame
        """
        display_industry_trends_chart(monthly_industry_stats)

        # 可以在這裡添加產業增長表格的渲染
        # display_industry_growth_table(monthly_industry_stats)


class IndustryTrendsPage:
    """
    產業趨勢頁面類別，協調數據處理和UI渲染。

    此類別遵循開放封閉原則(OCP)，可以通過擴展而不修改來添加新功能。
    """

    def __init__(self, job_data_analyzer, trend_analyzer):
        """
        初始化產業趨勢頁面

        Args:
            job_data_analyzer: 職缺數據分析器實例
            trend_analyzer: 趨勢分析器實例
        """
        self.data_processor = IndustryDataProcessor(job_data_analyzer, trend_analyzer)
        self.renderer = IndustryTrendsPageRenderer()

    def show(self, keywords=None, city=None, district=None, limit=DEFAULT_JOB_LIMIT, months=None):
        """
        顯示產業職缺分佈與趨勢頁面

        Args:
            keywords: 用於過濾職缺的關鍵詞列表
            city: 用於過濾職缺的城市
            district: 用於過濾職缺的地區
            limit: 最大獲取職缺數量
            months: 如果提供，只獲取最近N個月的職缺
        """
        logger.info("顯示產業職缺分佈與趨勢頁面")

        # 渲染頁面標題
        self.renderer.render_page_header()

        # 顯示過濾條件信息
        self.renderer.render_filter_info(keywords, city, district, months)

        try:
            # 載入數據
            jobs_df = self.data_processor.load_job_data(keywords, city, district, limit, months)
            if jobs_df is None:
                self.renderer.render_no_data_warning()
                return

            # 顯示數據載入狀態
            self.renderer.render_data_loading_status(len(jobs_df))

            # 檢查所需列
            columns_check = self.data_processor.has_required_columns(jobs_df)

            # 處理產業分佈
            self._process_industry_distribution(jobs_df, columns_check)

        except Exception as e:
            # 記錄錯誤信息
            logger.error(f"顯示產業職缺分佈與趨勢頁面時發生錯誤: {str(e)}", exc_info=True)
            self.renderer.render_analysis_error(str(e))

    def _process_industry_distribution(self, jobs_df: pd.DataFrame, columns_check: Dict[str, bool]):
        """
        處理產業分佈分析和渲染

        Args:
            jobs_df: 職缺數據DataFrame
            columns_check: 列檢查結果
        """
        if not columns_check['industry']:
            logger.warning("職缺數據中沒有產業類別信息")
            self.renderer.render_missing_column_info('industry')
            return

        # 分析產業分佈
        industry_dist = self.data_processor.analyze_industry_distribution(jobs_df)

        if industry_dist.empty:
            logger.warning("無法分析產業分佈，可能是數據不足")
            self.renderer.render_empty_analysis_info('distribution')
            return

        # 渲染產業分佈
        self.renderer.render_industry_distribution(industry_dist)

        # 處理產業趨勢
        self._process_industry_trends(jobs_df, columns_check)

    def _process_industry_trends(self, jobs_df: pd.DataFrame, columns_check: Dict[str, bool]):
        """
        處理產業趨勢分析和渲染

        Args:
            jobs_df: 職缺數據DataFrame
            columns_check: 列檢查結果
        """
        if not columns_check['date']:
            logger.warning("職缺數據中沒有日期信息")
            self.renderer.render_missing_column_info('date')
            return

        # 分析產業趨勢
        monthly_industry_stats = self.data_processor.analyze_industry_trends(jobs_df)

        if monthly_industry_stats.empty:
            logger.warning("無法分析產業趨勢，可能是數據不足")
            self.renderer.render_empty_analysis_info('trends')
            return

        # 渲染產業趨勢
        self.renderer.render_industry_trends(monthly_industry_stats)


def show_industry_trends_page(job_data_analyzer, trend_analyzer, keywords=None, city=None, district=None, limit=DEFAULT_JOB_LIMIT, months=None):
    """
    顯示產業職缺分佈與趨勢頁面，分析不同產業的職缺分佈和變化趨勢。

    此函數是為了保持向後兼容性，新代碼應直接使用IndustryTrendsPage類。

    Args:
        job_data_analyzer: JobDataAnalyzer instance for data processing
        trend_analyzer: TrendAnalyzer instance for trend analysis
        keywords: 用於過濾職缺的關鍵詞列表，默認為None
        city: 用於過濾職缺的城市，默認為None
        district: 用於過濾職缺的地區，默認為None
        limit: 最大獲取職缺數量，默認為10000
        months: 如果提供，只獲取最近N個月的職缺，默認為None
    """
    page = IndustryTrendsPage(job_data_analyzer, trend_analyzer)
    page.show(keywords, city, district, limit, months)
