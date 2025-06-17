"""
104職缺市場洞察平台的儀表板頁面。
此頁面提供職缺市場概況的總覽，包含關鍵指標和視覺化圖表。
"""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from apps.visualization.analysis.job_data_analyzer import JobDataAnalyzer
from apps.visualization.analysis.trend_analyzer import TrendAnalyzer
from apps.visualization.components import (
    display_filter_info
)
from config.settings import logger

# 常數定義
DATE_RANGE_OPTIONS = ["全部時間", "最近7天", "最近30天", "最近90天"]
DEFAULT_LIMIT = 10000
CHART_DATA_OPTIONS = ["新增職缺", "減少職缺", "淨變化", "累計變化"]
DEFAULT_CHART_OPTIONS = ["新增職缺", "減少職缺", "淨變化"]
COLOR_MAP = {
    '新增職缺': 'green',
    '減少職缺': 'red',
    '淨變化': 'blue',
    '累計變化': 'purple',
    '下架職缺': 'orange'
}
DASH_MAP = {
    '新增職缺': None,
    '減少職缺': None,
    '淨變化': 'dash',
    '累計變化': 'dot',
    '下架職缺': 'dashdot'
}
COLUMN_MAPPING = {
    'new_jobs': '新增職缺',
    'removed_jobs': '減少職缺',
    'date': '日期'
}
# 圖表常數
CHART_TITLE = "每日職缺變化趨勢"
CHART_XAXIS_TITLE = "日期"
CHART_YAXIS_TITLE = "職缺數量"
CHART_LEGEND_TITLE = "數據類型"
# 錯誤訊息常數
ERROR_MISSING_COLUMN = "圖表數據缺少必要的列"
ERROR_CHART_CREATION = "無法顯示職缺趨勢 - 數據格式不正確"
ERROR_CHART_EXCEPTION = "無法顯示職缺趨勢 - 發生錯誤"
# 表格顯示常數
TABLE_CHECKBOX_LABEL = "顯示詳細數據表格"





class DashboardDataProcessor:
    """
    儀表板數據處理器類，負責處理數據載入和處理。

    此類封裝了與數據相關的操作，包括從數據庫獲取數據、過濾數據等。
    遵循單一職責原則，專注於數據處理邏輯，不包含UI渲染邏輯。
    """

    def __init__(self, job_data_analyzer: JobDataAnalyzer):
        """
        初始化儀表板數據處理器。

        參數:
            job_data_analyzer: JobDataAnalyzer實例，用於數據處理
        """
        self.job_data_analyzer = job_data_analyzer
        self.db_manager = job_data_analyzer.db_manager

    def load_job_data(self, keywords=None, city=None, district=None, limit=DEFAULT_LIMIT, months=None):
        """
        從數據庫載入職缺數據和每日統計數據。

        參數:
            keywords: 關鍵詞列表，默認為None
            city: 城市名稱，默認為None
            district: 地區名稱，默認為None
            limit: 最大獲取職缺數量，默認為DEFAULT_LIMIT
            months: 月份數量，默認為None

        返回:
            Tuple[pd.DataFrame, pd.DataFrame]: (職缺數據DataFrame, 每日統計數據DataFrame)
            如果沒有數據，返回(None, None)
        """
        # 獲取所有帶過濾條件的職缺
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
            return None, None

        # 從數據庫獲取每日統計數據
        logger.info("獲取每日統計數據")


        trend_analyzer = TrendAnalyzer()

        # 1. 從數據庫獲取職缺數據
        logger.info("從數據庫獲取職缺數據")

        if jobs_df.empty:
            logger.warning("數據庫中沒有職缺數據，無法計算每日統計")
            return

        logger.info(f"獲取到 {len(jobs_df)} 筆職缺數據")

        # 2. 計算每日職缺變化
        logger.info("計算每日職缺變化")
        stats_df = trend_analyzer.create_job_trend_chart(jobs_df)


        return jobs_df, stats_df

    def load_inactive_jobs(self, keywords=None, city=None, district=None, limit=DEFAULT_LIMIT, months=None):
        """
        載入包括已下架的職缺數據。

        參數:
            keywords: 關鍵詞列表，默認為None
            city: 城市名稱，默認為None
            district: 地區名稱，默認為None
            limit: 最大獲取職缺數量，默認為DEFAULT_LIMIT
            months: 月份數量，默認為None

        返回:
            pd.DataFrame: 包括已下架職缺的DataFrame
        """
        logger.info("獲取包括已下架的職缺數據")
        return self.job_data_analyzer.get_jobs(
            limit=limit, 
            months=months, 
            keywords=keywords, 
            city=city, 
            district=district, 
            include_inactive=True
        )

    def get_delisted_jobs_data(self, start_date, end_date):
        """
        獲取指定日期範圍內的下架職缺數據。

        參數:
            start_date: 開始日期
            end_date: 結束日期

        返回:
            pd.DataFrame: 下架職缺數據DataFrame
        """
        try:
            # 獲取包括已下架職缺的數據
            all_jobs_df = self.job_data_analyzer.get_jobs(limit=DEFAULT_LIMIT, include_inactive=True)

            # 過濾出已下架的職缺
            if 'status' in all_jobs_df.columns and 'delisted_date' in all_jobs_df.columns:
                delisted_jobs = all_jobs_df[(all_jobs_df['status'] == 'inactive') & 
                                            (all_jobs_df['delisted_date'].notna())]

                # 將下架日期轉換為日期格式
                delisted_jobs['delisted_date'] = pd.to_datetime(delisted_jobs['delisted_date'])

                # 按下架日期分組統計
                delisted_by_date = delisted_jobs.groupby('delisted_date').size().reset_index()
                delisted_by_date.columns = ['date', 'delisted_count']

                # 過濾指定日期範圍
                delisted_by_date = delisted_by_date[(delisted_by_date['date'] >= start_date) & 
                                                   (delisted_by_date['date'] <= end_date)]

                return delisted_by_date
        except Exception as e:
            logger.warning(f"獲取下架職缺數據時發生錯誤: {str(e)}")
            raise ValueError(f"獲取下架職缺數據失敗: {str(e)}")

        return pd.DataFrame()

    def prepare_jobs_analysis(self, jobs_df):
        """
        準備職缺分析數據。

        參數:
            jobs_df: 職缺數據DataFrame

        返回:
            pd.DataFrame: 準備好的職缺分析數據
        """
        return self.job_data_analyzer.prepare_jobs_analysis_df(jobs_df)

    def filter_data_by_date_range(self, data, date_range):
        """
        根據選擇的時間範圍過濾數據。

        參數:
            data: 數據DataFrame
            date_range: 時間範圍字符串

        返回:
            pd.DataFrame: 過濾後的數據
        """
        if date_range == "全部時間":
            return data

        end_date = data['date'].max()
        if date_range == "最近7天":
            start_date = end_date - timedelta(days=7)
        elif date_range == "最近30天":
            start_date = end_date - timedelta(days=30)
        elif date_range == "最近90天":
            start_date = end_date - timedelta(days=90)
        else:
            return data

        return data[data['date'] >= start_date]

    def prepare_chart_data(self, filtered_stats, include_delisted=False):
        """
        準備圖表數據，包括計算淨變化和累計變化。

        參數:
            filtered_stats: 過濾後的統計數據DataFrame
            include_delisted: 是否包含下架職缺數據

        返回:
            pd.DataFrame: 準備好的圖表數據
        """
        # 計算淨變化和累計變化
        filtered_stats = filtered_stats.copy()
        filtered_stats['淨變化'] = filtered_stats['new_jobs'] - filtered_stats['removed_jobs']
        filtered_stats['累計變化'] = filtered_stats['淨變化'].cumsum()

        # 如果需要包含下架職缺數據
        if include_delisted and 'delisted_count' in filtered_stats.columns:
            filtered_stats['下架職缺'] = filtered_stats['delisted_count'].fillna(0)

        # 重命名列以便於圖表顯示
        return filtered_stats.rename(columns=COLUMN_MAPPING)

    def has_required_columns(self, df, required_columns):
        """
        檢查DataFrame是否包含所需的列。

        參數:
            df: 要檢查的DataFrame
            required_columns: 所需列的列表

        返回:
            bool: 如果包含所有所需列，則為True，否則為False
        """
        return all(col in df.columns for col in required_columns)


class DashboardPageRenderer:
    """
    儀表板頁面渲染器類，負責處理UI渲染相關的操作。

    此類封裝了與UI渲染相關的操作，包括頁面標題、過濾信息、數據載入狀態等。
    遵循單一職責原則，專注於UI渲染邏輯，不包含數據處理邏輯。
    """

    def render_page_header(self):
        """
        渲染頁面標題和描述。
        """
        logger.info("顯示總覽Dashboard頁面")
        st.header("總覽 Dashboard - 市場概況")
        st.markdown("快速了解當前職缺市場的整體狀況與近期變化趨勢。")

    def render_filter_info(self, keywords, city, district, months):
        """
        渲染過濾條件信息。

        參數:
            keywords: 關鍵詞列表
            city: 城市名稱
            district: 地區名稱
            months: 月份數量
        """
        display_filter_info(keywords, city, district, months)

    def render_data_loading_status(self, jobs_count: int):
        """
        渲染數據載入狀態。

        參數:
            jobs_count: 職缺數量
        """
        logger.debug("顯示數據載入狀態區塊")
        st.subheader("數據載入狀態")

        if jobs_count > 0:
            logger.info(f"找到 {jobs_count} 個符合條件的職缺")
            st.write(f"找到 {jobs_count} 個符合條件的職缺")
        else:
            logger.warning("數據庫中沒有符合條件的職缺數據")
            st.warning("數據庫中沒有符合條件的職缺數據。請調整篩選條件或先爬取更多數據。")

    def render_no_data_warning(self):
        """
        渲染無數據警告。
        """
        logger.warning("無每日職缺變化數據或數據格式不正確")
        st.info("暫無每日職缺變化數據。請先運行爬蟲或導入數據。")

    def render_analysis_error(self, error_message: str):
        """
        渲染分析錯誤信息。

        參數:
            error_message: 錯誤信息
        """
        logger.error(f"分析數據時發生錯誤: {error_message}")
        st.error(f"分析數據時發生錯誤: {error_message}")

    def render_data_summary(self, data):
        """
        渲染數據摘要統計。

        參數:
            data: 數據DataFrame
        """
        if len(data) > 0:
            total_new = data['new_jobs'].sum()
            total_removed = data['removed_jobs'].sum()
            net_change = total_new - total_removed

            st.markdown(f"""
            **數據摘要** ({data['date'].min().strftime('%Y-%m-%d')} 至 {data['date'].max().strftime('%Y-%m-%d')})
            - 總新增職缺: **{total_new:,}**
            - 總減少職缺: **{total_removed:,}**
            - 淨變化: **{net_change:,}** ({'增加' if net_change >= 0 else '減少'})
            """)

    def render_detailed_data_table(self, chart_data):
        """
        渲染詳細數據表格。

        參數:
            chart_data: 圖表數據DataFrame
        """
        # 準備顯示數據
        display_df = chart_data.copy()

        # 格式化日期
        if isinstance(display_df['日期'].iloc[0], pd.Timestamp):
            display_df['日期'] = display_df['日期'].dt.strftime('%Y-%m-%d')

        # 顯示表格
        st.dataframe(display_df, use_container_width=True)

    def create_job_trend_chart(self, chart_data, show_options):
        """
        創建職缺趨勢圖表。

        參數:
            chart_data: 圖表數據DataFrame
            show_options: 要顯示的數據選項列表

        返回:
            go.Figure: Plotly圖表對象
        """
        # 記錄創建圖表
        logger.debug("創建職缺趨勢圖表")

        # 檢查必要的列是否存在
        required_columns = ['日期']

        # 檢查必要的列是否存在
        for col in required_columns:
            if col not in chart_data.columns:
                logger.error(f"{ERROR_MISSING_COLUMN}: {col}")
                # 創建一個空的圖表
                fig = go.Figure()
                fig.update_layout(
                    title=ERROR_CHART_CREATION,
                    annotations=[dict(
                        text=f'{ERROR_MISSING_COLUMN}: {col}',
                        showarrow=False,
                        xref="paper", yref="paper",
                        x=0.5, y=0.5
                    )]
                )
                return fig

        # 檢查顯示選項中的列是否存在
        for option in show_options:
            if option not in chart_data.columns:
                logger.error(f"{ERROR_MISSING_COLUMN}: {option}")
                # 創建一個空的圖表
                fig = go.Figure()
                fig.update_layout(
                    title=ERROR_CHART_CREATION,
                    annotations=[dict(
                        text=f'{ERROR_MISSING_COLUMN}: {option}',
                        showarrow=False,
                        xref="paper", yref="paper",
                        x=0.5, y=0.5
                    )]
                )
                return fig

        try:
            fig = go.Figure()

            # 添加各種數據線
            for option in show_options:
                if option in chart_data.columns:
                    fig.add_trace(go.Scatter(
                        x=chart_data['日期'], 
                        y=chart_data[option],
                        name=option,
                        line=dict(
                            color=COLOR_MAP.get(option, 'gray'), 
                            width=2,
                            dash=DASH_MAP.get(option, None)
                        ),
                        mode='lines+markers' if len(chart_data) < 30 else 'lines'
                    ))

            # 添加零線（對於淨變化）
            if '淨變化' in show_options:
                fig.add_shape(
                    type="line",
                    x0=chart_data['日期'].min(),
                    y0=0,
                    x1=chart_data['日期'].max(),
                    y1=0,
                    line=dict(color="gray", width=1, dash="dot"),
                )

            # 更新圖表布局
            fig.update_layout(
                title=CHART_TITLE,
                xaxis_title=CHART_XAXIS_TITLE,
                yaxis_title=CHART_YAXIS_TITLE,
                legend_title=CHART_LEGEND_TITLE,
                hovermode='x unified',
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                margin=dict(l=20, r=20, t=60, b=20),
                plot_bgcolor='rgba(240,240,240,0.2)'
            )

            # 優化X軸日期顯示
            fig.update_xaxes(
                rangeslider_visible=False,
                rangeselector=dict(
                    buttons=list([
                        dict(count=7, label="1週", step="day", stepmode="backward"),
                        dict(count=1, label="1月", step="month", stepmode="backward"),
                        dict(count=3, label="3月", step="month", stepmode="backward"),
                        dict(step="all", label="全部")
                    ])
                )
            )

            return fig
        except Exception as e:
            logger.error(f"創建圖表時發生錯誤: {str(e)}")
            # 創建一個空的圖表
            fig = go.Figure()
            fig.update_layout(
                title=ERROR_CHART_EXCEPTION,
                annotations=[dict(
                    text=f'創建圖表時發生錯誤: {str(e)}',
                    showarrow=False,
                    xref="paper", yref="paper",
                    x=0.5, y=0.5
                )]
            )
            return fig

    def render_daily_job_trend(self, daily_stats, data_processor):
        """
        渲染每日職缺變化趨勢圖表。

        參數:
            daily_stats: 每日統計數據DataFrame
            data_processor: DashboardDataProcessor實例，用於數據處理
        """
        # 記錄顯示趨勢圖表
        logger.debug("顯示每日職缺變化趨勢區塊")
        st.subheader("每日職缺變化趨勢")

        # 檢查數據是否為空以及是否包含所需的列
        required_columns = ['date', 'new_jobs', 'removed_jobs']
        if not daily_stats.empty and data_processor.has_required_columns(daily_stats, required_columns):
            try:
                # 創建圖表控制選項
                col1, col2 = st.columns([3, 1])
                with col2:
                    # 添加時間範圍選擇器
                    if len(daily_stats) > 7:
                        date_range = st.selectbox(
                            "選擇時間範圍",
                            DATE_RANGE_OPTIONS,
                            index=0
                        )

                        # 根據選擇過濾數據
                        filtered_stats = data_processor.filter_data_by_date_range(daily_stats, date_range)
                    else:
                        filtered_stats = daily_stats.copy()

                    # 添加數據顯示選項
                    show_options = st.multiselect(
                        "顯示數據",
                        CHART_DATA_OPTIONS,
                        default=DEFAULT_CHART_OPTIONS
                    )

                with col1:
                    # 顯示數據摘要
                    self.render_data_summary(filtered_stats)

                try:
                    # 獲取下架職缺數據
                    delisted_data = data_processor.get_delisted_jobs_data(
                        filtered_stats['date'].min(), 
                        filtered_stats['date'].max()
                    )
                    include_delisted = not delisted_data.empty

                    if include_delisted:
                        # 合併下架數據
                        filtered_stats = pd.merge(filtered_stats, delisted_data, on='date', how='left')
                        # 添加到顯示選項
                        if "下架職缺" not in show_options and len(show_options) > 0:
                            show_options.append("下架職缺")
                except ValueError as e:
                    # 如果獲取下架職缺數據失敗，記錄錯誤但繼續處理
                    logger.warning(f"獲取下架職缺數據失敗: {str(e)}")
                    include_delisted = False

                # 準備圖表數據
                chart_data = data_processor.prepare_chart_data(filtered_stats, include_delisted)
                logger.debug(f"圖表數據準備完成，包含 {len(chart_data)} 個數據點")

                # 創建職缺趨勢圖表
                fig = self.create_job_trend_chart(chart_data, show_options)
                st.plotly_chart(fig, use_container_width=True)

                # 顯示詳細數據表格
                if st.checkbox(TABLE_CHECKBOX_LABEL):
                    self.render_detailed_data_table(chart_data)

            except Exception as e:
                logger.error(f"創建職缺趨勢圖表時發生錯誤: {str(e)}")
                st.error("無法創建職缺趨勢圖表，請檢查數據格式。")
        else:
            self.render_no_data_warning()


class DashboardPage:
    """
    儀表板頁面類，負責協調數據處理和UI渲染。

    此類封裝了儀表板頁面的整體邏輯，協調數據處理器和渲染器的工作。
    遵循開放封閉原則，可以通過擴展而不是修改來添加新功能。
    """

    def __init__(self, job_data_analyzer):
        """
        初始化儀表板頁面。

        參數:
            job_data_analyzer: JobDataAnalyzer實例，用於數據處理
        """
        self.job_data_analyzer = job_data_analyzer
        self.data_processor = DashboardDataProcessor(job_data_analyzer)
        self.page_renderer = DashboardPageRenderer()
        self.job_analysis_processor = JobAnalysisProcessor(job_data_analyzer)
        self.job_analysis_renderer = JobAnalysisRenderer()

    def show(self, keywords=None, city=None, district=None, limit=DEFAULT_LIMIT, months=None):
        """
        顯示儀表板頁面。

        參數:
            keywords: 關鍵詞列表，默認為None
            city: 城市名稱，默認為None
            district: 地區名稱，默認為None
            limit: 最大獲取職缺數量，默認為DEFAULT_LIMIT
            months: 月份數量，默認為None
        """
        # 渲染頁面標題和描述
        self.page_renderer.render_page_header()

        # 渲染過濾條件信息
        self.page_renderer.render_filter_info(keywords, city, district, months)

        try:
            # 載入數據
            jobs_df, daily_stats = self.data_processor.load_job_data(keywords, city, district, limit, months)
            if jobs_df is None:
                self.page_renderer.render_data_loading_status(0)
                return

            # 渲染數據載入狀態
            self.page_renderer.render_data_loading_status(len(jobs_df))

            # 獲取包括已下架的職缺
            inactive_jobs_df = self.data_processor.load_inactive_jobs(keywords, city, district, limit, months)

            # 顯示關鍵指標
            self.display_key_metrics(jobs_df, daily_stats)

            # 渲染每日職缺趨勢圖表
            self.page_renderer.render_daily_job_trend(daily_stats, self.data_processor)

            # 顯示產業分佈
            self.display_industry_distribution(self.job_data_analyzer, jobs_df)

            # 渲染職缺分析
            self._render_job_analysis(jobs_df, inactive_jobs_df)

        except Exception as e:
            # 記錄錯誤信息並渲染錯誤
            logger.error(f"顯示Dashboard頁面時發生錯誤: {str(e)}", exc_info=True)
            self.page_renderer.render_analysis_error(str(e))

    def display_key_metrics(self, jobs_df, daily_stats):
        """
        顯示關鍵指標

        參數:
            jobs_df: 職缺數據DataFrame
            daily_stats: 每日統計數據DataFrame
        """
        # 記錄顯示關鍵指標
        logger.debug("顯示關鍵指標區塊")
        st.subheader("關鍵指標")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("總職缺數", f"{len(jobs_df):,}")

        if not daily_stats.empty and 'date' in daily_stats.columns and not daily_stats['date'].empty:
            try:
                latest_date = daily_stats['date'].max()
                latest_stats = daily_stats[daily_stats['date'] == latest_date]

                if not latest_stats.empty:
                    logger.debug(f"顯示最新日期 {latest_date} 的統計數據")
                    with col2:
                        st.metric("當日新增", f"{latest_stats['new_jobs'].iloc[0]:,}")
                    with col3:
                        st.metric("當日減少", f"{latest_stats['removed_jobs'].iloc[0]:,}")
            except Exception as e:
                logger.warning(f"無法獲取最新日期的統計數據: {str(e)}")

    def display_industry_distribution(self, job_data_analyzer, jobs_df):
        """
        顯示產業職缺分佈

        參數:
            job_data_analyzer: JobDataAnalyzer實例
            jobs_df: 職缺數據DataFrame
        """
        # 記錄顯示產業分佈
        logger.debug("顯示產業職缺分佈區塊")
        st.subheader("產業職缺分佈")
        if 'coIndustryDesc' in jobs_df.columns:
            logger.info("分析產業職缺分佈")
            industry_counts = job_data_analyzer.analyze_industry_distribution(jobs_df)

            if not industry_counts.empty:
                logger.debug(f"獲取到 {len(industry_counts)} 個產業的分佈數據")
                # 顯示頂級產業表格
                st.write("各產業職缺數量排名:")
                st.dataframe(industry_counts.head(10).style.format({
                    '職缺數量': '{:,.0f}',
                    '佔比': '{:.1f}%'
                }))

                # 創建產業分佈餅圖
                logger.debug("創建產業分佈餅圖")
                fig = px.pie(
                    industry_counts.head(10), 
                    values='職缺數量', 
                    names='產業',
                    title='前10大產業職缺分佈',
                    hole=0.4
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
            else:
                logger.warning("產業分佈數據為空")
                st.info("無法分析產業分佈，可能是因為數據中缺少產業信息。")
        else:
            logger.warning("職缺數據中缺少產業信息欄位")
            st.info("職缺數據中缺少產業信息，無法顯示產業分佈。")

    def _render_job_analysis(self, jobs_df, inactive_jobs_df):
        """
        渲染職缺分析部分。

        參數:
            jobs_df: 職缺數據DataFrame
            inactive_jobs_df: 包含已下架職缺的DataFrame
        """
        # 渲染職缺分析標題
        self.job_analysis_renderer.render_job_analysis_header()

        try:
            # 準備職缺分析數據
            active_jobs_analysis, all_jobs_df, all_jobs_analysis, display_cols = (
                self.job_analysis_processor.prepare_job_analysis_data(jobs_df, inactive_jobs_df)
            )

            # 渲染職缺分析
            self.job_analysis_renderer.render_job_analysis(
                self.job_data_analyzer, active_jobs_analysis, all_jobs_analysis, display_cols
            )
        except Exception as e:
            logger.error(f"渲染職缺分析時發生錯誤: {str(e)}", exc_info=True)
            self.page_renderer.render_analysis_error(f"職缺分析失敗: {str(e)}")


class JobAnalysisProcessor:
    """
    職缺分析處理器類，負責處理職缺分析相關的數據處理操作。

    此類封裝了與職缺分析相關的數據處理操作，包括準備分析數據等。
    遵循單一職責原則，專注於數據處理邏輯，不包含UI渲染邏輯。
    """

    def __init__(self, job_data_analyzer):
        """
        初始化職缺分析處理器。

        參數:
            job_data_analyzer: JobDataAnalyzer實例，用於數據處理
        """
        self.job_data_analyzer = job_data_analyzer

    def prepare_job_analysis_data(self, jobs_df, inactive_jobs_df):
        """
        準備職缺分析數據。

        參數:
            jobs_df: 職缺數據DataFrame
            inactive_jobs_df: 包含已下架職缺的DataFrame

        返回:
            Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[str]]: 
            (活躍職缺分析數據, 所有職缺數據, 所有職缺分析數據, 顯示欄位列表)
        """
        logger.info("準備職缺分析數據")

        # 獲取包括活躍和已下架的職缺
        active_jobs_analysis = self.job_data_analyzer.prepare_jobs_analysis_df(jobs_df)

        # 合併活躍和已下架的職缺
        all_jobs_df = pd.concat([jobs_df, inactive_jobs_df[inactive_jobs_df['status'] == 'inactive']])
        all_jobs_analysis = self.job_data_analyzer.prepare_jobs_analysis_df(all_jobs_df)

        # 獲取標準顯示欄位
        display_cols = self.job_data_analyzer.get_job_display_columns(all_jobs_analysis)

        return active_jobs_analysis, all_jobs_df, all_jobs_analysis, display_cols


class JobAnalysisRenderer:
    """
    職缺分析渲染器類，負責渲染職缺分析相關的UI元素。

    此類封裝了與職缺分析相關的UI渲染操作，包括顯示應徵人數分析、長期未招滿職缺等。
    遵循單一職責原則，專注於UI渲染邏輯，不包含數據處理邏輯。
    """

    def render_job_analysis_header(self):
        """
        渲染職缺分析標題。
        """
        logger.debug("顯示職缺分析區塊")
        st.subheader("職缺分析")

    def render_empty_analysis_warning(self):
        """
        渲染空分析警告。
        """
        logger.warning("職缺分析數據為空")
        st.info("無法進行職缺分析，可能是因為數據格式不符合預期。")

    def display_application_analysis(self, job_data_analyzer, jobs_analysis):
        """
        顯示應徵人數分析

        參數:
            job_data_analyzer: JobDataAnalyzer實例
            jobs_analysis: 職缺分析數據DataFrame
        """
        # 記錄顯示應徵人數分析
        logger.debug("檢查是否有應徵人數範圍數據")
        if '應徵人數範圍' in jobs_analysis.columns:
            logger.info("分析應徵人數")
            jobs_analysis = job_data_analyzer.extract_application_counts(jobs_analysis)

            if '平均應徵人數' in jobs_analysis.columns:
                logger.debug("顯示應徵人數分析區塊")
                st.subheader("應徵人數分析")
                # 顯示應徵統計數據
                avg_applications = jobs_analysis['平均應徵人數'].mean()
                max_applications = jobs_analysis['最多應徵人數'].max()
                logger.debug(f"平均應徵人數: {avg_applications:.1f}, 最高應徵人數: {max_applications:.0f}")

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("平均應徵人數", f"{avg_applications:.1f}")
                with col2:
                    st.metric("最高應徵人數", f"{max_applications:.0f}")

                # 創建應徵人數分佈直方圖
                logger.debug("創建應徵人數分佈直方圖")
                fig = px.histogram(
                    jobs_analysis,
                    x='平均應徵人數',
                    nbins=20,
                    title='職缺應徵人數分佈',
                    labels={'平均應徵人數': '應徵人數', 'count': '職缺數量'}
                )
                st.plotly_chart(fig, use_container_width=True)

    def display_long_unfilled_jobs(self, jobs_analysis, display_cols):
        """
        顯示長期未招滿的職缺

        參數:
            jobs_analysis: 職缺分析數據DataFrame
            display_cols: 要顯示的列名列表
        """
        # 記錄顯示長期未招滿職缺
        logger.debug("檢查是否有在架天數數據")
        if '在架天數' in jobs_analysis.columns:
            logger.info("分析長期未招滿職缺")
            long_unfilled = jobs_analysis[jobs_analysis['是否長期未招滿'] == True]

            if not long_unfilled.empty:
                logger.debug(f"找到 {len(long_unfilled)} 個長期未招滿職缺")
                st.subheader("長期未招滿職缺")
                st.write(f"有 {len(long_unfilled)} 個職缺已在架超過30天")

                # 添加顯示數量的進度條
                total_jobs = len(jobs_analysis)
                unfilled_percentage = len(long_unfilled) / total_jobs
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.progress(unfilled_percentage)
                with col2:
                    st.write(f"{len(long_unfilled)}/{total_jobs} ({unfilled_percentage:.1%})")

                # 添加可調整顯示數量的滑桿，無上限可顯示所有筆數
                max_display = len(long_unfilled)  # 設置最大顯示數量為所有長期未招滿職缺
                if max_display > 5:
                    min_value = 5
                else:
                    min_value = max_display
                min_display = min(min_value, max_display)  # 確保min_value不大於max_value
                display_count = st.slider(
                    "顯示職缺數量",
                    min_value=min_display,
                    max_value=max_display,
                    value=min(10, max_display, min_display),  # 預設值為10或最大值或最小值（取適當值）
                    step=min(min_value, max(1, max_display - min_display))  # 確保step不會導致超出範圍
                )

                # 顯示是否顯示全部的選項
                show_all = st.checkbox("顯示所有職缺", value=False)

                # 顯示長期未招滿職缺的範例
                st.write("長期未招滿職缺範例:")

                # 根據是否顯示全部來決定顯示方式
                if show_all:
                    display_data = long_unfilled.sort_values('在架天數', ascending=False)[display_cols]
                else:
                    display_data = long_unfilled.sort_values('在架天數', ascending=False).head(display_count)[display_cols]

                st.dataframe(
                    display_data,
                    column_config={
                        "連結": st.column_config.LinkColumn(
                            "連結",
                            display_text="開啟連結",
                            width="small"
                        )
                    },
                    use_container_width=True
                )
            else:
                logger.debug("沒有找到長期未招滿職缺")

    def display_recent_jobs(self, jobs_analysis, display_cols):
        """
        顯示近期發布的職缺

        參數:
            jobs_analysis: 職缺分析數據DataFrame
            display_cols: 要顯示的列名列表
        """
        # 記錄顯示近期發布職缺
        logger.debug("顯示近期發布的職缺區塊")
        st.subheader("近期發布的職缺")
        new_job = jobs_analysis[jobs_analysis['近期發布的職缺'] == True]
        logger.debug(f"找到 {len(new_job)} 個近期發布的職缺")
        st.write(f"有 {len(new_job)} 近期發布的職缺")

        # 添加顯示數量的進度條
        total_jobs = len(jobs_analysis)
        new_job_percentage = len(new_job) / total_jobs
        col1, col2 = st.columns([3, 1])
        with col1:
            st.progress(new_job_percentage)
        with col2:
            st.write(f"{len(new_job)}/{total_jobs} ({new_job_percentage:.1%})")

        # 添加可調整顯示數量的滑桿，無上限可顯示所有筆數
        max_display = len(new_job)  # 設置最大顯示數量為所有近期發布的職缺
        if max_display > 5:
            min_value = 5
        else:
            min_value = max_display

        min_display = min(min_value, max_display)  # 確保min_value不大於max_value
        display_count = st.slider(
            "顯示職缺數量",
            min_value=min_display,
            max_value=max_display,
            value=min(10, max_display, min_display),  # 預設值為10或最大值或最小值（取適當值）
            step=min(min_value, max(1, max_display - min_display)),  # 確保step不會導致超出範圍
            key="recent_jobs_slider"
        )

        # 顯示是否顯示全部的選項
        show_all = st.checkbox("顯示所有職缺", value=False, key="show_all_recent")

        # 顯示近期發布職缺的範例
        st.write("近期發布的職缺範例:")

        # 根據是否顯示全部來決定顯示方式
        if show_all:
            display_data = new_job.sort_values('在架天數', ascending=False)[display_cols]
        else:
            display_data = new_job.sort_values('在架天數', ascending=False).head(display_count)[display_cols]

        st.dataframe(
            display_data,
            column_config={
                "連結": st.column_config.LinkColumn(
                    "連結",
                    display_text="開啟連結",
                    width="small"
                )
            },
            use_container_width=True
        )

    def display_delisted_jobs_statistics(self,delisted_jobs):
        """
        顯示下架職缺的統計信息

        參數:
            delisted_jobs: 下架職缺數據DataFrame
        """
        # 創建統計信息區塊
        st.write("### 下架職缺統計")

        # 創建多列布局
        col1, col2, col3 = st.columns(3)

        with col1:
            # 總下架職缺數
            st.metric("總下架職缺數", f"{len(delisted_jobs):,}")

        with col2:
            # 平均在架天數
            if '下架前在架天數' in delisted_jobs.columns:
                avg_days = delisted_jobs['下架前在架天數'].mean()
                st.metric("平均在架天數", f"{avg_days:.1f} 天")

        with col3:
            # 最近30天下架數量
            if '下架日期' in delisted_jobs.columns:
                recent_date = datetime.now() - timedelta(days=30)
                recent_delisted = delisted_jobs[delisted_jobs['下架日期'] >= recent_date]
                st.metric("最近30天下架數量", f"{len(recent_delisted):,}")

        # 顯示在架時間分佈
        if '下架前在架天數' in delisted_jobs.columns:
            st.write("#### 下架職缺在架時間分佈")

            # 創建在架時間分類
            duration_bins = [0, 7, 30, 90, float('inf')]
            duration_labels = ['少於7天', '7-30天', '30-90天', '超過90天']

            delisted_jobs['在架時間分類'] = pd.cut(
                delisted_jobs['下架前在架天數'],
                bins=duration_bins,
                labels=duration_labels,
                right=False
            )

            duration_counts = delisted_jobs['在架時間分類'].value_counts().sort_index()

            # 創建圓餅圖
            fig = px.pie(
                values=duration_counts.values,
                names=duration_counts.index,
                title="下架職缺在架時間分佈",
                color_discrete_sequence=px.colors.qualitative.Pastel
            )

            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(margin=dict(t=30, b=0, l=0, r=0))

            st.plotly_chart(fig, use_container_width=True)

    def display_delisted_jobs_trends(self,delisted_jobs):
        """
        顯示下架職缺的趨勢圖表

        參數:
            delisted_jobs: 下架職缺數據DataFrame
        """
        if '下架日期' in delisted_jobs.columns:
            st.write("### 下架職缺趨勢")

            # 按下架日期分組計算每日下架數量
            daily_delisted = delisted_jobs.groupby(delisted_jobs['下架日期'].dt.date).size().reset_index()
            daily_delisted.columns = ['日期', '下架數量']

            # 確保日期是日期時間格式
            daily_delisted['日期'] = pd.to_datetime(daily_delisted['日期'])

            # 創建趨勢圖
            fig = px.line(
                daily_delisted,
                x='日期',
                y='下架數量',
                title="每日下架職缺數量趨勢",
                markers=True
            )

            # 添加7天移動平均線
            if len(daily_delisted) > 7:
                daily_delisted['7天移動平均'] = daily_delisted['下架數量'].rolling(window=7, min_periods=1).mean()
                fig.add_scatter(
                    x=daily_delisted['日期'],
                    y=daily_delisted['7天移動平均'],
                    mode='lines',
                    name='7天移動平均',
                    line=dict(color='red', dash='dash')
                )

            fig.update_layout(
                xaxis_title="日期",
                yaxis_title="下架職缺數量",
                hovermode="x unified"
            )

            st.plotly_chart(fig, use_container_width=True)

            # 顯示按月份分組的下架數量
            if len(daily_delisted) > 30:
                st.write("#### 月度下架職缺數量")

                # 按月份分組
                monthly_delisted = delisted_jobs.groupby(
                    delisted_jobs['下架日期'].dt.to_period('M')).size().reset_index()
                monthly_delisted.columns = ['月份', '下架數量']
                monthly_delisted['月份'] = monthly_delisted['月份'].astype(str)

                # 創建柱狀圖
                fig = px.bar(
                    monthly_delisted,
                    x='月份',
                    y='下架數量',
                    title="月度下架職缺數量",
                    text='下架數量'
                )

                fig.update_layout(
                    xaxis_title="月份",
                    yaxis_title="下架職缺數量"
                )

                st.plotly_chart(fig, use_container_width=True)

    def display_delisted_jobs(self,jobs_analysis, display_cols):
        """
        顯示已經下架的職缺，包含統計數據、圖表和詳細列表

        參數:
            jobs_analysis: 職缺分析數據DataFrame
            display_cols: 要顯示的列名列表
        """
        # 記錄顯示已下架職缺
        logger.debug("檢查是否有下架日期數據")
        if 'status' in jobs_analysis.columns and 'delisted_date' in jobs_analysis.columns:
            logger.info("分析已下架職缺")
            # 過濾出已下架的職缺
            delisted_jobs = jobs_analysis[
                (jobs_analysis['status'] == 'inactive') &
                (jobs_analysis['delisted_date'].notna())
                ]

            # 過濾出活躍的職缺（用於比較）
            active_jobs = jobs_analysis[jobs_analysis['status'] == 'active']

            if not delisted_jobs.empty:
                logger.debug(f"找到 {len(delisted_jobs)} 個已下架職缺")
                st.subheader("已經下架的職缺分析")

                # 將下架日期轉換為日期格式
                if 'delisted_date' in delisted_jobs.columns:
                    delisted_jobs['下架日期'] = pd.to_datetime(delisted_jobs['delisted_date'])

                    # 計算下架前在架天數
                    if '上架日期' in delisted_jobs.columns:
                        delisted_jobs['下架前在架天數'] = (
                                    delisted_jobs['下架日期'] - delisted_jobs['上架日期']).dt.days

                        # 顯示下架職缺統計信息
                        self.display_delisted_jobs_statistics(delisted_jobs)

                        # 顯示下架職缺趨勢圖表
                        self.display_delisted_jobs_trends(delisted_jobs)

                        # 按下架日期排序，顯示最近下架的職缺
                        sort_col = '下架日期'
                    else:
                        sort_col = 'delisted_date'
                else:
                    sort_col = 'jobNo'

                # 添加篩選選項
                st.subheader("下架職缺詳細資料")

                # 創建篩選選項
                col1, col2 = st.columns(2)
                with col1:
                    # 選擇排序方式
                    sort_options = {
                        '最近下架優先': ('下架日期', False) if '下架日期' in delisted_jobs.columns else (sort_col,
                                                                                                         False),
                        '最早下架優先': ('下架日期', True) if '下架日期' in delisted_jobs.columns else (sort_col, True),
                        '在架時間最長優先': ('下架前在架天數',
                                             False) if '下架前在架天數' in delisted_jobs.columns else (sort_col, False),
                        '在架時間最短優先': ('下架前在架天數', True) if '下架前在架天數' in delisted_jobs.columns else (
                            sort_col, True)
                    }

                    sort_by = st.selectbox(
                        "排序方式",
                        options=list(sort_options.keys()),
                        index=0
                    )

                    sort_col, sort_ascending = sort_options[sort_by]

                with col2:
                    # 選擇顯示數量
                    display_count = st.slider("顯示數量", min_value=5, max_value=50, value=10, step=5)

                # 顯示已下架職缺的詳細資料
                st.write(f"### 已下架職缺列表 (共 {len(delisted_jobs)} 個)")

                # 確保display_cols中的所有列都在delisted_jobs中
                valid_cols = [col for col in display_cols if col in delisted_jobs.columns]

                # 如果有下架日期列，添加到顯示列中
                if '下架日期' in delisted_jobs.columns and '下架日期' not in valid_cols:
                    valid_cols.append('下架日期')

                # 如果有下架前在架天數列，添加到顯示列中
                if '下架前在架天數' in delisted_jobs.columns and '下架前在架天數' not in valid_cols:
                    valid_cols.append('下架前在架天數')

                # 排序並顯示數據
                sorted_jobs = delisted_jobs.sort_values(sort_col, ascending=sort_ascending)

                st.dataframe(
                    sorted_jobs.head(display_count)[valid_cols],
                    column_config={
                        "連結": st.column_config.LinkColumn(
                            "連結",
                            display_text="開啟連結",
                            width="small"
                        ),
                        "下架日期": st.column_config.DatetimeColumn(
                            "下架日期",
                            format="YYYY-MM-DD"
                        ),
                        "下架前在架天數": st.column_config.NumberColumn(
                            "下架前在架天數",
                            help="職缺從上架到下架的天數",
                            format="%d 天"
                        )
                    },
                    use_container_width=True
                )

                # 提供下載功能
                if st.button("下載完整下架職缺資料"):
                    csv = sorted_jobs[valid_cols].to_csv(index=False)
                    st.download_button(
                        label="確認下載CSV檔案",
                        data=csv,
                        file_name=f"delisted_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                    )
            else:
                logger.debug("沒有找到已下架職缺")
                st.info("沒有找到已下架的職缺資料")
        else:
            logger.debug("職缺數據中缺少下架日期或狀態信息")
            st.info("職缺數據中缺少下架日期或狀態信息，無法分析已下架職缺")

    def display_delisted_jobs(self,jobs_analysis, display_cols):
        """
        顯示已經下架的職缺，包含統計數據、圖表和詳細列表

        參數:
            jobs_analysis: 職缺分析數據DataFrame
            display_cols: 要顯示的列名列表
        """
        # 記錄顯示已下架職缺
        logger.debug("檢查是否有下架日期數據")
        if 'status' in jobs_analysis.columns and 'delisted_date' in jobs_analysis.columns:
            logger.info("分析已下架職缺")
            # 過濾出已下架的職缺
            delisted_jobs = jobs_analysis[
                (jobs_analysis['status'] == 'inactive') &
                (jobs_analysis['delisted_date'].notna())
                ]

            # 過濾出活躍的職缺（用於比較）
            active_jobs = jobs_analysis[jobs_analysis['status'] == 'active']

            if not delisted_jobs.empty:
                logger.debug(f"找到 {len(delisted_jobs)} 個已下架職缺")
                st.subheader("已經下架的職缺分析")

                # 將下架日期轉換為日期格式
                if 'delisted_date' in delisted_jobs.columns:
                    delisted_jobs['下架日期'] = pd.to_datetime(delisted_jobs['delisted_date'])

                    # 計算下架前在架天數
                    if '上架日期' in delisted_jobs.columns:
                        delisted_jobs['下架前在架天數'] = (
                                    delisted_jobs['下架日期'] - delisted_jobs['上架日期']).dt.days

                        # 顯示下架職缺統計信息
                        self.display_delisted_jobs_statistics(delisted_jobs)

                        # 顯示下架職缺趨勢圖表
                        self.display_delisted_jobs_trends(delisted_jobs)

                        # 按下架日期排序，顯示最近下架的職缺
                        sort_col = '下架日期'
                    else:
                        sort_col = 'delisted_date'
                else:
                    sort_col = 'jobNo'

                # 添加篩選選項
                st.subheader("下架職缺詳細資料")

                # 創建篩選選項
                col1, col2 = st.columns(2)
                with col1:
                    # 選擇排序方式
                    sort_options = {
                        '最近下架優先': ('下架日期', False) if '下架日期' in delisted_jobs.columns else (sort_col,
                                                                                                         False),
                        '最早下架優先': ('下架日期', True) if '下架日期' in delisted_jobs.columns else (sort_col, True),
                        '在架時間最長優先': ('下架前在架天數',
                                             False) if '下架前在架天數' in delisted_jobs.columns else (sort_col, False),
                        '在架時間最短優先': ('下架前在架天數', True) if '下架前在架天數' in delisted_jobs.columns else (
                            sort_col, True)
                    }

                    sort_by = st.selectbox(
                        "排序方式",
                        options=list(sort_options.keys()),
                        index=0
                    )

                    sort_col, sort_ascending = sort_options[sort_by]

                with col2:
                    # 選擇顯示數量
                    display_count = st.slider("顯示數量", min_value=5, max_value=50, value=10, step=5)

                # 顯示已下架職缺的詳細資料
                st.write(f"### 已下架職缺列表 (共 {len(delisted_jobs)} 個)")

                # 確保display_cols中的所有列都在delisted_jobs中
                valid_cols = [col for col in display_cols if col in delisted_jobs.columns]

                # 如果有下架日期列，添加到顯示列中
                if '下架日期' in delisted_jobs.columns and '下架日期' not in valid_cols:
                    valid_cols.append('下架日期')

                # 如果有下架前在架天數列，添加到顯示列中
                if '下架前在架天數' in delisted_jobs.columns and '下架前在架天數' not in valid_cols:
                    valid_cols.append('下架前在架天數')

                # 排序並顯示數據
                sorted_jobs = delisted_jobs.sort_values(sort_col, ascending=sort_ascending)

                st.dataframe(
                    sorted_jobs.head(display_count)[valid_cols],
                    column_config={
                        "連結": st.column_config.LinkColumn(
                            "連結",
                            display_text="開啟連結",
                            width="small"
                        ),
                        "下架日期": st.column_config.DatetimeColumn(
                            "下架日期",
                            format="YYYY-MM-DD"
                        ),
                        "下架前在架天數": st.column_config.NumberColumn(
                            "下架前在架天數",
                            help="職缺從上架到下架的天數",
                            format="%d 天"
                        )
                    },
                    use_container_width=True
                )

                # 提供下載功能
                if st.button("下載完整下架職缺資料"):
                    csv = sorted_jobs[valid_cols].to_csv(index=False)
                    st.download_button(
                        label="確認下載CSV檔案",
                        data=csv,
                        file_name=f"delisted_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                    )
            else:
                logger.debug("沒有找到已下架職缺")
                st.info("沒有找到已下架的職缺資料")
        else:
            logger.debug("職缺數據中缺少下架日期或狀態信息")
            st.info("職缺數據中缺少下架日期或狀態信息，無法分析已下架職缺")

    def render_job_analysis(self, job_data_analyzer, active_jobs_analysis, all_jobs_analysis, display_cols):
        """
        渲染職缺分析。

        參數:
            job_data_analyzer: JobDataAnalyzer實例，用於數據處理
            active_jobs_analysis: 活躍職缺分析數據DataFrame
            all_jobs_analysis: 所有職缺分析數據DataFrame
            display_cols: 顯示欄位列表
        """
        if not active_jobs_analysis.empty:
            logger.debug(f"獲取到 {len(active_jobs_analysis)} 條職缺分析數據")

            # 顯示應徵人數分析
            self.display_application_analysis(job_data_analyzer, active_jobs_analysis)

            # 顯示長期未招滿職缺
            self.display_long_unfilled_jobs(active_jobs_analysis, display_cols)

            # 顯示近期發布的職缺
            self.display_recent_jobs(active_jobs_analysis, display_cols)

            # 顯示已經下架的職缺
            self.display_delisted_jobs(all_jobs_analysis, display_cols)
        else:
            self.render_empty_analysis_warning()

def show_dashboard_page(job_data_analyzer, keywords=None, city=None, district=None, limit=DEFAULT_LIMIT, months=None):
    """
    顯示總覽Dashboard頁面，提供市場概況的快速視圖。

    參數:
        job_data_analyzer: 用於數據處理的JobDataAnalyzer實例
        keywords: 用於過濾職缺的關鍵詞列表，默認為None
        city: 用於過濾職缺的城市，默認為None
        district: 用於過濾職缺的地區，默認為None
        limit: 最大獲取職缺數量，默認為DEFAULT_LIMIT
        months: 如果提供，只獲取最近N個月的職缺，默認為None
    """
    # 創建儀表板頁面並顯示
    dashboard_page = DashboardPage(job_data_analyzer)
    dashboard_page.show(keywords, city, district, limit, months)
