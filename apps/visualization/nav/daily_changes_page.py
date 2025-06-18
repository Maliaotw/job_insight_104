"""
Daily changes page for the 104 Job Insight visualization app.
This page analyzes the daily changes in job listings, showing trends and detailed changes.
"""

from datetime import datetime
from typing import Callable, List, Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from apps.visualization.analysis.df_utils import (
    get_job_display_columns,
    prepare_jobs_analysis_df,
)
from apps.visualization.components import display_filter_info
from config.settings import logger


def display_jobs_for_selected_date(selected_date, jobs_df, trend_analyzer=None):
    """
    顯示選定日期的職缺詳情表格

    參數:
        selected_date: 選定的日期
        jobs_df: 職缺數據DataFrame
        trend_analyzer: TrendAnalyzer實例，可選，用於額外的數據分析
    """
    # 記錄顯示選定日期的職缺詳情
    logger.debug(f"顯示{selected_date}的職缺詳情表格")

    # 使用更美觀的標題
    st.markdown(
        f"""
        <div style='padding: 15px; background-color: #f0f2f6; border-radius: 10px; margin: 20px 0;'>
            <h2 style='margin: 0; color: #0068c9;'>
                <span style='font-size: 1.5em;'>📋</span> {selected_date.strftime('%Y年%m月%d日')}的新增職缺
            </h2>
        </div>
        """,
        unsafe_allow_html=True,
    )

    logger.info(f"顯示{selected_date}的新增職缺詳情")

    # 將日期欄位轉換為日期格式
    jobs_df["appear_date"] = pd.to_datetime(jobs_df["appearDate"], format="%Y%m%d")

    # 篩選選定日期的職缺
    selected_jobs = jobs_df[jobs_df["appear_date"].dt.date == selected_date]

    if selected_jobs.empty:
        logger.warning(f"{selected_date}沒有新增職缺")
        st.info(f"{selected_date.strftime('%Y年%m月%d日')}沒有新增職缺。")
        return

    logger.debug(f"找到{len(selected_jobs)}個{selected_date}的新增職缺")

    # 顯示職缺數量統計
    st.markdown(
        f"""
        <div style='margin-bottom: 20px;'>
            <p style='font-size: 1.1em;'>
                在 <span style='font-weight: bold;'>{selected_date.strftime('%Y年%m月%d日')}</span> 
                共發現 <span style='font-weight: bold; color: #0068c9;'>{len(selected_jobs)}</span> 個新增職缺
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 準備職缺分析數據
    # 準備分析數據
    jobs_analysis = prepare_jobs_analysis_df(selected_jobs)

    # 獲取標準顯示欄位
    display_cols = get_job_display_columns(jobs_analysis)

    # 如果沒有連結欄位，創建一個
    if "連結" not in jobs_analysis.columns and "jobNo" in jobs_analysis.columns:
        jobs_analysis["連結"] = jobs_analysis["jobNo"].apply(
            lambda x: f"https://www.104.com.tw/job/{x}" if pd.notna(x) else ""
        )

    # 添加表格標題
    st.markdown("### 職缺詳細資料")

    # 顯示職缺表格 - 使用與dashboard_page相同的配置
    st.dataframe(
        jobs_analysis[display_cols],
        column_config={
            "連結": st.column_config.LinkColumn(
                "連結", display_text="開啟連結", width="small"
            ),
            "職稱": st.column_config.TextColumn("職稱", width="large"),
            "公司名稱": st.column_config.TextColumn("公司名稱", width="medium"),
            "薪資範圍": st.column_config.TextColumn("薪資範圍", width="medium"),
            "城市": st.column_config.TextColumn("城市", width="small"),
            "地區": st.column_config.TextColumn("地區", width="small"),
        },
        use_container_width=True,
        hide_index=True,
    )

    # 提供下載選項 - 美化下載按鈕
    csv = jobs_analysis[display_cols].to_csv(index=False).encode("utf-8-sig")

    download_container = st.container()
    with download_container:
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.download_button(
                label="📥 下載職缺數據 CSV",
                data=csv,
                file_name=f"jobs_{selected_date.strftime('%Y%m%d')}.csv",
                mime="text/csv",
                help="下載所有職缺數據為CSV檔案",
                use_container_width=True,
            )

    # 添加使用說明
    with st.expander("📘 如何使用這些數據?"):
        st.markdown(
            """
        ### 數據使用指南

        1. **瀏覽職缺**: 表格中顯示了當日新增的所有職缺，可以點擊表頭進行排序。
        2. **開啟職缺頁面**: 點擊「開啟連結」可直接前往104人力銀行查看完整職缺內容。
        3. **下載數據**: 使用「下載職缺數據」按鈕可將所有職缺資料下載為CSV檔案，方便進一步分析。
        4. **數據分析建議**:
           - 比較不同日期的職缺變化趨勢
           - 分析特定產業或職位的薪資範圍
           - 觀察企業招聘活動的頻率和規模
        """
        )

    logger.info(f"{selected_date}的職缺詳情表格顯示完成")


# 常數定義
DEFAULT_JOB_LIMIT = 10000
ALL_CITIES_LABEL = "全部城市"
ALL_DISTRICTS_LABEL = "全部地區"


class JobDataLoader:
    """
    職缺數據載入器，負責從數據庫載入和過濾職缺數據。
    """

    def __init__(self, job_data_analyzer):
        """
        初始化職缺數據載入器。

        參數:
            job_data_analyzer: JobDataAnalyzer實例，用於數據處理
        """
        self.job_data_analyzer = job_data_analyzer

    def load_jobs(
        self,
        keywords: Optional[List[str]] = None,
        city: Optional[str] = None,
        district: Optional[str] = None,
        limit: int = DEFAULT_JOB_LIMIT,
        months: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """
        從數據庫載入職缺數據。

        參數:
            keywords: 關鍵詞列表
            city: 城市名稱
            district: 地區名稱
            limit: 最大獲取職缺數量
            months: 月份數量

        返回:
            jobs_df: 職缺數據DataFrame，如果沒有數據則返回None
        """
        logger.debug("顯示數據載入狀態區塊")
        st.subheader("數據載入狀態")

        # 獲取所有帶過濾條件的職缺
        logger.info("從數據庫獲取職缺數據")
        jobs_df = self.job_data_analyzer.get_jobs(
            limit=limit, months=months, keywords=keywords, city=city, district=district
        )
        logger.debug(f"獲取到 {len(jobs_df)} 條職缺數據")

        if jobs_df.empty:
            logger.warning("數據庫中沒有符合條件的職缺數據")
            st.warning(
                "數據庫中沒有符合條件的職缺數據。請調整篩選條件或先爬取更多數據。"
            )
            return None

        # 顯示職缺數量
        logger.info(f"找到 {len(jobs_df)} 個符合條件的職缺")
        st.write(f"找到 {len(jobs_df)} 個符合條件的職缺")

        return jobs_df

    def filter_by_location(
        self, jobs_df: pd.DataFrame, selected_city: str, selected_district: str
    ) -> Optional[pd.DataFrame]:
        """
        根據選擇的城市和地區過濾職缺數據。

        參數:
            jobs_df: 職缺數據DataFrame
            selected_city: 選擇的城市
            selected_district: 選擇的地區

        返回:
            filtered_jobs_df: 過濾後的職缺數據DataFrame，如果沒有數據則返回None
        """
        logger.info(
            f"根據選擇的城市和地區過濾職缺: 城市={selected_city}, 地區={selected_district}"
        )
        logger.debug(f"過濾前職缺數量: {len(jobs_df)}")

        # 應用過濾條件
        filtered_df = jobs_df.copy()
        if selected_city != ALL_CITIES_LABEL:
            filtered_df = filtered_df[filtered_df["city"] == selected_city]

        if selected_district != ALL_DISTRICTS_LABEL:
            filtered_df = filtered_df[filtered_df["district"] == selected_district]

        logger.debug(f"過濾後職缺數量: {len(filtered_df)}")

        # 檢查過濾後的數據是否為空
        if filtered_df.empty:
            filter_desc = self._get_location_filter_description(
                selected_city, selected_district
            )
            logger.warning(f"沒有找到{filter_desc}的職缺")
            st.warning(f"沒有找到{filter_desc}的職缺")
            return None

        filter_desc = self._get_location_filter_description(
            selected_city, selected_district
        )
        logger.info(f"找到 {len(filtered_df)} 個{filter_desc}的職缺")
        st.write(f"找到 {len(filtered_df)} 個{filter_desc}的職缺")
        return filtered_df

    def _get_location_filter_description(
        self, selected_city: str, selected_district: str
    ) -> str:
        """
        獲取位置過濾條件描述。

        參數:
            selected_city: 選擇的城市
            selected_district: 選擇的地區

        返回:
            filter_desc: 過濾條件描述
        """
        if (
            selected_city != ALL_CITIES_LABEL
            and selected_district != ALL_DISTRICTS_LABEL
        ):
            return f"城市為 '{selected_city}' 且地區為 '{selected_district}'"
        elif selected_city != ALL_CITIES_LABEL:
            return f"城市為 '{selected_city}'"
        else:
            return f"地區為 '{selected_district}'"


class DailyChangesAnalyzer:
    """
    每日職缺變化分析器，負責分析職缺數據的每日變化趨勢。
    """

    def __init__(self, trend_analyzer):
        """
        初始化每日職缺變化分析器。

        參數:
            trend_analyzer: TrendAnalyzer實例，用於趨勢分析
        """
        self.trend_analyzer = trend_analyzer

    def analyze_daily_changes(self, jobs_df: pd.DataFrame) -> None:
        """
        分析每日職缺變化趨勢，顯示每月日歷視圖並允許點擊日期查看詳情。

        參數:
            jobs_df: 職缺數據DataFrame
        """
        # 檢查是否有日期信息
        if "appearDate" not in jobs_df.columns:
            logger.warning("職缺數據中沒有日期信息")
            st.info("職缺數據中沒有日期信息，無法分析每日變化趨勢。")
            return

        # 記錄分析每日職缺變化
        logger.debug("分析每日職缺變化趨勢區塊")
        logger.info("分析每日職缺變化趨勢")
        logger.debug(f"使用 {len(jobs_df)} 條職缺數據進行分析")

        # 獲取每日職缺變化數據
        daily_jobs = self.trend_analyzer.analyze_daily_job_changes(jobs_df)
        logger.debug(
            f"獲取到 {len(daily_jobs) if not daily_jobs.empty else 0} 條每日職缺變化數據"
        )

        if daily_jobs.empty:
            logger.warning("無法分析每日職缺變化趨勢，可能是數據不足")
            st.info("無法分析每日職缺變化趨勢，可能是因為數據不足。")
            return

        logger.debug(
            f"數據時間範圍: {daily_jobs['appear_date'].min() if not daily_jobs.empty else 'N/A'} 至 {daily_jobs['appear_date'].max() if not daily_jobs.empty else 'N/A'}"
        )

        self._display_analysis_results(daily_jobs, jobs_df)

    def _display_analysis_results(
        self, daily_jobs: pd.DataFrame, jobs_df: pd.DataFrame
    ) -> None:
        """
        顯示分析結果，包括日歷視圖、趨勢圖表和詳情表格。

        參數:
            daily_jobs: 每日職缺變化數據
            jobs_df: 原始職缺數據
        """
        # 顯示每月日歷視圖，僅顯示新增職缺，並允許點擊日期查看詳情
        display_monthly_calendar_view(daily_jobs, jobs_df)

        # 顯示每日職缺變化趨勢圖表
        display_daily_job_trend_chart(daily_jobs)

        # 顯示每日職缺變化詳情表格
        display_daily_job_details_table(daily_jobs)

        # 提供日期選擇器進行詳細分析
        self.provide_date_selector_for_detailed_analysis(jobs_df)

    def provide_date_selector_for_detailed_analysis(
        self, jobs_df, on_date_selected: Optional[Callable] = None
    ):
        """
        提供日期選擇器進行詳細分析

        參數:
            jobs_df: 職缺數據DataFrame
            on_date_selected: 當日期被選中時的回調函數，如果為None則使用默認處理
        """
        # 記錄提供日期選擇器
        logger.debug("顯示職缺詳細變化分析區塊")
        st.subheader("職缺詳細變化分析")
        logger.info("分析職缺詳細變化，提供日期選擇器")

        # 獲取職缺詳細變化數據
        job_details = self.trend_analyzer.analyze_job_details_by_date(jobs_df)
        logger.debug(
            f"獲取到 {len(job_details) if not job_details.empty else 0} 條職缺詳細變化數據"
        )

        if job_details.empty:
            logger.warning("無法獲取職缺詳細變化數據")
            st.info("無法獲取職缺詳細變化數據，可能是因為數據不足。")
            return

        logger.debug(
            f"詳細數據時間範圍: {job_details['appear_date'].min().strftime('%Y-%m-%d') if not job_details.empty else 'N/A'} 至 {job_details['appear_date'].max().strftime('%Y-%m-%d') if not job_details.empty else 'N/A'}"
        )

        # 允許用戶選擇日期
        dates = job_details["appear_date"].dt.strftime("%Y-%m-%d").unique().tolist()
        # 按降序排序日期，顯示最近的日期在前
        dates.sort(reverse=True)

        if not dates:
            logger.warning("沒有可選的日期")
            st.info("無法獲取職缺詳細變化數據，可能是因為數據不足。")
            return

        logger.info(f"提供 {len(dates)} 個日期供用戶選擇")
        logger.debug(f"可選日期範圍: {min(dates)} 至 {max(dates)}")
        selected_date = st.selectbox(
            "選擇日期查看詳細變化", dates, key="daily_changes_date"
        )

        if selected_date:
            # 分析選定日期的詳細變化
            if on_date_selected:
                on_date_selected(selected_date, job_details)
            else:
                self.analyze_selected_date_changes(job_details, selected_date)

    def analyze_selected_date_changes(self, job_details, selected_date):
        """
        分析選定日期的詳細變化

        參數:
            job_details: 職缺詳細變化數據DataFrame
            selected_date: 選定的日期
        """
        # 記錄分析選定日期
        logger.debug(f"分析選定日期 {selected_date} 的詳細變化")
        logger.info(f"分析選定日期 {selected_date} 的職缺變化詳情")

        # 找到選定日期和前一天的數據
        selected_date_obj = datetime.strptime(selected_date, "%Y-%m-%d")
        selected_idx = job_details[
            job_details["appear_date"] == selected_date_obj
        ].index
        logger.debug(
            f"選定日期索引: {selected_idx[0] if len(selected_idx) > 0 else 'N/A'}"
        )

        if len(selected_idx) == 0 or selected_idx[0] <= 0:
            logger.warning(f"無法比較 {selected_date} 與前一天的數據")
            st.info(
                f"無法比較 {selected_date} 與前一天的數據，可能是因為這是數據中的第一天。"
            )
            return

        # 獲取當前和前一天的索引
        curr_idx = selected_idx[0]
        prev_idx = curr_idx - 1
        logger.debug(f"當前日期索引: {curr_idx}, 前一日期索引: {prev_idx}")

        # 獲取當前和前一天的日期
        curr_date = job_details.iloc[curr_idx]["appear_date"].strftime("%Y-%m-%d")
        prev_date = job_details.iloc[prev_idx]["appear_date"].strftime("%Y-%m-%d")
        logger.debug(f"比較日期: {curr_date} vs {prev_date}")

        # 獲取當前和前一天的職缺數據
        curr_row = job_details.iloc[curr_idx]
        prev_row = job_details.iloc[prev_idx]
        logger.debug(
            f"當前日期職缺數: {len(curr_row['jobName'])}, 前一日期職缺數: {len(prev_row['jobName'])}"
        )

        # 創建數據框
        curr_df, prev_df = self.create_job_dataframes(curr_row, prev_row)

        # 計算新增和減少的職缺
        new_job_keys, removed_job_keys = self.calculate_job_changes(curr_df, prev_df)

        # 顯示結果
        self.display_job_change_results(
            curr_date,
            curr_idx,
            job_details,
            new_job_keys,
            removed_job_keys,
            curr_df,
            prev_df,
        )

    def create_job_dataframes(self, curr_row, prev_row):
        """
        創建當前和前一天的職缺數據框

        參數:
            curr_row: 當前日期的數據行
            prev_row: 前一日期的數據行

        返回:
            curr_df: 當前日期的職缺數據框
            prev_df: 前一日期的職缺數據框
        """
        # 記錄創建數據框
        logger.debug("創建當前和前一天的職缺數據框")

        # 從行數據創建數據框
        curr_df = pd.DataFrame(
            {
                "職缺名稱": curr_row["jobName"],
                "公司名稱": curr_row["custName"],
                "城市": curr_row["city"],
                "地區": curr_row["district"],
                "job": curr_row["job"],
                "search_keyword": curr_row["search_keyword"],
            }
        )

        prev_df = pd.DataFrame(
            {
                "職缺名稱": prev_row["jobName"],
                "公司名稱": prev_row["custName"],
                "城市": prev_row["city"],
                "地區": prev_row["district"],
                "job": prev_row["job"],
                "search_keyword": prev_row["search_keyword"],
            }
        )

        # 創建複合鍵
        curr_df["composite_key"] = curr_df["職缺名稱"] + "|" + curr_df["公司名稱"]
        prev_df["composite_key"] = prev_df["職缺名稱"] + "|" + prev_df["公司名稱"]

        return curr_df, prev_df

    def calculate_job_changes(self, curr_df, prev_df):
        """
        計算新增和減少的職缺

        參數:
            curr_df: 當前日期的職缺數據框
            prev_df: 前一日期的職缺數據框

        返回:
            new_job_keys: 新增職缺的鍵集合
            removed_job_keys: 減少職缺的鍵集合
        """
        # 記錄計算職缺變化
        logger.debug("計算新增和減少的職缺")

        # 獲取唯一的複合鍵
        curr_jobs_keys = set(curr_df["composite_key"])
        prev_jobs_keys = set(prev_df["composite_key"])
        logger.debug(
            f"當前日期不重複職缺: {len(curr_jobs_keys)}, 前一日期不重複職缺: {len(prev_jobs_keys)}"
        )

        # 計算新增和減少的職缺鍵
        new_job_keys = curr_jobs_keys - prev_jobs_keys
        removed_job_keys = prev_jobs_keys - curr_jobs_keys
        logger.debug(
            f"新增職缺數: {len(new_job_keys)}, 減少職缺數: {len(removed_job_keys)}"
        )

        return new_job_keys, removed_job_keys

    def display_job_change_results(
        self,
        curr_date,
        curr_idx,
        job_details,
        new_job_keys,
        removed_job_keys,
        curr_df,
        prev_df,
    ):
        """
        顯示職缺變化結果

        參數:
            curr_date: 當前日期
            curr_idx: 當前日期的索引
            job_details: 職缺詳細變化數據DataFrame
            new_job_keys: 新增職缺的鍵集合
            removed_job_keys: 減少職缺的鍵集合
            curr_df: 當前日期的職缺數據框
            prev_df: 前一日期的職缺數據框
        """
        # 記錄顯示結果
        logger.debug("顯示職缺變化結果")

        # 顯示標題和總職缺數
        st.write(f"### {curr_date} 職缺變化")
        st.write(f"總職缺數: {job_details.iloc[curr_idx]['total_count']}")

        # 顯示新增職缺
        self.display_new_jobs(new_job_keys, curr_df)

        # 顯示減少職缺
        self.display_removed_jobs(removed_job_keys, prev_df)

    def display_new_jobs(self, new_job_keys, curr_df):
        """
        顯示新增職缺

        參數:
            new_job_keys: 新增職缺的鍵集合
            curr_df: 當前日期的職缺數據框
        """
        # 檢查是否有新增職缺
        if len(new_job_keys) == 0:
            return

        # 記錄顯示新增職缺
        logger.debug(f"顯示 {len(new_job_keys)} 個新增職缺")
        st.write(f"#### 新增職缺 ({len(new_job_keys)}):")

        # 過濾只顯示新增職缺
        new_jobs_df = curr_df[curr_df["composite_key"].isin(new_job_keys)]

        # 刪除複合鍵列用於顯示
        new_jobs_df = new_jobs_df.drop(columns=["composite_key"])

        # 將DataFrame轉換為prepare_jobs_analysis_df所需的格式
        jobs_df = pd.DataFrame(
            {
                "jobName": new_jobs_df["職缺名稱"],
                "custName": new_jobs_df["公司名稱"],
                "city": new_jobs_df["城市"],
                "district": new_jobs_df["地區"],
                "job": new_jobs_df["job"],
                "search_keyword": new_jobs_df["search_keyword"],
            }
        )

        # 使用prepare_jobs_analysis_df優化DataFrame
        from apps.visualization.analysis.df_utils import (
            get_job_display_columns,
            prepare_jobs_analysis_df,
        )

        optimized_df = prepare_jobs_analysis_df(jobs_df)

        # 獲取顯示列
        display_columns = get_job_display_columns(optimized_df)

        # 只顯示存在的列
        display_df = optimized_df[display_columns]

        st.dataframe(display_df, use_container_width=True)

    def display_removed_jobs(self, removed_job_keys, prev_df):
        """
        顯示減少職缺

        參數:
            removed_job_keys: 減少職缺的鍵集合
            prev_df: 前一日期的職缺數據框
        """
        # 檢查是否有減少職缺
        if len(removed_job_keys) == 0:
            return

        # 記錄顯示減少職缺
        logger.debug(f"顯示 {len(removed_job_keys)} 個減少職缺")
        st.write(f"#### 減少職缺 ({len(removed_job_keys)}):")

        # 過濾只顯示減少職缺
        removed_jobs_df = prev_df[prev_df["composite_key"].isin(removed_job_keys)]

        # 刪除複合鍵列用於顯示
        removed_jobs_df = removed_jobs_df.drop(columns=["composite_key"])

        # 將DataFrame轉換為prepare_jobs_analysis_df所需的格式
        jobs_df = pd.DataFrame(
            {
                "jobName": removed_jobs_df["職缺名稱"],
                "custName": removed_jobs_df["公司名稱"],
                "city": removed_jobs_df["城市"],
                "district": removed_jobs_df["地區"],
                "job": removed_jobs_df["job"],
                "search_keyword": removed_jobs_df["search_keyword"],
            }
        )

        # 使用prepare_jobs_analysis_df優化DataFrame
        from apps.visualization.analysis.df_utils import (
            get_job_display_columns,
            prepare_jobs_analysis_df,
        )

        optimized_df = prepare_jobs_analysis_df(jobs_df)

        # 獲取顯示列
        display_columns = get_job_display_columns(optimized_df)

        # 只顯示存在的列
        display_df = optimized_df[display_columns]

        st.dataframe(display_df, use_container_width=True)


class DailyChangesPageRenderer:
    """
    每日職缺變化頁面渲染器，負責渲染頁面UI和協調數據處理。
    """

    def __init__(self, job_data_analyzer, trend_analyzer):
        """
        初始化每日職缺變化頁面渲染器。

        參數:
            job_data_analyzer: JobDataAnalyzer實例，用於數據處理
            trend_analyzer: TrendAnalyzer實例，用於趨勢分析
        """
        self.job_data_loader = JobDataLoader(job_data_analyzer)
        self.daily_changes_analyzer = DailyChangesAnalyzer(trend_analyzer)

    def render_page(
        self,
        keywords: Optional[List[str]] = None,
        city: Optional[str] = None,
        district: Optional[str] = None,
        limit: int = DEFAULT_JOB_LIMIT,
        months: Optional[int] = None,
    ) -> None:
        """
        渲染每日職缺變化分析頁面。

        參數:
            keywords: 關鍵詞列表
            city: 城市名稱
            district: 地區名稱
            limit: 最大獲取職缺數量
            months: 月份數量
        """
        logger.info("顯示每日職缺變化分析頁面")
        st.header("每日職缺變化分析")
        st.markdown("分析每日新增和減少的職缺情況，了解市場動態變化。")

        # 顯示過濾條件信息
        display_filter_info(keywords, city, district, months)

        try:
            # 載入數據
            jobs_df = self.job_data_loader.load_jobs(
                keywords, city, district, limit, months
            )
            if jobs_df is None:
                return

            # 分析每日職缺變化
            self.daily_changes_analyzer.analyze_daily_changes(jobs_df)

        except Exception as e:
            logger.error(f"顯示每日職缺變化分析頁面時發生錯誤: {str(e)}", exc_info=True)
            st.error(f"分析數據時發生錯誤: {str(e)}")


def display_monthly_calendar_view(
    daily_jobs, jobs_df, on_date_selected: Optional[Callable] = None
):
    """
    以每月日歷的方式顯示每日新增的職缺，允許選擇月份並點擊日期查看詳情

    參數:
        daily_jobs: 每日職缺變化數據DataFrame
        jobs_df: 原始職缺數據DataFrame
        on_date_selected: 當日期被選中時的回調函數，如果為None則使用默認處理
    """
    # 記錄顯示月歷視圖
    logger.debug("顯示每月日歷視圖區塊")
    st.subheader("每月新增職缺日歷視圖")
    logger.info("創建每月新增職缺日歷視圖")

    # 確保日期列是日期類型
    daily_jobs["appear_date"] = pd.to_datetime(daily_jobs["appear_date"])

    # 獲取數據中的最小和最大日期
    min_date = daily_jobs["appear_date"].min()
    max_date = daily_jobs["appear_date"].max()

    # 計算數據中包含的月份
    months = pd.date_range(
        start=pd.Timestamp(min_date.year, min_date.month, 1),
        end=pd.Timestamp(max_date.year, max_date.month, 1),
        freq="MS",  # 月初
    )

    # 創建月份選擇器 - 使用更美觀的選擇器
    st.markdown("### 選擇月份查看職缺變化")
    month_options = [month.strftime("%Y年%m月") for month in months]

    # 使用容器和列來美化月份選擇器
    month_container = st.container()
    with month_container:
        col1, col2 = st.columns([3, 1])
        with col1:
            selected_month_str = st.selectbox(
                "選擇月份",
                month_options,
                index=len(month_options) - 1,  # 默認選擇最新月份
                format_func=lambda x: f"📅 {x}",  # 添加圖標
            )
        with col2:
            # 添加一個小的說明
            st.markdown(
                "<div style='margin-top: 30px;'>選擇月份查看該月職缺變化</div>",
                unsafe_allow_html=True,
            )

    # 將選擇的月份字符串轉換回日期對象
    selected_year = int(selected_month_str.split("年")[0])
    selected_month = int(selected_month_str.split("年")[1].split("月")[0])
    selected_month_start = pd.Timestamp(selected_year, selected_month, 1)
    selected_month_end = selected_month_start + pd.offsets.MonthEnd(1)

    # 篩選選定月份的數據
    month_data = daily_jobs[
        (daily_jobs["appear_date"] >= selected_month_start)
        & (daily_jobs["appear_date"] <= selected_month_end)
    ]

    if month_data.empty:
        st.info(f"{selected_month_str}沒有職缺數據")
        return

    # 添加月份摘要信息
    total_new_jobs = month_data["new_jobs"].sum()
    st.markdown(
        f"""
        <div style='padding: 10px; border-radius: 10px; margin-bottom: 20px;'>
            <h3 style='margin-top: 0;'>{selected_month_str}職缺摘要</h3>
            <p>本月共新增 <span style='font-weight: bold; color: #0068c9;'>{int(total_new_jobs)}</span>個職缺</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 創建當月的日期範圍
    days_in_month = pd.date_range(selected_month_start, selected_month_end)

    # 獲取當月第一天是星期幾 (0=星期一, 6=星期日)
    first_day_weekday = selected_month_start.weekday()

    # 創建日歷網格 - 使用更美觀的容器
    calendar_container = st.container()
    with calendar_container:
        # 添加日歷標題
        st.markdown(
            f"""
            <div style='text-align: center; margin-bottom: 15px;'>
                <h3 style='margin-bottom: 5px;'>{selected_month_str}日歷視圖</h3>
                <p style='color: #666; font-size: 0.9em;'>點擊有職缺的日期查看詳情</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # 使用st.columns創建7列代表一週的7天
        cols = st.columns(7)

        # 顯示星期標題 - 美化星期標題
        weekdays = ["一", "二", "三", "四", "五", "六", "日"]
        for i, day in enumerate(weekdays):
            # 週末使用不同顏色
            if i >= 5:  # 週六和週日
                color = "#ff4b4b"  # 紅色
            else:
                color = "#0068c9"  # 藍色

            cols[i].markdown(
                f"""
                <div style='text-align: center; padding: 8px; background-color: #f0f2f6; 
                border-radius: 5px; font-weight: bold; color: {color};'>
                    {day}
                </div>
                """,
                unsafe_allow_html=True,
            )

        # 計算需要顯示的行數
        num_rows = (len(days_in_month) + first_day_weekday + 6) // 7

        # 創建日歷網格
        day_idx = -first_day_weekday  # 從負數開始以處理月初不是星期一的情況

        # 使用session_state來存儲選中的日期
        if "selected_date" not in st.session_state:
            st.session_state.selected_date = None

        # 獲取當月最大的新增職缺數，用於顏色強度計算
        max_new_jobs = month_data["new_jobs"].max() if not month_data.empty else 0

        for row in range(num_rows):
            # 為每一行創建7列
            day_cols = st.columns(7)

            for col in range(7):
                day_idx += 1
                if day_idx < 0 or day_idx >= len(days_in_month):
                    # 當月之外的日期顯示空白
                    day_cols[col].markdown("&nbsp;")
                else:
                    current_date = days_in_month[day_idx]
                    day_str = str(current_date.day)

                    # 查找當天的新增職缺數
                    day_data = month_data[
                        month_data["appear_date"].dt.date == current_date.date()
                    ]
                    new_jobs_count = (
                        day_data["new_jobs"].sum() if not day_data.empty else 0
                    )

                    # 創建一個唯一的按鈕鍵
                    button_key = f"date_button_{current_date.strftime('%Y%m%d')}"

                    # 檢查是否為今天
                    is_today = current_date.date() == datetime.now().date()

                    # 檢查是否為週末
                    is_weekend = current_date.weekday() >= 5

                    # 根據新增職缺數量設置顏色
                    if new_jobs_count > 0:
                        # 計算顏色強度 - 根據最大值進行歸一化
                        color_intensity = min(
                            0.2 + (new_jobs_count / max(max_new_jobs, 1)) * 0.8, 1.0
                        )

                        # 使用綠色顯示有新增職缺的日期，並使其可點擊
                        bg_color = "#f0f2f6"
                        text_color = "#333333"  # 默認文字顏色
                        border = ""

                        day_cols[col].markdown(
                            f"""
                                     <style>
                                         .stColumn div[data-testid="stColumn"] {{
                                              text-align: center; padding: 10px; background-color: {bg_color}; color: {text_color}; border-radius: 10px; {border}
                                          }}
                                          div[data-testid="stButton"] > button {{
                                                text-align: center;
                                         }}

                                      </style>

                                      """,
                            unsafe_allow_html=True,
                        )

                        # 添加今天的標記
                        day_label = f"{day_str}"
                        if is_today:
                            day_label = f"📌 {day_str}"

                        if day_cols[col].button(
                            f"{day_label}\n{int(new_jobs_count)}(個職缺)",
                            key=button_key,
                        ):
                            st.session_state.selected_date = current_date.date()
                    else:
                        # 普通顯示沒有新增職缺的日期
                        # 根據是否為今天和週末設置不同的樣式
                        bg_color = (
                            "#f0f2f6"  # 默認背景色 - 使用與其他元素相同的淺藍色背景
                        )
                        text_color = "#333333"  # 默認文字顏色
                        border = ""

                        if is_today:
                            border = "border: 2px solid #0068c9;"  # 今天加藍色邊框
                            day_str = f"📌 {day_str}"  # 今天加標記

                        if is_weekend:
                            bg_color = "#f0f0f0"  # 週末使用淺灰色背景
                            text_color = "#666666"  # 週末使用灰色文字

                        day_cols[col].markdown(
                            f"""
                           <style>
                               div[data-testid="stColumn"] {{
                                    text-align: center; 
                                    padding: 10px; 
                                }}
                            </style>
                            """,
                            unsafe_allow_html=True,
                        )

                        day_cols[col].button(
                            f"{day_str}",
                        )

    # 如果有選中的日期，顯示該日期的職缺詳情
    if st.session_state.selected_date:
        st.markdown("---")
        if on_date_selected:
            on_date_selected(st.session_state.selected_date)
        else:
            display_jobs_for_selected_date(st.session_state.selected_date, jobs_df)

    logger.info("每月新增職缺日歷視圖顯示完成")


def create_job_trend_chart(chart_data):
    """
    創建職缺趨勢圖表

    參數:
        chart_data: 圖表數據DataFrame

    返回:
        fig: Plotly圖表對象
    """
    # 記錄創建圖表
    logger.debug("創建職缺趨勢圖表")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=chart_data["date"],
            y=chart_data["新增職缺"],
            name="新增職缺",
            line=dict(color="green", width=2),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=chart_data["date"],
            y=chart_data["減少職缺"],
            name="減少職缺",
            line=dict(color="red", width=2),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=chart_data["date"],
            y=chart_data["淨變化"],
            name="淨變化",
            line=dict(color="blue", width=2),
        )
    )

    fig.update_layout(
        title="每日職缺變化趨勢",
        xaxis_title="日期",
        yaxis_title="職缺數量",
        hovermode="x unified",
    )
    logger.debug("圖表配置完成，準備顯示")

    return fig


def display_daily_job_trend_chart(daily_jobs):
    """
    顯示每日職缺變化趨勢圖表

    參數:
        daily_jobs: 每日職缺變化數據DataFrame
    """
    # 記錄顯示趨勢圖表
    logger.debug("顯示每日職缺變化趨勢圖表區塊")
    st.subheader("每日職缺變化趨勢")
    logger.info("創建每日職缺變化趨勢圖表")
    logger.debug(
        f"圖表數據範圍: 新增職缺 {daily_jobs['new_jobs'].min()} 至 {daily_jobs['new_jobs'].max()}, 減少職缺 {daily_jobs['removed_jobs'].min()} 至 {daily_jobs['removed_jobs'].max()}"
    )

    # 計算淨變化
    daily_jobs["淨變化"] = daily_jobs["new_jobs"] - daily_jobs["removed_jobs"]
    chart_data = daily_jobs.rename(
        columns={
            "appear_date": "date",
            "new_jobs": "新增職缺",
            "removed_jobs": "減少職缺",
        }
    )
    logger.debug(f"圖表數據準備完成，包含 {len(chart_data)} 個數據點")

    # 創建趨勢圖表
    fig = create_job_trend_chart(chart_data)
    st.plotly_chart(fig, use_container_width=True)
    logger.info("每日職缺變化趨勢圖表顯示完成")


def display_daily_job_details_table(daily_jobs):
    """
    顯示每日職缺變化詳情表格

    參數:
        daily_jobs: 每日職缺變化數據DataFrame
    """
    # 記錄顯示詳情表格
    logger.debug("顯示每日職缺變化詳情表格區塊")
    st.subheader("每日職缺變化詳情")
    logger.info("顯示每日職缺變化詳情表格")

    # 格式化數據用於顯示
    display_df = daily_jobs.copy()
    display_df["appear_date"] = display_df["appear_date"].dt.strftime("%Y-%m-%d")
    display_df = display_df.rename(
        columns={
            "appear_date": "日期",
            "jobNo": "職缺數",
            "new_jobs": "新增職缺",
            "removed_jobs": "減少職缺",
            "new_delta": "新增變化",
            "removed_delta": "減少變化",
        }
    )
    logger.debug(
        f"表格數據準備完成，包含 {len(display_df)} 行，{len(display_df.columns)} 列"
    )

    # 選擇要顯示的列
    display_cols = ["日期", "職缺數", "新增職缺", "減少職缺", "新增變化", "減少變化"]
    st.dataframe(display_df[display_cols], use_container_width=True)
    logger.info("每日職缺變化詳情表格顯示完成")


def show_daily_changes_page(
    job_data_analyzer,
    trend_analyzer,
    keywords=None,
    city=None,
    district=None,
    limit=DEFAULT_JOB_LIMIT,
    months=None,
):
    """
    顯示每日職缺變化分析頁面，分析職缺市場的動態變化。

    參數:
        job_data_analyzer: JobDataAnalyzer實例，用於數據處理
        trend_analyzer: TrendAnalyzer實例，用於趨勢分析
        keywords: 用於過濾職缺的關鍵詞列表，默認為None
        city: 用於過濾職缺的城市，默認為None
        district: 用於過濾職缺的地區，默認為None
        limit: 最大獲取職缺數量，默認為10000
        months: 如果提供，只獲取最近N個月的職缺，默認為None
    """
    page_renderer = DailyChangesPageRenderer(job_data_analyzer, trend_analyzer)
    page_renderer.render_page(keywords, city, district, limit, months)
