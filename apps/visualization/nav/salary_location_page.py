"""
Salary and location page for the 104 Job Insight visualization app.
This page analyzes the distribution of jobs across different salary ranges and locations.
"""

from typing import Optional, Dict

import pandas as pd
import plotly.express as px
import streamlit as st

from apps.visualization.components import display_filter_info
from config.settings import logger

# Constants
DEFAULT_JOB_LIMIT = 10000
REQUIRED_COLUMNS = {"location": ["city", "district"], "salary": ["salaryDesc"]}
ERROR_MESSAGES = {
    "no_location_data": "職缺數據中沒有城市和地區信息",
    "general_error": "無法分析薪資與地區，請確保數據格式正確。",
}


class SalaryLocationDataProcessor:
    """
    處理薪資與地區分析頁面的數據加載和處理邏輯
    """

    def __init__(self, job_data_analyzer):
        """
        初始化薪資與地區數據處理器

        Args:
            job_data_analyzer: JobDataAnalyzer實例，用於從數據庫獲取職缺數據
        """
        self.job_data_analyzer = job_data_analyzer

    def load_job_data(
        self,
        keywords=None,
        city=None,
        district=None,
        limit=DEFAULT_JOB_LIMIT,
        months=None,
    ) -> Optional[pd.DataFrame]:
        """
        從數據庫載入職缺數據

        Args:
            keywords: 用於過濾職缺的關鍵詞列表
            city: 用於過濾職缺的城市
            district: 用於過濾職缺的地區
            limit: 最大獲取職缺數量
            months: 如果提供，只獲取最近N個月的職缺

        Returns:
            職缺數據DataFrame，如果沒有數據則返回None
        """
        logger.info("從數據庫獲取職缺數據")
        jobs_df = self.job_data_analyzer.get_jobs(
            limit=limit, months=months, keywords=keywords, city=city, district=district
        )
        logger.debug(f"獲取到 {len(jobs_df)} 條職缺數據")

        if jobs_df.empty:
            logger.warning("數據庫中沒有符合條件的職缺數據")
            return None

        return jobs_df

    def has_required_columns(self, jobs_df: pd.DataFrame) -> Dict[str, bool]:
        """
        檢查數據框是否包含所需的列

        Args:
            jobs_df: 職缺數據DataFrame

        Returns:
            包含各類型列是否存在的字典
        """
        columns_check = {}

        # 檢查地區列
        columns_check["location"] = all(
            col in jobs_df.columns for col in REQUIRED_COLUMNS["location"]
        )

        # 檢查薪資列
        columns_check["salary"] = all(
            col in jobs_df.columns for col in REQUIRED_COLUMNS["salary"]
        )

        return columns_check


"""
Component functions for salary and location page
"""


def display_salary_distribution(jobs_df):
    """
    顯示薪資區間職缺分佈圖表

    參數:
        jobs_df: 職缺數據DataFrame
    """
    # 檢查是否有薪資信息
    if "salaryDesc" not in jobs_df.columns:
        logger.warning("職缺數據中沒有薪資信息")
        st.info("職缺數據中沒有薪資信息")
        return

    # 記錄分析薪資分佈開始
    logger.debug("顯示薪資區間職缺分佈區塊")
    st.subheader("薪資區間職缺分佈")
    logger.info("分析薪資區間職缺分佈")

    # 統計薪資區間職缺數量
    salary_dist = jobs_df["salaryDesc"].value_counts().sort_index()
    logger.debug(f"薪資區間數量: {len(salary_dist)}")

    # 創建條形圖
    fig = px.bar(
        x=salary_dist.index,
        y=salary_dist.values,
        title="薪資區間分佈",
        labels={"x": "薪資區間", "y": "職缺數量"},
        color_discrete_sequence=["#1f77b4"],
    )
    st.plotly_chart(fig, use_container_width=True)
    logger.info("薪資區間職缺分佈圖表顯示完成")


def display_city_distribution(jobs_df):
    """
    顯示城市職缺分佈圖表

    參數:
        jobs_df: 職缺數據DataFrame

    返回:
        city_counts: 城市職缺數量DataFrame
    """
    # 記錄分析城市分佈開始
    logger.debug("顯示城市職缺分佈區塊")
    st.subheader("城市職缺分佈")
    logger.info("分析城市職缺分佈")

    # 統計城市職缺數量
    city_counts = jobs_df["city"].value_counts().reset_index()
    city_counts.columns = ["城市", "職缺數"]
    logger.debug(f"城市數量: {len(city_counts)}")

    # 創建城市職缺分佈條形圖
    fig = px.bar(
        city_counts.head(10),
        x="城市",
        y="職缺數",
        title="城市職缺分佈 (前10名)",
        color="城市",
    )
    st.plotly_chart(fig, use_container_width=True)
    logger.info("城市職缺分佈圖表顯示完成")

    return city_counts


def display_district_distribution(jobs_df):
    """
    顯示地區職缺分佈圖表

    參數:
        jobs_df: 職缺數據DataFrame

    返回:
        district_counts: 地區職缺數量DataFrame
    """
    # 記錄分析地區分佈開始
    logger.debug("顯示地區職缺分佈區塊")
    st.subheader("地區職缺分佈")
    logger.info("分析地區職缺分佈")

    # 統計地區職缺數量
    district_counts = (
        jobs_df.groupby(["city", "district"]).size().reset_index(name="職缺數")
    )
    district_counts.columns = ["城市", "地區", "職缺數"]
    logger.debug(f"地區數量: {len(district_counts)}")

    # 排序並獲取前15名地區
    district_counts = district_counts.sort_values("職缺數", ascending=False).head(15)

    # 創建地區職缺分佈條形圖
    fig = px.bar(
        district_counts,
        x="地區",
        y="職缺數",
        title="地區職缺分佈 (前15名)",
        color="城市",
        hover_data=["城市"],
    )
    st.plotly_chart(fig, use_container_width=True)
    logger.info("地區職缺分佈圖表顯示完成")

    return district_counts


def display_city_salary_relationship(jobs_df, city_counts):
    """
    顯示城市與薪資關係熱力圖

    參數:
        jobs_df: 職缺數據DataFrame
        city_counts: 城市職缺數量DataFrame
    """
    # 記錄分析城市與薪資關係開始
    logger.debug("顯示城市與薪資關係區塊")
    st.subheader("城市與薪資關係")
    logger.info("分析城市與薪資關係")

    # 創建城市與薪資交叉表
    city_salary_counts = pd.crosstab(jobs_df["city"], jobs_df["salaryDesc"])
    logger.debug(f"城市薪資交叉表大小: {city_salary_counts.shape}")

    # 獲取前10名城市
    top_cities = city_counts.head(10)["城市"].tolist()
    city_salary_filtered = city_salary_counts.loc[top_cities]

    # 創建城市與薪資關係熱力圖
    fig = px.imshow(
        city_salary_filtered,
        title="城市與薪資關係熱力圖",
        labels=dict(x="薪資區間", y="城市", color="職缺數"),
        color_continuous_scale="Viridis",
    )
    st.plotly_chart(fig, use_container_width=True)
    logger.info("城市與薪資關係熱力圖顯示完成")


def display_district_salary_relationship(jobs_df, district_counts):
    """
    顯示地區與薪資關係熱力圖

    參數:
        jobs_df: 職缺數據DataFrame
        district_counts: 地區職缺數量DataFrame
    """
    # 記錄分析地區與薪資關係開始
    logger.debug("顯示地區與薪資關係區塊")
    st.subheader("地區與薪資關係")
    logger.info("分析地區與薪資關係")

    # 創建城市-地區組合列
    jobs_df["city_district"] = jobs_df["city"] + "-" + jobs_df["district"]

    # 創建地區與薪資交叉表
    district_salary_counts = pd.crosstab(
        jobs_df["city_district"], jobs_df["salaryDesc"]
    )
    logger.debug(f"地區薪資交叉表大小: {district_salary_counts.shape}")

    # 獲取前10名地區
    top_districts = (
        district_counts.head(10)
        .apply(lambda x: f"{x['城市']}-{x['地區']}", axis=1)
        .tolist()
    )

    # 過濾只顯示前10名地區
    district_salary_filtered = district_salary_counts.loc[
        district_salary_counts.index.isin(top_districts)
    ]

    # 創建地區與薪資關係熱力圖
    fig = px.imshow(
        district_salary_filtered,
        title="地區與薪資關係熱力圖",
        labels=dict(x="薪資區間", y="城市-地區", color="職缺數"),
        color_continuous_scale="Viridis",
    )
    st.plotly_chart(fig, use_container_width=True)
    logger.info("地區與薪資關係熱力圖顯示完成")


def display_location_map(city_counts):
    """
    顯示地區分佈地圖

    參數:
        city_counts: 城市職缺數量DataFrame
    """
    # 記錄創建地區分佈地圖開始
    logger.debug("顯示地區分佈地圖區塊")
    st.subheader("地區分佈地圖")
    logger.info("創建地區分佈地圖")

    # 定義城市座標
    city_coordinates = {
        "台北市": [121.5598, 25.0598],
        "新北市": [121.4657, 25.0125],
        "桃園市": [121.3010, 24.9936],
        "台中市": [120.6839, 24.1377],
        "台南市": [120.2141, 23.0008],
        "高雄市": [120.3011, 22.6273],
        "新竹市": [120.9647, 24.8138],
        "新竹縣": [121.0165, 24.8390],
        "基隆市": [121.7419, 25.1287],
    }
    logger.debug(f"城市座標數量: {len(city_coordinates)}")

    # 添加座標到城市數據
    city_counts["lat"] = city_counts["城市"].map(
        lambda x: city_coordinates.get(x, [0, 0])[1]
    )
    city_counts["lon"] = city_counts["城市"].map(
        lambda x: city_coordinates.get(x, [0, 0])[0]
    )

    # 過濾沒有座標的城市
    city_counts_plot = city_counts[
        (city_counts["lat"] != 0) & (city_counts["lon"] != 0)
    ].copy()
    logger.debug(f"有座標的城市數量: {len(city_counts_plot)}")

    if city_counts_plot.empty:
        logger.warning("無法創建地圖，可能是城市名稱無法匹配到已知座標")
        st.info("無法創建地圖，可能是城市名稱無法匹配到已知座標")
        return

    # 創建地圖
    _create_map_visualization(city_counts_plot)


def _create_map_visualization(city_counts_plot):
    """
    創建地圖可視化

    參數:
        city_counts_plot: 帶有座標的城市職缺數量DataFrame
    """
    # 記錄創建地圖可視化
    logger.debug("創建地圖可視化")
    fig = px.scatter_mapbox(
        city_counts_plot,
        lat="lat",
        lon="lon",
        size="職缺數",
        hover_name="城市",
        hover_data=["職缺數"],
        color="職缺數",
        zoom=7,
        title="台灣各城市職缺分布",
        mapbox_style="carto-positron",
        color_continuous_scale="Viridis",
    )

    # 設置地圖中心
    fig.update_layout(
        mapbox=dict(
            center=dict(lat=23.5, lon=121),
        )
    )
    fig.update_layout(height=800)  # 只設高度，寬度自適應
    st.plotly_chart(fig, use_container_width=True)
    logger.info("地區分佈地圖顯示完成")


class SalaryLocationPageRenderer:
    """
    處理薪資與地區分析頁面的UI渲染邏輯
    """

    def render_page_header(self):
        """渲染頁面標題和描述"""
        st.header("薪資與地區分析")
        st.markdown("分析不同薪資區間和地區的職缺分佈情況。")

    def render_filter_info(self, keywords, city, district, months):
        """渲染過濾條件信息"""
        display_filter_info(keywords, city, district, months)

    def render_data_loading_status(self, jobs_count: int):
        """
        渲染數據載入狀態

        Args:
            jobs_count: 載入的職缺數量
        """
        st.subheader("數據載入狀態")
        st.write(f"找到 {jobs_count} 個符合條件的職缺")

    def render_no_data_warning(self):
        """渲染無數據警告"""
        st.warning("數據庫中沒有符合條件的職缺數據。請調整篩選條件或先爬取更多數據。")

    def render_missing_column_info(self, column_type: str):
        """
        渲染缺失列信息

        Args:
            column_type: 缺失的列類型
        """
        if column_type == "location":
            st.info(ERROR_MESSAGES["no_location_data"])

    def render_analysis_error(self, error_message: str):
        """
        渲染分析錯誤信息

        Args:
            error_message: 錯誤信息
        """
        st.error(f"分析薪資與地區時發生錯誤: {error_message}")
        st.info(ERROR_MESSAGES["general_error"])

    def render_salary_distribution(self, jobs_df: pd.DataFrame):
        """
        渲染薪資分佈圖表

        Args:
            jobs_df: 職缺數據DataFrame
        """
        display_salary_distribution(jobs_df)

    def render_location_distribution(self, jobs_df: pd.DataFrame):
        """
        渲染地區分佈圖表

        Args:
            jobs_df: 職缺數據DataFrame

        Returns:
            city_counts: 城市職缺數量
            district_counts: 地區職缺數量
        """
        # 分析城市分佈
        city_counts = display_city_distribution(jobs_df)

        # 分析地區分佈
        district_counts = display_district_distribution(jobs_df)

        return city_counts, district_counts

    def render_salary_location_relationship(
        self, jobs_df: pd.DataFrame, city_counts, district_counts
    ):
        """
        渲染薪資與地區關係圖表

        Args:
            jobs_df: 職缺數據DataFrame
            city_counts: 城市職缺數量
            district_counts: 地區職缺數量
        """
        display_city_salary_relationship(jobs_df, city_counts)
        display_district_salary_relationship(jobs_df, district_counts)

    def render_location_map(self, city_counts):
        """
        渲染地區分佈地圖

        Args:
            city_counts: 城市職缺數量
        """
        display_location_map(city_counts)


class SalaryLocationPage:
    """
    薪資與地區分析頁面，整合數據處理和UI渲染
    """

    def __init__(self, job_data_analyzer):
        """
        初始化薪資與地區分析頁面

        Args:
            job_data_analyzer: JobDataAnalyzer實例，用於從數據庫獲取職缺數據
        """
        self.data_processor = SalaryLocationDataProcessor(job_data_analyzer)
        self.renderer = SalaryLocationPageRenderer()

    def show(
        self,
        keywords=None,
        city=None,
        district=None,
        limit=DEFAULT_JOB_LIMIT,
        months=None,
    ):
        """
        顯示薪資與地區分析頁面

        Args:
            keywords: 用於過濾職缺的關鍵詞列表
            city: 用於過濾職缺的城市
            district: 用於過濾職缺的地區
            limit: 最大獲取職缺數量
            months: 如果提供，只獲取最近N個月的職缺
        """
        logger.info("顯示薪資與地區分析頁面")

        # 渲染頁面標題和過濾條件
        self.renderer.render_page_header()
        self.renderer.render_filter_info(keywords, city, district, months)

        try:
            # 載入數據
            jobs_df = self.data_processor.load_job_data(
                keywords, city, district, limit, months
            )

            if jobs_df is None:
                self.renderer.render_no_data_warning()
                return

            # 顯示數據載入狀態
            self.renderer.render_data_loading_status(len(jobs_df))

            # 檢查所需列是否存在
            columns_check = self.data_processor.has_required_columns(jobs_df)

            # 分析薪資分佈
            if columns_check["salary"]:
                self.renderer.render_salary_distribution(jobs_df)
            else:
                self.renderer.render_missing_column_info("salary")

            # 分析地區分佈
            if columns_check["location"]:
                city_counts, district_counts = (
                    self.renderer.render_location_distribution(jobs_df)
                )

                # 分析薪資與地區關係
                if columns_check["salary"]:
                    self.renderer.render_salary_location_relationship(
                        jobs_df, city_counts, district_counts
                    )

                # 創建地區分佈地圖
                self.renderer.render_location_map(city_counts)
            else:
                logger.warning(ERROR_MESSAGES["no_location_data"])
                self.renderer.render_missing_column_info("location")

        except Exception as e:
            # 記錄錯誤信息
            logger.error(f"顯示薪資與地區分析頁面時發生錯誤: {str(e)}", exc_info=True)
            self.renderer.render_analysis_error(str(e))


def show_salary_location_page(
    job_data_analyzer,
    keywords=None,
    city=None,
    district=None,
    limit=DEFAULT_JOB_LIMIT,
    months=None,
):
    """
    顯示薪資與地區分析頁面，分析不同薪資區間和地區的職缺分佈情況。

    Args:
        job_data_analyzer: JobDataAnalyzer instance for data processing
        keywords: 用於過濾職缺的關鍵詞列表，默認為None
        city: 用於過濾職缺的城市，默認為None
        district: 用於過濾職缺的地區，默認為None
        limit: 最大獲取職缺數量，默認為DEFAULT_JOB_LIMIT
        months: 如果提供，只獲取最近N個月的職缺，默認為None
    """
    page = SalaryLocationPage(job_data_analyzer)
    page.show(keywords, city, district, limit, months)
