"""
104職缺市場洞察平台視覺化應用程序的主要文件。
此文件整合了所有獨立頁面並提供統一的界面。

此模組遵循MVC架構:
- 模型(Model): 數據分析器類別，負責業務邏輯和數據處理
- 視圖(View): 頁面模組和UI元件，負責用戶界面
- 控制器(Controller): 本模組，負責協調模型和視圖

作者: Job Insight 104 Team
版本: 2.0.0
"""

import streamlit as st
from typing import List, Optional, Dict, Any, Tuple, Union
from functools import partial
import time

# 分析器模組 (Model)

from apps.visualization.analysis.job_data_analyzer import JobDataAnalyzer
from apps.visualization.analysis.trend_analyzer import TrendAnalyzer

# 頁面模組 (View)
from apps.visualization.nav.daily_changes_page import show_daily_changes_page
from apps.visualization.nav.dashboard_page import show_dashboard_page
from apps.visualization.nav.hiring_efficiency_page import show_hiring_efficiency_page
from apps.visualization.nav.industry_trends_page import show_industry_trends_page
from apps.visualization.nav.salary_location_page import show_salary_location_page

# UI元件 (View)
from apps.visualization.components.sidebar import create_sidebar
from apps.visualization.components.header import create_header
from apps.visualization.components.footer import create_footer

# 配置和工具
from config.settings import logger, TAIWAN_CITY, CRAWLER_KEYWORDS
from src.database.duckdb_manager import DuckDBManager

# 應用程序常量
APP_TITLE = "104 職缺市場洞察平台"
APP_ICON = "📊"
APP_LAYOUT = "wide"
INITIAL_SIDEBAR_STATE = "expanded"

# 配置頁面設置
st.set_page_config(
    page_title=APP_TITLE,
    page_icon=APP_ICON,
    layout=APP_LAYOUT,
    initial_sidebar_state=INITIAL_SIDEBAR_STATE,
)


@st.cache_resource(show_spinner="正在連接到數據庫...")
def get_db_manager() -> Optional[DuckDBManager]:
    """
    獲取數據庫管理器實例。
    使用st.cache_resource裝飾器確保在會話期間只創建一個數據庫連接。

    返回:
        DuckDBManager: 數據庫管理器實例，如果連接失敗則返回None
    """
    try:
        logger.info("初始化數據庫連接")
        db_manager = DuckDBManager()
        logger.info("數據庫連接初始化成功")
        return db_manager
    except Exception as e:
        logger.error(f"初始化數據庫連接時發生錯誤: {str(e)}", exc_info=True)
        return None


@st.cache_resource(show_spinner="正在初始化分析器...")
def initialize_analyzers() -> Tuple[JobDataAnalyzer, TrendAnalyzer]:
    """
    初始化所有數據分析器。
    使用st.cache_resource裝飾器確保在會話期間只創建一個分析器實例。

    參數:
        _db_manager: 數據庫管理器實例，使用前導下劃線告訴Streamlit不要嘗試對此參數進行哈希處理

    返回:
        Tuple[JobDataAnalyzer, TrendAnalyzer, HiringAnalyzer]: 包含所有分析器的元組
    """
    logger.info("初始化數據分析器")
    job_data_analyzer = JobDataAnalyzer()
    trend_analyzer = TrendAnalyzer()
    logger.info("數據分析器初始化完成")
    return job_data_analyzer, trend_analyzer


def handle_page_navigation(
    page: str,
    job_data_analyzer: JobDataAnalyzer,
    trend_analyzer: TrendAnalyzer,
    keywords: List[str],
    city: Optional[str],
    district: Optional[str],
    limit: Union[str, int],
    months: Optional[int],
) -> None:
    """
    根據選擇的頁面顯示相應的內容。

    參數:
        page: 選擇的頁面名稱
        job_data_analyzer: 職缺數據分析器
        trend_analyzer: 趨勢分析器
        hiring_analyzer: 招聘效率分析器
        keywords: 關鍵詞列表
        city: 選擇的城市
        district: 選擇的地區
        limit: 最大獲取職缺數量
        months: 時間範圍（月）
    """
    # 頁面映射表，將頁面名稱映射到對應的處理函數
    page_handlers = {
        "總覽 Dashboard": lambda: show_dashboard_page(
            job_data_analyzer, keywords, city, district, limit, months
        ),
        "每日職缺變化分析": lambda: show_daily_changes_page(
            job_data_analyzer, trend_analyzer, keywords, city, district, limit, months
        ),
        "產業職缺分佈與趨勢": lambda: show_industry_trends_page(
            job_data_analyzer, trend_analyzer, keywords, city, district, limit, months
        ),
        "招聘效率分析": lambda: show_hiring_efficiency_page(
            job_data_analyzer, keywords, city, district, limit, months
        ),
        "薪資與地區分析": lambda: show_salary_location_page(
            job_data_analyzer, keywords, city, district, limit, months
        ),
    }

    # 記錄用戶選擇的頁面
    logger.info(f"用戶選擇頁面: {page}")
    logger.debug(
        f"頁面參數 - 關鍵詞: {keywords}, 城市: {city}, 地區: {district}, 限制: {limit}, 月份: {months}"
    )

    # 調用對應的頁面處理函數
    if page in page_handlers:
        page_handlers[page]()
    else:
        st.error(f"未知頁面: {page}")
        logger.error(f"用戶嘗試訪問未知頁面: {page}")


def main():
    """
    主應用程序函數，負責設置UI並處理頁面導航。
    遵循MVC架構，此函數作為控制器協調模型(分析器)和視圖(UI元件)。
    """
    start_time = time.time()
    logger.info("啟動104職缺市場洞察平台應用程序")

    try:
        # 初始化分析器 (Model)
        job_data_analyzer, trend_analyzer = initialize_analyzers()

        # 創建側邊欄 (View)
        sidebar_result = create_sidebar(CRAWLER_KEYWORDS, TAIWAN_CITY)

        # 從側邊欄獲取過濾條件
        keywords = sidebar_result.get("keywords", [])
        city = sidebar_result.get("city")
        district = sidebar_result.get("district")
        limit = sidebar_result.get("limit", "無限制")
        months = sidebar_result.get("months")
        page = sidebar_result.get("page", "總覽 Dashboard")

        # 創建頁面標題和介紹 (View)，傳入當前頁面
        create_header(page=page)

        # 根據選擇的頁面顯示相應的內容 (Controller)
        handle_page_navigation(
            page,
            job_data_analyzer,
            trend_analyzer,
            keywords,
            city,
            district,
            limit,
            months,
        )

        # 創建頁腳 (View)
        create_footer()

        # 記錄應用程序啟動時間
        elapsed_time = time.time() - start_time
        logger.info(f"應用程序啟動完成，耗時: {elapsed_time:.2f}秒")

    except Exception as e:
        logger.error(f"應用程序運行時發生錯誤: {str(e)}", exc_info=True)
        st.error(f"應用程序運行時發生錯誤: {str(e)}")
        st.info("如果問題持續存在，請聯繫系統管理員獲取幫助。")

        # 即使發生錯誤，也顯示頁腳
        create_footer()


if __name__ == "__main__":
    main()
