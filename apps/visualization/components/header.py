"""
頁面標題和介紹元件模組。

此模組提供創建應用程序標題和介紹的功能。
遵循元件化設計原則，將UI元件與業務邏輯分離。

作者: Job Insight 104 Team
版本: 2.0.0
"""

import streamlit as st
from typing import Optional, Dict
from datetime import datetime
from config.settings import logger


def create_title(title: str = "📊 104 職缺市場洞察平台") -> None:
    """
    創建頁面標題。

    參數:
        title: 頁面標題文字
    """
    st.title(title)


def create_introduction(introduction: Optional[str] = None) -> None:
    """
    創建頁面介紹文字。

    參數:
        introduction: 介紹文字，如果為None則使用默認介紹
    """
    if introduction is None:
        introduction = """
        這個平台提供基於 104 職缺數據的市場分析與視覺化功能，幫助您了解台灣就業市場的動態變化。
        """

    st.markdown(introduction)


def create_subheader(page: str) -> None:
    """
    根據當前頁面創建動態子標題。

    參數:
        page: 當前頁面名稱
    """
    # 頁面對應的子標題
    page_subheaders = {
        "總覽 Dashboard": "市場概況與關鍵指標",
        "每日職缺變化分析": "職缺數量變化趨勢與分析",
        "產業職缺分佈與趨勢": "各產業職缺分佈與發展趨勢",
        "招聘效率分析": "企業招聘流程效率指標",
        "薪資與地區分析": "各地區與職位薪資水平比較"
    }

    # 獲取當前頁面的子標題
    subheader = page_subheaders.get(page, "")

    if subheader:
        st.subheader(subheader)


def create_last_updated_info() -> None:
    """
    創建最後更新時間信息。
    """
    # 獲取當前時間作為最後更新時間
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 在右側顯示最後更新時間
    col1, col2 = st.columns([3, 1])
    with col2:
        st.caption(f"最後更新: {last_updated}")


def create_help_button() -> None:
    """
    創建幫助按鈕，為新用戶提供使用指南。
    """
    with st.expander("💡 使用指南", expanded=False):
        st.markdown("""
        ### 如何使用本平台

        1. **選擇分析頁面**: 在左側導航欄選擇您想查看的分析頁面
        2. **設置過濾條件**: 使用左側過濾器選擇關鍵詞、城市、地區等
        3. **查看分析結果**: 在主頁面查看數據視覺化和分析結果
        4. **互動探索**: 點擊圖表元素可查看更詳細的信息

        ### 常見問題

        - **數據來源**: 所有數據來自104人力銀行網站
        - **更新頻率**: 數據每日自動更新
        - **過濾條件**: 可以組合多個過濾條件進行精確分析
        """)


def create_header(title: str = "📊 104 職缺市場洞察平台", introduction: Optional[str] = None, page: str = "總覽 Dashboard") -> None:
    """
    創建頁面標題和介紹。

    參數:
        title: 頁面標題文字
        introduction: 介紹文字，如果為None則使用默認介紹
        page: 當前頁面名稱，用於創建動態子標題
    """
    try:
        # 創建標題和介紹
        create_title(title)
        create_introduction(introduction)

        # 創建最後更新時間信息
        create_last_updated_info()

        # 創建動態子標題
        create_subheader(page)

        # 創建幫助按鈕
        create_help_button()

        logger.debug(f"創建頁面標題: {title}, 頁面: {page}")
    except Exception as e:
        logger.error(f"創建頁面標題時發生錯誤: {str(e)}", exc_info=True)
        st.error("載入頁面標題時發生錯誤。")
