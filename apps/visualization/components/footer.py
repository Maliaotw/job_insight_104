"""
頁面頁腳元件模組。

此模組提供創建應用程序頁腳的功能，包括版權信息、技術堆棧信息、社交媒體連結等。
遵循元件化設計原則，將UI元件與業務邏輯分離。

作者: Job Insight 104 Team
版本: 2.0.0
"""

import streamlit as st
from typing import Optional, List, Dict
from datetime import datetime
from config.settings import logger


def create_tech_stack_info(tech_stack: Optional[List[str]] = None) -> None:
    """
    創建技術堆棧信息。

    參數:
        tech_stack: 技術堆棧列表，如果為None則使用默認列表
    """
    if tech_stack is None:
        tech_stack = ["Python", "DuckDB", "Streamlit", "Pandas", "Plotly"]

    # 使用更美觀的方式顯示技術堆棧
    st.sidebar.markdown("### 技術堆棧")

    # 將技術堆棧分成兩列顯示
    cols = st.sidebar.columns(2)
    for i, tech in enumerate(tech_stack):
        cols[i % 2].markdown(f"- {tech}")

    st.sidebar.info("本平台提供對 104 職缺數據的深入分析與視覺化。")


def create_social_media_links() -> None:
    """
    創建社交媒體連結。
    """
    st.sidebar.markdown("### 關注我們")

    # 定義社交媒體連結
    social_media = {
        "GitHub": "https://github.com/job-insight-104",
        "LinkedIn": "https://www.linkedin.com/company/job-insight-104",
        "Facebook": "https://www.facebook.com/jobinsight104",
        "Twitter": "https://twitter.com/jobinsight104",
    }

    # 創建社交媒體連結按鈕
    cols = st.sidebar.columns(4)
    for i, (platform, url) in enumerate(social_media.items()):
        cols[i].markdown(f"[{platform}]({url})")


def create_feedback_section() -> None:
    """
    創建反饋和問題報告部分。
    """
    st.sidebar.markdown("### 幫助我們改進")

    # 創建反饋表單連結
    feedback_url = "https://forms.gle/XYZ123"  # 替換為實際的表單URL
    st.sidebar.markdown(f"[📝 提供反饋]({feedback_url})")

    # 創建問題報告連結
    issue_url = (
        "https://github.com/job-insight-104/issues/new"  # 替換為實際的GitHub issues URL
    )
    st.sidebar.markdown(f"[🐞 報告問題]({issue_url})")


def create_copyright_info(year: Optional[int] = None) -> None:
    """
    創建版權信息。

    參數:
        year: 版權年份，如果為None則使用當前年份
    """
    if year is None:
        year = datetime.now().year

    st.sidebar.caption(f"© {year} Job Insight 104 Team. All Rights Reserved.")


def create_version_info(version: str = "2.0.0") -> None:
    """
    創建版本信息。

    參數:
        version: 應用程序版本
    """
    st.sidebar.caption(f"版本: {version}")


def create_footer(
    tech_stack: Optional[List[str]] = None,
    year: Optional[int] = None,
    version: str = "2.0.0",
) -> None:
    """
    創建頁面頁腳。

    參數:
        tech_stack: 技術堆棧列表，如果為None則使用默認列表
        year: 版權年份，如果為None則使用當前年份
        version: 應用程序版本
    """
    try:
        # 添加分隔線
        st.sidebar.markdown("---")

        # 創建技術堆棧信息
        create_tech_stack_info(tech_stack)

        # 創建社交媒體連結
        create_social_media_links()

        # 創建反饋部分
        create_feedback_section()

        # 添加分隔線
        st.sidebar.markdown("---")

        # 創建版權信息
        create_copyright_info(year)

        # 創建版本信息
        create_version_info(version)

        logger.debug("創建頁面頁腳")
    except Exception as e:
        logger.error(f"創建頁面頁腳時發生錯誤: {str(e)}", exc_info=True)
        # 不顯示錯誤，因為頁腳不是關鍵元件
