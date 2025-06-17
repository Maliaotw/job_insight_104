"""
Filter information component for the 104 Job Insight visualization app.
This component displays filter information for job listings.
"""

import streamlit as st

from config.settings import logger


def display_filter_info(keywords, city, district, months):
    """
    顯示過濾條件信息

    參數:
        keywords: 關鍵詞列表
        city: 城市名稱
        district: 地區名稱
        months: 月份數量
    """
    # 記錄顯示過濾條件
    logger.debug("顯示搜尋條件區塊")
    st.subheader("搜尋條件")
    filter_info = []
    if keywords:
        filter_info.append(f"關鍵詞: '{', '.join(keywords)}'")
    if city:
        filter_info.append(f"城市: {city}")
    if district:
        filter_info.append(f"地區: {district}")
    if months:
        filter_info.append(f"最近 {months} 個月")

    if filter_info:
        logger.debug(f"過濾條件: {', '.join(filter_info)}")
        st.info(f"正在分析符合以下條件的職缺: {', '.join(filter_info)}")
