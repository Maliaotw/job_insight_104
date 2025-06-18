"""
側邊欄元件模組。

此模組提供創建應用程序側邊欄的功能，包括搜索過濾器、城市和地區選擇器以及頁面導航。
遵循元件化設計原則，將UI元件與業務邏輯分離。

作者: Job Insight 104 Team
版本: 1.0.0
"""

from typing import Any, Dict, List, Optional, Union

import streamlit as st

from config.settings import logger


def update_keywords(suggestion: str) -> None:
    """
    當用戶點擊關鍵詞建議時更新關鍵詞。

    參數:
        suggestion: 用戶選擇的關鍵詞建議
    """
    # 獲取當前關鍵詞
    current_keywords = st.session_state.search_keywords

    # 如果當前關鍵詞為空，直接設置為建議
    if not current_keywords:
        st.session_state.search_keywords = suggestion
    else:
        # 否則，替換最後一個關鍵詞為建議
        parts = current_keywords.split(",")
        parts[-1] = suggestion
        st.session_state.search_keywords = ",".join(parts)


def create_keyword_filter(keywords_choices: List[str]) -> List[str]:
    """
    創建關鍵詞過濾器元件。

    參數:
        keywords_choices: 可用的關鍵詞列表

    返回:
        List[str]: 用戶選擇的關鍵詞列表
    """
    st.sidebar.title("全局搜索")

    # 初始化session state以保存關鍵詞
    if "search_keywords" not in st.session_state:
        st.session_state.search_keywords = ""

    # 使用text_input搭配自動補全功能，並從session state獲取值
    search_keywords = st.sidebar.text_input(
        "關鍵詞 (可輸入多個，用逗號分隔)",
        value=st.session_state.search_keywords,
        key="search_keywords_input",
        help="留空表示搜尋全部關鍵詞，輸入時會自動提示可用的關鍵詞",
    )

    # 更新session state中的關鍵詞
    st.session_state.search_keywords = search_keywords

    # 顯示可用的關鍵詞作為提示
    if search_keywords and not search_keywords.endswith(","):
        # 獲取用戶當前正在輸入的關鍵詞（最後一個逗號後的文字）
        current_input = search_keywords.split(",")[-1].strip().lower()

        if current_input:
            # 過濾出符合當前輸入的關鍵詞建議
            suggestions = [
                k for k in keywords_choices if k.lower().startswith(current_input)
            ]

            if suggestions:
                st.sidebar.caption("建議的關鍵詞:")
                for suggestion in suggestions[:5]:  # 限制顯示前5個建議
                    if st.sidebar.button(
                        suggestion,
                        key=f"suggest_{suggestion}",
                        on_click=update_keywords,
                        args=(suggestion,),
                    ):
                        pass  # 按鈕點擊時會調用update_keywords函數

    # 顯示所有可用的關鍵詞
    with st.sidebar.expander("查看所有可用的關鍵詞"):
        all_keywords_text = ", ".join(keywords_choices)
        st.write(all_keywords_text)

    # 處理關鍵詞
    keywords = []
    if search_keywords:
        keywords = [k.strip() for k in search_keywords.split(",") if k.strip()]

    return keywords


def create_limit_filter() -> Union[str, int]:
    """
    創建數據限制過濾器元件。

    返回:
        Union[str, int]: 用戶選擇的數據限制
    """
    limit_options = ["無限制", 1000, 5000, 10000, 20000]
    limit = st.sidebar.selectbox("最大獲取職缺數量", limit_options, index=0)
    return limit


def create_time_filter() -> Optional[int]:
    """
    創建時間範圍過濾器元件。

    返回:
        Optional[int]: 用戶選擇的時間範圍（月），如果選擇全部時間則為None
    """
    months_options = [
        ("全部時間", None),
        ("最近1個月", 1),
        ("最近3個月", 3),
        ("最近6個月", 6),
        ("最近12個月", 12),
    ]
    selected_months = st.sidebar.selectbox(
        "時間範圍", options=[label for label, _ in months_options], index=0
    )
    months = dict(months_options)[selected_months]
    return months


def create_location_filter(
    cities: List[str], all_districts: Dict[str, List[str]]
) -> tuple:
    """
    創建位置過濾器元件。

    參數:
        cities: 城市列表
        all_districts: 城市到地區的映射

    返回:
        tuple: (city, district) 用戶選擇的城市和地區
    """
    # 創建城市選擇下拉框
    selected_city = st.sidebar.selectbox(
        "選擇城市進行分析", ["全部城市"] + cities, key="sidebar_city"
    )
    logger.debug(f"用戶選擇的城市: {selected_city}")

    # 根據選擇的城市獲取對應的地區列表
    districts = all_districts.get(selected_city, [])

    # 創建地區選擇下拉框
    selected_district = st.sidebar.selectbox(
        "選擇地區進行分析", ["全部地區"] + districts, key="sidebar_district"
    )
    logger.debug(f"用戶選擇的地區: {selected_district}")

    # 根據選擇設置城市和地區變數
    city = None if selected_city == "全部城市" else selected_city
    district = None if selected_district == "全部地區" else selected_district

    return city, district


def create_page_navigation() -> str:
    """
    創建頁面導航元件。

    返回:
        str: 用戶選擇的頁面
    """
    st.sidebar.title("導覽")

    # 定義頁面選項和對應的圖標
    page_options = [
        "總覽 Dashboard",
        "每日職缺變化分析",
        "產業職缺分佈與趨勢",
        "招聘效率分析",
        "薪資與地區分析",
    ]

    page_icons = {
        "總覽 Dashboard": "📊",
        "每日職缺變化分析": "📈",
        "產業職缺分佈與趨勢": "🏢",
        "招聘效率分析": "⏱️",
        "薪資與地區分析": "💰",
    }

    # 使用帶有圖標的選項
    page_display_options = [f"{page_icons[page]} {page}" for page in page_options]

    # 創建帶有圖標的頁面選擇器
    selected_page_with_icon = st.sidebar.radio(
        "請選擇分析頁面", page_display_options, format_func=lambda x: x
    )

    # 提取頁面名稱（去除圖標）
    selected_page = selected_page_with_icon[2:].strip()

    # 顯示當前頁面的簡短說明
    page_descriptions = {
        "總覽 Dashboard": "查看職缺市場的整體概況和關鍵指標",
        "每日職缺變化分析": "分析職缺數量的每日變化趨勢",
        "產業職缺分佈與趨勢": "探索不同產業的職缺分佈和發展趨勢",
        "招聘效率分析": "分析企業招聘流程的效率指標",
        "薪資與地區分析": "比較不同地區和職位的薪資水平",
    }

    st.sidebar.info(page_descriptions[selected_page])

    return selected_page


def display_filter_summary(
    keywords: List[str],
    city: Optional[str],
    district: Optional[str],
    months: Optional[int],
    limit: Union[str, int],
) -> None:
    """
    顯示過濾條件摘要。

    參數:
        keywords: 關鍵詞列表
        city: 選擇的城市
        district: 選擇的地區
        months: 時間範圍（月）
        limit: 最大獲取職缺數量
    """
    # 顯示當前過濾條件摘要
    st.sidebar.title("當前過濾條件")

    if keywords:
        st.sidebar.write(f"關鍵詞: {', '.join(keywords)}")
    else:
        st.sidebar.write("關鍵詞: 全部")

    if city:
        st.sidebar.write(f"城市: {city}")
    else:
        st.sidebar.write("城市: 全部")

    if district:
        st.sidebar.write(f"地區: {district}")
    else:
        st.sidebar.write("地區: 全部")

    if months:
        st.sidebar.write(f"時間範圍: 最近{months}個月")
    else:
        st.sidebar.write("時間範圍: 全部時間")

    st.sidebar.write(f"最大獲取職缺數量: {limit}")


def reset_filters():
    """
    重置所有過濾條件到默認值。
    """
    if "search_keywords" in st.session_state:
        st.session_state.search_keywords = ""
    if "sidebar_city" in st.session_state:
        st.session_state.sidebar_city = "全部城市"
    if "sidebar_district" in st.session_state:
        st.session_state.sidebar_district = "全部地區"
    # 重置其他過濾器的session state
    st.session_state.filter_reset = True


def create_sidebar(
    keywords_choices: List[str], taiwan_city: Dict[str, List[str]]
) -> Dict[str, Any]:
    """
    創建應用程序側邊欄。

    參數:
        keywords_choices: 可用的關鍵詞列表
        taiwan_city: 台灣城市和地區數據

    返回:
        Dict[str, Any]: 包含所有側邊欄選擇的字典
    """
    try:
        # 初始化重置標誌
        if "filter_reset" not in st.session_state:
            st.session_state.filter_reset = False

        # 創建過濾器標題和重置按鈕
        col1, col2 = st.sidebar.columns([3, 1])
        with col1:
            st.title("過濾條件")
        with col2:
            st.button("重置", on_click=reset_filters, help="重置所有過濾條件到默認值")

        # 創建關鍵詞過濾器
        keywords = create_keyword_filter(keywords_choices)

        # 創建高級過濾器的可折疊部分
        with st.sidebar.expander("高級過濾選項", expanded=False):
            # 創建數據限制過濾器
            limit = create_limit_filter()

            # 創建時間範圍過濾器
            months = create_time_filter()

        # 使用傳入的台灣城市數據
        cities = sorted(list(taiwan_city.keys()))
        all_districts = {}

        # 為每個城市獲取地區
        for city, districts in taiwan_city.items():
            all_districts[city] = sorted(districts)

        # 獲取所有地區（用於"全部城市"選項）
        all_districts["全部城市"] = sorted(
            list(
                {
                    district
                    for districts in taiwan_city.values()
                    for district in districts
                }
            )
        )

        # 創建位置過濾器
        city, district = create_location_filter(cities, all_districts)

        # 創建頁面導航
        page = create_page_navigation()

        # 顯示過濾條件摘要
        display_filter_summary(keywords, city, district, months, limit)

        # 如果過濾器被重置，清除重置標誌
        if st.session_state.filter_reset:
            st.session_state.filter_reset = False
            st.rerun()

        # 返回所有側邊欄選擇
        return {
            "keywords": keywords,
            "city": city,
            "district": district,
            "limit": limit,
            "months": months,
            "page": page,
        }
    except Exception as e:
        logger.error(f"創建側邊欄時發生錯誤: {str(e)}", exc_info=True)
        st.sidebar.error(f"載入側邊欄時發生錯誤: {str(e)}")

        # 返回默認值
        return {
            "keywords": [],
            "city": None,
            "district": None,
            "limit": "無限制",
            "months": None,
            "page": "總覽 Dashboard",
        }
