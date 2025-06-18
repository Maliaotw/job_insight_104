import json
import re
from typing import Tuple

from config.code_tables import CODE_TABLES_DIR
from config.settings import logger


def extract_lowest_level_area_codes(json_path=None):
    """
    從 area_codes.json 提取台灣地區下所有最下層的地區代碼

    參數:
        json_path: area_codes.json 的路徑，默認為 None（使用預設路徑）

    返回:
        Dict[str, str]: 地區代碼與地區名稱的映射字典

    實作細節:
        - 讀取地區代碼JSON文件
        - 找出台灣地區的數據（代碼為6001000000）
        - 遞迴遍歷所有子地區，提取最下層地區代碼
        - 只保留包含目標城市（台北市、新北市、桃園、新竹）的地區
    """
    if json_path is None:
        json_path = CODE_TABLES_DIR / "area_codes.json"

    try:
        # 讀取 area_codes.json 文件
        with open(json_path, "r", encoding="utf-8") as f:
            area_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"讀取 {json_path} 文件失敗: {e}")
        return {}

    # 確保我們只處理台灣地區的數據
    taiwan_data = next(
        (item for item in area_data if item.get("no") == "6001000000"), None
    )

    if not taiwan_data:
        logger.warning("未找到台灣地區數據，將返回空字典")
        return {}

    # 定義目標城市列表，便於維護
    TARGET_CITIES = {"台北市", "新北市", "桃園", "新竹"}

    area_code_to_name_map = {}

    def extract_codes(items):
        """遞迴函數用於提取最下層地區代碼"""
        for item in items:
            if not isinstance(item, dict):
                continue

            # 檢查是否有子地區
            children = item.get("n")
            if children:
                # 如果有子地區，遞迴處理
                extract_codes(children)
            else:
                # 如果沒有子地區，這是最下層地區
                description = item.get("des", "")
                area_code = item.get("no", "")

                # 檢查是否包含目標城市
                if area_code and any(city in description for city in TARGET_CITIES):
                    area_code_to_name_map[area_code] = description

    # 開始遞迴提取台灣地區下的所有地區
    children = taiwan_data.get("n")
    if children:
        extract_codes(children)

    logger.info(f"提取到 {len(area_code_to_name_map)} 個最下層地區代碼")
    return area_code_to_name_map


def split_link_field(link_str: str) -> Tuple[str, str, str]:
    """
    將link字段分割為applyAnalyze、job和cust三個部分。

    參數:
        link_str: 要分割的link字段，例如：
        "{'applyAnalyze': //www.104.com.tw/jobs/apply/analysis/5znmj?channel=104rpt&jobsource=hotjob_chr,
          'job': //www.104.com.tw/job/5znmj?jobsource=hotjob_chr,
          'cust': //www.104.com.tw/company/oye0i0?jobsource=hotjob_chr}"

    返回:
        包含 (applyAnalyze, job, cust) 的元組。如果無法解析，則返回空字符串。

    實作細節:
        - 使用正則表達式從字符串中提取三個URL部分
        - 分別匹配'applyAnalyze'、'job'和'cust'後面的URL
        - 確保URL格式正確，添加https:前綴（如果缺少）
        - 處理異常情況，確保即使解析失敗也能返回有效結果
    """
    try:
        # 使用正則表達式提取三個URL
        apply_analyze_url_pattern = r"'applyAnalyze':\s*(//[^,}]+)"
        job_url_pattern = r"'job':\s*(//[^,}]+)"
        company_url_pattern = r"'cust':\s*(//[^,}]+)"

        apply_analyze_match = re.search(apply_analyze_url_pattern, link_str)
        job_match = re.search(job_url_pattern, link_str)
        company_match = re.search(company_url_pattern, link_str)

        # 提取匹配的URL，如果沒有匹配則返回空字符串
        apply_analyze_url = apply_analyze_match.group(1) if apply_analyze_match else ""
        job_url = job_match.group(1) if job_match else ""
        company_url = company_match.group(1) if company_match else ""

        # 添加協議前綴
        if apply_analyze_url and not apply_analyze_url.startswith(("http:", "https:")):
            apply_analyze_url = "https:" + apply_analyze_url
        if job_url and not job_url.startswith(("http:", "https:")):
            job_url = "https:" + job_url
        if company_url and not company_url.startswith(("http:", "https:")):
            company_url = "https:" + company_url

        return apply_analyze_url, job_url, company_url
    except Exception as e:
        logger.error(f"解析link字段時出錯: {e}, 原始字段: {link_str}")
        return "", "", ""


def split_city_district(address: str) -> Tuple[str, str]:
    """
    將台灣地址分割為城市和地區。

    參數:
        address: 要分割的地址，例如："台中市西屯區"

    返回:
        包含 (城市, 地區) 的元組。如果地址不是城市，則返回 ("", address)。

    實作細節:
        - 使用正則表達式匹配台灣常見的地址格式
        - 支持兩種主要格式：
          1. 完整地址（如"台北市大安區"）- 返回城市和地區
          2. 僅城市（如"台北市"）- 返回城市和空字符串
        - 如果無法匹配任何已知格式，則返回空城市和原始地址
        - 處理各種縣市和區域的命名規則（市、縣、區、鄉、鎮）
    """
    # 台灣常見的城市格式
    city_patterns = [
        r"^(.*?[市縣])(.*?[區鄉鎮市])$",  # 針對類似"台北市大安區"的地址
        r"^(.*?[市縣])$",  # 針對只有城市的地址
    ]

    for pattern in city_patterns:
        match = re.match(pattern, address)
        if match:
            groups = match.groups()
            if len(groups) == 2:
                return groups[0], groups[1]  # 城市和地區
            elif len(groups) == 1:
                return groups[0], ""  # 只有城市

    # 如果沒有匹配的模式，則按原樣返回原始地址
    return "", address
