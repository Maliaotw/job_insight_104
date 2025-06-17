"""
104 職缺代碼表模組

此模組提供 104 人力銀行的地區、職務類別和產業代碼的常數、字典和枚舉類，
以及從 104 API 加載完整代碼表的功能。

代碼表來源：
- 地區代碼表：https://static.104.com.tw/category-tool/json/Area.json
- 職務類別代碼表：https://static.104.com.tw/category-tool/json/JobCat.json
- 產業代碼表：https://static.104.com.tw/category-tool/json/Indust.json
"""

import json
import requests
from pathlib import Path
from enum import Enum
from typing import Dict, Any, Optional, List, Union
import os
import threading

from config.settings import BASE_DIR, logger

# 代碼表 URL
AREA_JSON_URL = "https://static.104.com.tw/category-tool/json/Area.json"
JOB_CAT_JSON_URL = "https://static.104.com.tw/category-tool/json/JobCat.json"
INDUSTRY_JSON_URL = "https://static.104.com.tw/category-tool/json/Indust.json"

# 代碼表本地緩存路徑
CODE_TABLES_DIR = BASE_DIR / "data" / "code_tables"
CODE_TABLES_DIR.mkdir(parents=True, exist_ok=True)

#################################################
# 1. 常數命名（適用於直接引用代碼）
#################################################

# 地區代碼常數 (AREA_CODE)
AREA_CODE_TAIPEI = "6001001000"  # 台北市
AREA_CODE_NEW_TAIPEI = "6001002000"  # 新北市
AREA_CODE_TAOYUAN = "6001005000"  # 桃園市
AREA_CODE_HSINCHU_CITY = "6001006000"  # 新竹市
AREA_CODE_HSINCHU_COUNTY = "6001007000"  # 新竹縣
AREA_CODE_TAICHUNG = "6001008000"  # 台中市
AREA_CODE_TAINAN = "6001016000"  # 台南市
AREA_CODE_KAOHSIUNG = "6001017000"  # 高雄市

# 職務類別代碼常數 (JOB_CAT_CODE)
JOB_CAT_CODE_SOFTWARE_ENGINEER = "2007001000"  # 軟體工程師
JOB_CAT_CODE_DATA_ANALYST = "2007002000"  # 資料分析師
JOB_CAT_CODE_WEB_DEVELOPER = "2007003000"  # 網站開發工程師
JOB_CAT_CODE_FRONTEND_ENGINEER = "2007004000"  # 前端工程師
JOB_CAT_CODE_BACKEND_ENGINEER = "2007005000"  # 後端工程師
JOB_CAT_CODE_DATABASE_ENGINEER = "2007006000"  # 資料庫工程師
JOB_CAT_CODE_AI_ENGINEER = "2007007000"  # AI工程師

# 產業代碼常數 (INDUSTRY_CODE)
INDUSTRY_CODE_IT = "1001000000"  # 資訊科技業
INDUSTRY_CODE_FINANCE = "1002000000"  # 金融業
INDUSTRY_CODE_MANUFACTURING = "1003000000"  # 製造業
INDUSTRY_CODE_SERVICE = "1004000000"  # 服務業
INDUSTRY_CODE_EDUCATION = "1005000000"  # 教育業

#################################################
# 2. 字典映射（適用於代碼與描述的對應）
#################################################

# 地區代碼字典
AREA_CODES = {
    "6001001000": "台北市",
    "6001002000": "新北市",
    "6001005000": "桃園市",
    "6001006000": "新竹市",
    "6001007000": "新竹縣",
    "6001008000": "台中市",
    "6001016000": "台南市",
    "6001017000": "高雄市",
    # 其他地區代碼可以根據需要添加
}

# 地區名稱與代碼對應字典（反向查詢用）
AREA_NAMES = {v: k for k, v in AREA_CODES.items()}

# 職務類別代碼字典
JOB_CAT_CODES = {
    "2007001000": "軟體工程師",
    "2007002000": "資料分析師",
    "2007003000": "網站開發工程師",
    "2007004000": "前端工程師",
    "2007005000": "後端工程師",
    "2007006000": "資料庫工程師",
    "2007007000": "AI工程師",
    # 其他職務類別代碼可以根據需要添加
}

# 職務類別名稱與代碼對應字典（反向查詢用）
JOB_CAT_NAMES = {v: k for k, v in JOB_CAT_CODES.items()}

# 產業代碼字典
INDUSTRY_CODES = {
    "1001000000": "資訊科技業",
    "1002000000": "金融業",
    "1003000000": "製造業",
    "1004000000": "服務業",
    "1005000000": "教育業",
    # 其他產業代碼可以根據需要添加
}

# 產業名稱與代碼對應字典（反向查詢用）
INDUSTRY_NAMES = {v: k for k, v in INDUSTRY_CODES.items()}

#################################################
# 3. 枚舉類（適用於代碼的分類管理）
#################################################


# 地區代碼枚舉
class AreaCode(Enum):
    TAIPEI = "6001001000"  # 台北市
    NEW_TAIPEI = "6001002000"  # 新北市
    TAOYUAN = "6001005000"  # 桃園市
    HSINCHU_CITY = "6001006000"  # 新竹市
    HSINCHU_COUNTY = "6001007000"  # 新竹縣
    TAICHUNG = "6001008000"  # 台中市
    TAINAN = "6001016000"  # 台南市
    KAOHSIUNG = "6001017000"  # 高雄市
    # 其他地區可以根據需要添加


# 職務類別代碼枚舉
class JobCatCode(Enum):
    SOFTWARE_ENGINEER = "2007001000"  # 軟體工程師
    DATA_ANALYST = "2007002000"  # 資料分析師
    WEB_DEVELOPER = "2007003000"  # 網站開發工程師
    FRONTEND_ENGINEER = "2007004000"  # 前端工程師
    BACKEND_ENGINEER = "2007005000"  # 後端工程師
    DATABASE_ENGINEER = "2007006000"  # 資料庫工程師
    AI_ENGINEER = "2007007000"  # AI工程師
    # 其他職務類別可以根據需要添加


# 產業代碼枚舉
class IndustryCode(Enum):
    IT = "1001000000"  # 資訊科技業
    FINANCE = "1002000000"  # 金融業
    MANUFACTURING = "1003000000"  # 製造業
    SERVICE = "1004000000"  # 服務業
    EDUCATION = "1005000000"  # 教育業
    # 其他產業可以根據需要添加


#################################################
# 4. 代碼表加載與使用
#################################################


def load_code_table(
    url: str, cache_filename: str, force_refresh: bool = False
) -> Dict[str, Any]:
    """
    從 URL 加載代碼表，並緩存到本地文件

    參數:
        url: 代碼表的URL
        cache_filename: 本地緩存文件名
        force_refresh: 是否強制從URL重新加載，忽略本地緩存

    返回:
        Dict: 代碼表數據
    """
    cache_path = CODE_TABLES_DIR / cache_filename

    # 如果本地緩存存在且不強制刷新，則從緩存加載
    if cache_path.exists() and not force_refresh:
        logger.debug(f"從本地緩存加載代碼表: {cache_filename}")
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"讀取緩存文件失敗: {e}，將重新從URL加載")

    # 從 URL 加載並緩存
    logger.info(f"從URL加載代碼表: {url}")
    try:
        print(f"准备请求: {url}")
        response = requests.get(
            url,
            timeout=10,
            verify=False,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )
        print(f"收到响应状态码: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        print(f"成功解析JSON数据, 数据类型: {type(data)}")

        # 保存到本地緩存
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.debug(f"代碼表已緩存到: {cache_path}")
        return data
    except Exception as e:
        logger.error(f"從URL加載代碼表失敗: {e}")
        # 如果緩存存在但可能已損壞，嘗試刪除它
        if cache_path.exists() and force_refresh:
            try:
                os.remove(cache_path)
                logger.warning(f"已刪除可能損壞的緩存文件: {cache_path}")
            except:
                pass

        # 如果緩存存在，嘗試使用緩存
        if cache_path.exists():
            logger.warning("嘗試使用可能過期的緩存...")
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except:
                pass

        # 如果所有嘗試都失敗，返回空數據結構
        logger.error("無法加載代碼表，返回空數據結構")
        return {"data": []}


def load_area_codes(force_refresh: bool = False) -> Dict[str, Any]:
    """
    加載地區代碼表

    參數:
        force_refresh: 是否強制從URL重新加載

    返回:
        Dict: 地區代碼表數據
    """
    return load_code_table(AREA_JSON_URL, "area_codes.json", force_refresh)


def load_job_cat_codes(force_refresh: bool = False) -> Dict[str, Any]:
    """
    加載職務類別代碼表

    參數:
        force_refresh: 是否強制從URL重新加載

    返回:
        Dict: 職務類別代碼表數據
    """
    return load_code_table(JOB_CAT_JSON_URL, "job_cat_codes.json", force_refresh)


def load_industry_codes(force_refresh: bool = False) -> Dict[str, Any]:
    """
    加載產業代碼表

    參數:
        force_refresh: 是否強制從URL重新加載

    返回:
        Dict: 產業代碼表數據
    """
    return load_code_table(INDUSTRY_JSON_URL, "industry_codes.json", force_refresh)


def extract_codes_from_data(
    data: Union[Dict[str, Any], List[Dict[str, Any]]], parent_code: str = ""
) -> Dict[str, str]:
    """
    從API返回的數據中提取代碼和名稱

    參數:
        data: API返回的數據，可能是字典或列表
        parent_code: 父級代碼，用於處理層級結構

    返回:
        Dict[str, str]: 代碼到名稱的映射字典
    """
    result = {}

    # 處理當前層級的項目
    if isinstance(data, dict) and "data" in data:
        # 如果數據是字典且有data鍵，使用data的值
        items = data.get("data", [])
    elif isinstance(data, list):
        # 如果數據直接是列表，直接使用
        items = data
    else:
        # 其他情況，使用空列表
        items = []

    for item in items:
        if not isinstance(item, dict):
            continue

        code = item.get("no", "")
        name = item.get("des", "")

        if code and name:
            # 如果有父級代碼，則添加完整路徑
            full_code = code
            if parent_code:
                # 某些API返回的代碼可能已經包含父級代碼
                if not code.startswith(parent_code):
                    full_code = f"{parent_code}.{code}"

            result[full_code] = name

        # 遞歸處理子項目
        children = item.get("children", [])
        if children:
            child_results = extract_codes_from_data(children, code)
            result.update(child_results)

        # 處理子地區或子類別 (n 鍵)
        sub_items = item.get("n", [])
        if sub_items:
            # 遞歸處理子項目，確保所有層級的子項目都被提取
            sub_results = extract_codes_from_data(sub_items, "")
            result.update(sub_results)

    return result


def build_full_code_tables(force_refresh: bool = False) -> Dict[str, Dict[str, str]]:
    """
    構建完整的代碼表字典

    參數:
        force_refresh: 是否強制從URL重新加載

    返回:
        Dict: 包含所有代碼表的字典
    """
    try:
        # 加載完整代碼表
        area_data = load_area_codes(force_refresh)
        job_cat_data = load_job_cat_codes(force_refresh)
        industry_data = load_industry_codes(force_refresh)

        # 構建完整的代碼-名稱映射
        full_area_codes = {}
        full_job_cat_codes = {}
        full_industry_codes = {}

        # 使用通用的提取函數處理所有代碼表
        area_codes_dict = extract_codes_from_data(area_data)
        full_area_codes.update(area_codes_dict)

        job_cat_codes_dict = extract_codes_from_data(job_cat_data)
        full_job_cat_codes.update(job_cat_codes_dict)

        industry_codes_dict = extract_codes_from_data(industry_data)
        full_industry_codes.update(industry_codes_dict)

        # 如果提取的代碼表為空，使用預定義的代碼表作為備用
        if not full_area_codes:
            full_area_codes = AREA_CODES
        if not full_job_cat_codes:
            full_job_cat_codes = JOB_CAT_CODES
        if not full_industry_codes:
            full_industry_codes = INDUSTRY_CODES

        return {
            "area_codes": full_area_codes,
            "job_cat_codes": full_job_cat_codes,
            "industry_codes": full_industry_codes,
        }
    except Exception as e:
        logger.error(f"構建完整代碼表失敗: {e}")
        return {
            "area_codes": AREA_CODES,
            "job_cat_codes": JOB_CAT_CODES,
            "industry_codes": INDUSTRY_CODES,
        }


def get_code_name(
    code_type: str, code: str, use_full_table: bool = False
) -> Optional[str]:
    """
    根據代碼類型和代碼獲取名稱

    參數:
        code_type: 代碼類型，可以是 'area', 'job_cat', 或 'industry'
        code: 代碼值
        use_full_table: 是否使用完整代碼表查詢

    返回:
        Optional[str]: 代碼對應的名稱，如果找不到則返回None
    """
    if use_full_table:
        # 使用完整代碼表查詢
        all_codes = build_full_code_tables()
        if code_type == "area":
            return all_codes["area_codes"].get(code)
        elif code_type == "job_cat":
            return all_codes["job_cat_codes"].get(code)
        elif code_type == "industry":
            return all_codes["industry_codes"].get(code)
    else:
        # 使用預定義的代碼表查詢
        if code_type == "area":
            return AREA_CODES.get(code)
        elif code_type == "job_cat":
            return JOB_CAT_CODES.get(code)
        elif code_type == "industry":
            return INDUSTRY_CODES.get(code)
    return None


def get_code_by_name(
    code_type: str, name: str, use_full_table: bool = False
) -> Optional[str]:
    """
    根據代碼類型和名稱獲取代碼

    參數:
        code_type: 代碼類型，可以是 'area', 'job_cat', 或 'industry'
        name: 名稱
        use_full_table: 是否使用完整代碼表查詢

    返回:
        Optional[str]: 名稱對應的代碼，如果找不到則返回None
    """
    if use_full_table:
        # 使用完整代碼表查詢
        all_codes = build_full_code_tables()
        if code_type == "area":
            codes_dict = all_codes["area_codes"]
            for code, code_name in codes_dict.items():
                if code_name == name:
                    return code
        elif code_type == "job_cat":
            codes_dict = all_codes["job_cat_codes"]
            for code, code_name in codes_dict.items():
                if code_name == name:
                    return code
        elif code_type == "industry":
            codes_dict = all_codes["industry_codes"]
            for code, code_name in codes_dict.items():
                if code_name == name:
                    return code
    else:
        # 使用預定義的代碼表查詢
        if code_type == "area":
            return AREA_NAMES.get(name)
        elif code_type == "job_cat":
            return JOB_CAT_NAMES.get(name)
        elif code_type == "industry":
            return INDUSTRY_NAMES.get(name)
    return None


def search_code_by_keyword(
    code_type: str, keyword: str, use_full_table: bool = True
) -> Dict[str, str]:
    """
    根據關鍵字搜索代碼

    參數:
        code_type: 代碼類型，可以是 'area', 'job_cat', 或 'industry'
        keyword: 搜索關鍵字
        use_full_table: 是否使用完整代碼表搜索

    返回:
        Dict[str, str]: 符合關鍵字的代碼和名稱字典
    """
    result = {}

    if use_full_table:
        # 使用完整代碼表搜索
        all_codes = build_full_code_tables()
        if code_type == "area":
            codes_dict = all_codes["area_codes"]
        elif code_type == "job_cat":
            codes_dict = all_codes["job_cat_codes"]
        elif code_type == "industry":
            codes_dict = all_codes["industry_codes"]
        else:
            return result

        # 搜索包含關鍵字的名稱
        for code, name in codes_dict.items():
            if keyword.lower() in name.lower():
                result[code] = name
    else:
        # 使用預定義的代碼表搜索
        if code_type == "area":
            codes_dict = AREA_CODES
        elif code_type == "job_cat":
            codes_dict = JOB_CAT_CODES
        elif code_type == "industry":
            codes_dict = INDUSTRY_CODES
        else:
            return result

        # 搜索包含關鍵字的名稱
        for code, name in codes_dict.items():
            if keyword.lower() in name.lower():
                result[code] = name

    return result


if __name__ == "__main__":
    build_full_code_tables()
