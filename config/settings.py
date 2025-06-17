"""
104 職缺數據洞察平台配置
"""

from pathlib import Path
import sys
import logging
import os
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

# 加載.env文件中的環境變量
load_dotenv()

# 程序主目錄
BASE_DIR = Path(__file__).resolve().parent.parent

# 確保日誌目錄存在
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 從環境變量獲取配置，使用平舖的鍵名
# 日誌設置
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "DEBUG")
LOGGING_ROOT_LEVEL = os.environ.get("LOGGING_ROOT_LEVEL", LOGGING_LEVEL)
LOGGING_CONSOLE_LEVEL = os.environ.get("LOGGING_CONSOLE_LEVEL", LOGGING_LEVEL)
LOGGING_FILE_LEVEL = os.environ.get("LOGGING_FILE_LEVEL", LOGGING_LEVEL)

# MongoDB 設置
MONGODB_CONNECTION_STRING = os.environ.get("MONGODB_CONNECTION_STRING")
MONGODB_DB_NAME = os.environ.get("MONGODB_DB_NAME", "job_insight_104")
MONGODB_AUTH_SOURCE = os.environ.get("MONGODB_AUTH_SOURCE", "admin")

# 數據庫設置
DATABASE_PROCESSED_DATA_PATH = BASE_DIR / os.environ.get(
    "DATABASE_PROCESSED_DATA_PATH", "data/processed_job_data.duckdb"
)

# 爬蟲設置
CRAWLER_KEYWORDS_STR = os.environ.get(
    "CRAWLER_KEYWORDS", "flask,Python,DevOps,SRE,fastapi,django"
)
CRAWLER_KEYWORDS = [keyword.strip() for keyword in CRAWLER_KEYWORDS_STR.split("|")]
CRAWLER_SCHEDULE_DAILY_CRAWL_HOUR = int(
    os.environ.get("CRAWLER_SCHEDULE_DAILY_CRAWL_HOUR", "10")
)
CRAWLER_SCHEDULE_DAILY_CRAWL_MINUTE = int(
    os.environ.get("CRAWLER_SCHEDULE_DAILY_CRAWL_MINUTE", "0")
)

CURRENT_ENV = os.environ.get("CURRENT_ENV", "local")

AWS_S3_BUCKET = os.environ.get("AWS_S3_BUCKET")
AWS_S3_ACCESS_KEY_ID = os.environ.get("AWS_S3_ACCESS_KEY_ID")
AWS_S3_SECRET_ACCESS_KEY = os.environ.get("AWS_S3_SECRET_ACCESS_KEY")
AWS_S3_REGION_NAME = os.environ.get("AWS_S3_REGION_NAME")


# 配置日誌
def setup_logging(config=None):
    """設置日誌系統，包括控制台和文件輸出"""
    if config is None:
        # 使用平舖的配置
        root_level = getattr(logging, LOGGING_ROOT_LEVEL)
        console_level = getattr(logging, LOGGING_CONSOLE_LEVEL)
        file_level = getattr(logging, LOGGING_FILE_LEVEL)
    else:
        # 兼容舊的嵌套配置結構
        log_levels = config.get("logging", {})
        root_level = getattr(logging, log_levels.get("root_level", "DEBUG"))
        console_level = getattr(logging, log_levels.get("console_level", "INFO"))
        file_level = getattr(logging, log_levels.get("file_level", "DEBUG"))

    # 創建日誌格式
    log_format = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(process)d-%(thread)d | %(name)s | %(filename)s:%(lineno)d | %(message)s"
    )

    # 獲取根日誌記錄器
    root_logger = logging.getLogger()
    root_logger.setLevel(root_level)

    # 清除現有的處理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 添加控制台處理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    console_handler.setLevel(console_level)
    root_logger.addHandler(console_handler)

    return root_logger


# 配置字典已移除，直接使用變量
# 如需配置信息，請直接導入相應的變量

TAIWAN_CITY = {
    "台北市": [
        "中正區",
        "大同區",
        "中山區",
        "松山區",
        "大安區",
        "萬華區",
        "信義區",
        "士林區",
        "北投區",
        "內湖區",
        "南港區",
        "文山區",
    ],
    "新北市": [
        "板橋區",
        "三重區",
        "中和區",
        "永和區",
        "新莊區",
        "新店區",
        "土城區",
        "蘆洲區",
        "樹林區",
        "鶯歌區",
        "三峽區",
        "淡水區",
        "汐止區",
        "瑞芳區",
        "五股區",
        "泰山區",
        "林口區",
        "八里區",
        "三芝區",
        "石門區",
        "金山區",
        "萬里區",
        "平溪區",
        "雙溪區",
        "貢寮區",
        "深坑區",
        "石碇區",
        "坪林區",
        "烏來區",
    ],
    "桃園市": [
        "桃園區",
        "中壢區",
        "平鎮區",
        "八德區",
        "楊梅區",
        "蘆竹區",
        "大溪區",
        "大園區",
        "龜山區",
        "龍潭區",
        "新屋區",
        "觀音區",
        "復興區",
    ],
    "新竹市": ["東區", "北區", "香山區"],
    "新竹縣": [
        "竹北市",
        "湖口鄉",
        "新豐鄉",
        "新埔鎮",
        "關西鎮",
        "芎林鄉",
        "寶山鄉",
        "竹東鎮",
        "五峰鄉",
        "橫山鄉",
        "尖石鄉",
        "北埔鄉",
        "峨眉鄉",
    ],
    "苗栗縣": [
        "苗栗市",
        "苑裡鎮",
        "通霄鎮",
        "竹南鎮",
        "頭份市",
        "後龍鎮",
        "卓蘭鎮",
        "大湖鄉",
        "公館鄉",
        "銅鑼鄉",
        "南庄鄉",
        "頭屋鄉",
        "三義鄉",
        "西湖鄉",
        "造橋鄉",
        "三灣鄉",
        "獅潭鄉",
        "泰安鄉",
    ],
    "台中市": [
        "中區",
        "東區",
        "南區",
        "西區",
        "北區",
        "西屯區",
        "南屯區",
        "北屯區",
        "豐原區",
        "大里區",
        "太平區",
        "清水區",
        "沙鹿區",
        "大甲區",
        "東勢區",
        "梧棲區",
        "大肚區",
        "烏日區",
        "后里區",
        "神岡區",
        "潭子區",
        "大雅區",
        "新社區",
        "石岡區",
        "外埔區",
        "大安區",
        "和平區",
    ],
    "彰化縣": [
        "彰化市",
        "鹿港鎮",
        "和美鎮",
        "線西鄉",
        "伸港鄉",
        "福興鄉",
        "秀水鄉",
        "花壇鄉",
        "芬園鄉",
        "員林市",
        "溪湖鎮",
        "田中鎮",
        "大村鄉",
        "埔鹽鄉",
        "埔心鄉",
        "永靖鄉",
        "社頭鄉",
        "二水鄉",
        "北斗鎮",
        "二林鎮",
        "田尾鄉",
        "埤頭鄉",
        "芳苑鄉",
        "大城鄉",
        "竹塘鄉",
        "溪州鄉",
    ],
    "南投縣": [
        "南投市",
        "中寮鄉",
        "草屯鎮",
        "國姓鄉",
        "埔里鎮",
        "仁愛鄉",
        "名間鄉",
        "集集鎮",
        "水里鄉",
        "魚池鄉",
        "信義鄉",
        "竹山鎮",
        "鹿谷鄉",
    ],
    "雲林縣": [
        "斗南鎮",
        "大埤鄉",
        "虎尾鎮",
        "土庫鎮",
        "褒忠鄉",
        "東勢鄉",
        "台西鄉",
        "崙背鄉",
        "麥寮鄉",
        "斗六市",
        "林內鄉",
        "古坑鄉",
        "莿桐鄉",
        "西螺鎮",
        "二崙鄉",
        "北港鎮",
        "水林鄉",
        "口湖鄉",
        "四湖鄉",
        "元長鄉",
    ],
    "嘉義市": ["東區", "西區"],
    "嘉義縣": [
        "番路鄉",
        "梅山鄉",
        "竹崎鄉",
        "阿里山鄉",
        "中埔鄉",
        "大埔鄉",
        "水上鄉",
        "鹿草鄉",
        "太保市",
        "朴子市",
        "東石鄉",
        "六腳鄉",
        "新港鄉",
        "民雄鄉",
        "大林鎮",
        "溪口鄉",
        "義竹鄉",
        "布袋鎮",
    ],
    "台南市": [
        "中西區",
        "東區",
        "南區",
        "北區",
        "安平區",
        "安南區",
        "永康區",
        "歸仁區",
        "新化區",
        "左鎮區",
        "玉井區",
        "楠西區",
        "南化區",
        "仁德區",
        "關廟區",
        "龍崎區",
        "官田區",
        "麻豆區",
        "佳里區",
        "西港區",
        "七股區",
        "將軍區",
        "學甲區",
        "北門區",
        "新營區",
        "後壁區",
        "白河區",
        "東山區",
        "六甲區",
        "下營區",
        "柳營區",
        "鹽水區",
        "善化區",
        "大內區",
        "山上區",
        "新市區",
        "安定區",
    ],
    "高雄市": [
        "新興區",
        "前金區",
        "苓雅區",
        "鹽埕區",
        "鼓山區",
        "旗津區",
        "前鎮區",
        "三民區",
        "楠梓區",
        "小港區",
        "左營區",
        "仁武區",
        "大社區",
        "岡山區",
        "路竹區",
        "阿蓮區",
        "田寮區",
        "燕巢區",
        "橋頭區",
        "梓官區",
        "彌陀區",
        "永安區",
        "湖內區",
        "鳳山區",
        "大寮區",
        "林園區",
        "鳥松區",
        "大樹區",
        "旗山區",
        "美濃區",
        "六龜區",
        "內門區",
        "杉林區",
        "甲仙區",
        "桃源區",
        "那瑪夏區",
        "茂林區",
        "茄萣區",
    ],
    "屏東縣": [
        "屏東市",
        "三地門鄉",
        "霧台鄉",
        "瑪家鄉",
        "九如鄉",
        "里港鄉",
        "高樹鄉",
        "鹽埔鄉",
        "長治鄉",
        "麟洛鄉",
        "竹田鄉",
        "內埔鄉",
        "萬丹鄉",
        "潮州鎮",
        "泰武鄉",
        "來義鄉",
        "萬巒鄉",
        "崁頂鄉",
        "新埤鄉",
        "南州鄉",
        "林邊鄉",
        "東港鎮",
        "琉球鄉",
        "佳冬鄉",
        "新園鄉",
        "枋寮鄉",
        "枋山鄉",
        "春日鄉",
        "獅子鄉",
        "車城鄉",
        "牡丹鄉",
        "恆春鎮",
        "滿州鄉",
    ],
    "宜蘭縣": [
        "宜蘭市",
        "頭城鎮",
        "礁溪鄉",
        "壯圍鄉",
        "員山鄉",
        "羅東鎮",
        "三星鄉",
        "大同鄉",
        "五結鄉",
        "冬山鄉",
        "蘇澳鎮",
        "南澳鄉",
        "釣魚台",
    ],
    "花蓮縣": [
        "花蓮市",
        "新城鄉",
        "秀林鄉",
        "吉安鄉",
        "壽豐鄉",
        "鳳林鎮",
        "光復鄉",
        "豐濱鄉",
        "瑞穗鄉",
        "萬榮鄉",
        "玉里鎮",
        "卓溪鄉",
        "富里鄉",
    ],
    "台東縣": [
        "台東市",
        "綠島鄉",
        "蘭嶼鄉",
        "延平鄉",
        "卑南鄉",
        "鹿野鄉",
        "關山鎮",
        "海端鄉",
        "池上鄉",
        "東河鄉",
        "成功鎮",
        "長濱鄉",
        "太麻里鄉",
        "金峰鄉",
        "大武鄉",
        "達仁鄉",
    ],
    "澎湖縣": ["馬公市", "西嶼鄉", "望安鄉", "七美鄉", "白沙鄉", "湖西鄉"],
    "金門縣": ["金沙鎮", "金湖鎮", "金寧鄉", "金城鎮", "烈嶼鄉", "烏坵鄉"],
    "連江縣": ["南竿鄉", "北竿鄉", "莒光鄉", "東引鄉"],
}


# 初始化日誌系統
logger = setup_logging()
