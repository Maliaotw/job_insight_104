"""
MongoDB 到 DuckDB 數據轉移腳本

此腳本從 MongoDB 讀取職缺數據，並將其轉移到 DuckDB 數據庫中。
可以按需執行，或設置為定時任務。
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# 添加項目根目錄到 Python 路徑
sys.path.append(str(Path(__file__).parent.parent))

from config.settings import logger
from src.database.mongodb_manager import MongoDBManager
from src.database.duckdb_manager import DuckDBManager


def transfer_data(days_ago: int = 0, limit: int = 0):
    """
    將 MongoDB 中的數據轉移到 DuckDB

    參數:
        days_ago: 只轉移最近幾天的數據，0 表示所有數據
        limit: 最大轉移記錄數，0 表示不限制
    """
    logger.info(f"=== 開始從 MongoDB 轉移數據到 DuckDB ===")

    # 初始化數據庫管理器
    mongo_manager = MongoDBManager()
    duckdb_manager = DuckDBManager()

    try:
        # 構建過濾條件
        filters = {}
        if days_ago > 0:
            date_threshold = (datetime.now() - timedelta(days=days_ago)).strftime(
                "%Y-%m-%d"
            )
            filters["crawl_date"] = {"$gte": date_threshold}

        # 從 MongoDB 獲取數據
        logger.info(f"從 MongoDB 獲取數據，過濾條件: {filters}")
        df = mongo_manager.get_jobs_dataframe(
            filters=filters, limit=limit if limit > 0 else 0
        )

        if df.empty:
            logger.warning("沒有找到符合條件的數據")
            return 0

        logger.info(f"獲取到 {len(df)} 筆數據，準備轉移到 DuckDB")

        # 使用無結構模式直接將數據保存到 DuckDB，不進行過濾
        try:
            # 將數據保存到 DuckDB 的 jobs_json 表中

            num_inserted = duckdb_manager.insert_jobs(df.to_dict(orient="list"))
        except Exception as e:
            logger.error(f"插入數據時發生錯誤: {e}")
            return 0

        logger.info(f"成功將 {num_inserted} 筆數據從 MongoDB 轉移到 DuckDB")
        return num_inserted

    finally:
        # 關閉數據庫連接
        mongo_manager.close()
        duckdb_manager.close()
        logger.info("=== 數據轉移完成 ===")


def main():
    """主函數"""
    import argparse

    parser = argparse.ArgumentParser(description="從 MongoDB 轉移數據到 DuckDB")
    parser.add_argument(
        "--days", type=int, default=0, help="只轉移最近幾天的數據，0 表示所有數據"
    )
    parser.add_argument(
        "--limit", type=int, default=0, help="最大轉移記錄數，0 表示不限制"
    )

    args = parser.parse_args()

    transfer_data(days_ago=args.days, limit=args.limit)


if __name__ == "__main__":
    main()
