"""
DuckDB 管理器模塊

此模塊負責管理用於存儲和查詢職缺數據的DuckDB數據庫。
它提供了一個高級接口用於與數據庫交互，包括S3上傳和下載功能。
"""

import os
from datetime import datetime
from json import JSONEncoder
from pathlib import Path
from typing import Dict, List, Union

import boto3
import duckdb
import pandas as pd

from config.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_S3_BUCKET,
    AWS_SECRET_ACCESS_KEY,
    BASE_DIR,
    CURRENT_ENV,
    DATABASE_PROCESSED_DATA_PATH,
    logger,
)

logger.info(os.environ)


class CustomJSONEncoder(JSONEncoder):
    """
    自定義JSON編碼器，用於處理pandas Timestamp等非標準JSON類型。

    實作細節：
    - 擴展標準JSONEncoder類
    - 添加對pandas Timestamp對象的支持，將其轉換為ISO格式字符串
    - 添加對其他常見非JSON可序列化類型的支持
    """

    def default(self, obj):
        # 處理pandas Timestamp對象
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        # 處理numpy數據類型
        if hasattr(obj, "item"):
            return obj.item()
        # 處理其他可能的非JSON可序列化類型
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)  # 最後嘗試將對象轉換為字符串


class DuckDBManager:
    """
    DuckDB數據庫管理器。

    此類提供創建表格、插入數據、查詢數據庫以及S3上傳下載的方法。

    實作細節：
    - 使用DuckDB作為數據存儲引擎，因為它快速、輕量且支持SQL
    - 提供完整的職缺數據管理功能，包括存儲、檢索和分析
    - 支持從多種來源（CSV、JSON、DataFrame）導入數據
    - 實現數據歷史追蹤和統計分析功能
    - 支持S3雲端存儲的數據上傳和下載
    """

    bucket = AWS_S3_BUCKET

    def __init__(self, db_path: Union[str, Path] = None, db_type: str = "processed"):
        """
        初始化DuckDB管理器。

        實作細節：
        1. 設置數據庫文件路徑
        2. 確保數據庫目錄存在
        3. 建立數據庫連接
        4. 初始化必要的表結構
        5. 安裝AWS和HTTPFS擴展

        Args:
            db_path: DuckDB數據庫文件的路徑。默認為None，會使用預設路徑。
            db_type: 數據庫類型，可以是 "raw" 或 "processed"。默認為 "processed"。
                     "raw" 用於存儲原始爬蟲數據，"processed" 用於存儲處理後的分析數據。
        """
        logger.info("初始化DuckDB管理器")

        # 如果未提供路徑，根據db_type使用相應的默認路徑
        if db_path is None:
            db_path = BASE_DIR / DATABASE_PROCESSED_DATA_PATH
            logger.info(f"使用處理後數據庫路徑: {db_path}")
        else:
            db_path = Path(db_path)
            logger.info(f"使用自定義數據庫路徑: {db_path}")

        # 確保目錄存在，避免因目錄不存在而導致的錯誤
        db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"確保數據庫目錄存在: {db_path.parent}")

        self.db_path = str(db_path)
        self.db_type = db_type
        self.conn = duckdb.connect(self.db_path)
        logger.info(f"已連接到DuckDB數據庫: {self.db_path}")

        logger.info(f"CURRENT_ENV {CURRENT_ENV}")
        if CURRENT_ENV != "dev":
            self.check_aws_env()

    def check_aws_env(self):
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            logger.info(f"AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
            # 安裝必要的擴展
            self._install_extensions()
            self.read_from_s3_parquet(f"s3://{self.bucket}/jobs.parquet", "news_jobs")
        else:
            logger.info(f"AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY is None")

    def _install_extensions(self):
        """
        安裝DuckDB擴展以支持S3操作。
        """
        try:
            # 安裝httpfs擴展（用於S3操作）
            logger.info("安裝httpfs擴展...")
            self.conn.execute("INSTALL httpfs;")
            self.conn.execute("LOAD httpfs;")

            # 安裝aws擴展（用於AWS認證）
            # logger.info("安裝aws擴展...")
            # self.conn.execute("INSTALL aws;")
            # self.conn.execute("LOAD aws;")

            logger.info("擴展安裝完成")
        except Exception as e:
            logger.warning(f"擴展安裝失敗: {e}")

    def test_upload_s3(self):
        try:
            # 請將 your-bucket 改為你的 S3 bucket 名
            self.conn.execute(
                f"""
                        COPY (SELECT 100 AS test_col) 
                        TO 's3://{self.bucket}/duckdb_s3_test.csv' (FORMAT CSV)
                    """
            )
            logger.info("S3配置測試成功，可以正常寫入S3。")
        except Exception as e:
            logger.error(f"S3配置測試失敗: {e}")

    def configure_aws_credentials(
        self,
        access_key_id: str = None,
        secret_access_key: str = None,
        region: str = "us-east-1",
        session_token: str = None,
    ):
        """
        配置AWS認證信息。

        Args:
            access_key_id: AWS訪問密鑰ID，如果為None則使用環境變數
            secret_access_key: AWS秘密訪問密鑰，如果為None則使用環境變數
            region: AWS區域，默認為us-east-1
            session_token: AWS會話令牌（可選）
        """
        try:
            # 如果沒有提供認證信息，嘗試從環境變數獲取
            if not access_key_id or not secret_access_key:
                access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
                secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
                session_token = os.getenv("AWS_SESSION_TOKEN")
                region = os.getenv("AWS_DEFAULT_REGION")

            # 設置AWS認證信息
            logger.info("配置AWS認證信息...")
            self.conn.execute(f"SET s3_region='{region}';")
            self.conn.execute(f"SET s3_access_key_id='{access_key_id}';")
            self.conn.execute(f"SET s3_secret_access_key='{secret_access_key}';")
            logger.info(
                f"s3 params: region={region}, access_key_id={access_key_id}, secret_access_key=***"
            )

            if session_token:
                self.conn.execute("SET s3_session_token = ?", [session_token])

            logger.info("AWS認證配置完成")

        except Exception as e:
            logger.error(f"AWS認證配置失敗: {e}")
            raise

    def export_to_s3_parquet(
        self, table_name: str, s3_path: str, filters: Dict = None, limit: int = None
    ) -> bool:
        """
        將數據表導出為Parquet格式並上傳到S3。

        Args:
            table_name: 要導出的表名
            s3_path: S3路徑，格式為 's3://bucket/path/file.parquet'
            filters: 過濾條件（可選）
            limit: 限制導出的行數（可選）

        Returns:
            bool: 導出是否成功
        """
        try:
            s3_path = (
                f"s3://{self.bucket}/{s3_path}"
                if not s3_path.startswith("s3://")
                else s3_path
            )
            logger.info(f"開始將表 {table_name} 導出到 S3: {s3_path}")

            # 構建查詢語句
            query = f"SELECT * FROM {table_name}"
            params = []

            if filters:
                conditions = []
                for column, value in filters.items():
                    conditions.append(f"{column} = ?")
                    params.append(value)
                query += " WHERE " + " AND ".join(conditions)

            if limit:
                query += f" LIMIT {limit}"

            # 構建COPY TO語句
            copy_query = f"COPY ({query}) TO '{s3_path}' (FORMAT PARQUET)"

            # 執行導出
            logger.info(f"copy_query {copy_query}")
            if params:
                self.conn.execute(copy_query, params)
            else:
                self.conn.execute(copy_query)

            logger.info(f"成功將數據導出到 S3: {s3_path}")
            return True

        except Exception as e:
            logger.error(f"導出到S3失敗: {e}", exc_info=True)
            return False

    def export_to_s3_csv(
        self, table_name: str, s3_path: str, filters: Dict = None, limit: int = None
    ) -> bool:
        """
        將數據表導出為CSV格式並上傳到S3。

        Args:
            table_name: 要導出的表名
            s3_path: S3路徑，格式為 's3://bucket/path/file.csv'
            filters: 過濾條件（可選）
            limit: 限制導出的行數（可選）

        Returns:
            bool: 導出是否成功
        """
        try:
            logger.info(f"開始將表 {table_name} 導出為CSV到 S3: {s3_path}")

            # 構建查詢語句
            query = f"SELECT * FROM {table_name}"
            params = []

            if filters:
                conditions = []
                for column, value in filters.items():
                    conditions.append(f"{column} = ?")
                    params.append(value)
                query += " WHERE " + " AND ".join(conditions)

            if limit:
                query += f" LIMIT {limit}"

            # 構建COPY TO語句
            copy_query = f"COPY ({query}) TO '{s3_path}' (FORMAT CSV, HEADER)"

            # 執行導出
            if params:
                self.conn.execute(copy_query, params)
            else:
                self.conn.execute(copy_query)

            logger.info(f"成功將數據導出為CSV到 S3: {s3_path}")
            return True

        except Exception as e:
            logger.error(f"導出CSV到S3失敗: {e}")
            return False

    def read_from_s3_parquet(
        self, s3_path: str, table_name: str = None
    ) -> pd.DataFrame:
        """
        從S3讀取Parquet文件並返回DataFrame。

        Args:
            s3_path: S3路徑，格式為 's3://bucket/path/file.parquet'
            table_name: 如果提供，將數據載入到指定的表中

        Returns:
            pd.DataFrame: 讀取的數據
        """
        try:
            s3_path = (
                f"s3://{self.bucket}/{s3_path}"
                if not s3_path.startswith("s3://")
                else s3_path
            )
            logger.info(f"開始從 S3 讀取 Parquet 文件: {s3_path}")

            # 直接查詢S3上的Parquet文件
            query = f"SELECT * FROM '{s3_path}'"
            df = self.conn.execute(query).fetchdf()

            logger.info(f"成功從 S3 讀取 {len(df)} 行數據")

            # 如果指定了表名，將數據載入到表中
            if table_name:
                self.conn.register("temp_s3_data", df)
                self.conn.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_s3_data"
                )
                logger.info(f"數據已載入到表 {table_name}")

            return df

        except Exception as e:
            logger.error(f"從S3讀取Parquet文件失敗: {e}")
            return pd.DataFrame()

    def read_from_s3_csv(self, s3_path: str, table_name: str = None) -> pd.DataFrame:
        """
        從S3讀取CSV文件並返回DataFrame。

        Args:
            s3_path: S3路徑，格式為 's3://bucket/path/file.csv'
            table_name: 如果提供，將數據載入到指定的表中

        Returns:
            pd.DataFrame: 讀取的數據
        """
        try:
            s3_path = (
                f"s3://{self.bucket}/{s3_path}"
                if not s3_path.startswith("s3://")
                else s3_path
            )
            logger.info(f"開始從 S3 讀取 CSV 文件: {s3_path}")

            # 直接查詢S3上的CSV文件
            query = f"SELECT * FROM read_csv_auto('{s3_path}')"
            df = self.conn.execute(query).fetchdf()

            logger.info(f"成功從 S3 讀取 {len(df)} 行數據")

            # 如果指定了表名，將數據載入到表中
            if table_name:
                self.conn.register("temp_s3_data", df)
                self.conn.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_s3_data"
                )
                logger.info(f"數據已載入到表 {table_name}")

            return df

        except Exception as e:
            logger.error(f"從S3讀取CSV文件失敗: {e}")
            return pd.DataFrame()

    def backup_database_to_s3(
        self, s3_bucket: str, s3_prefix: str = "backups/"
    ) -> bool:
        """
        將整個數據庫備份到S3。

        Args:
            s3_bucket: S3存儲桶名稱
            s3_prefix: S3前綴路徑

        Returns:
            bool: 備份是否成功
        """
        try:
            logger.info(f"開始備份數據庫到 S3 存儲桶: {s3_bucket}")

            # 創建boto3客戶端
            s3_client = boto3.client("s3")

            # 生成備份文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"database_backup_{timestamp}.db"
            s3_key = f"{s3_prefix}{backup_filename}"

            # 上傳數據庫文件到S3
            s3_client.upload_file(self.db_path, s3_bucket, s3_key)

            logger.info(f"數據庫備份成功: s3://{s3_bucket}/{s3_key}")
            return True

        except Exception as e:
            logger.error(f"數據庫備份到S3失敗: {e}")
            return False

    def restore_database_from_s3(
        self, s3_bucket: str, s3_key: str, local_path: str = None
    ) -> bool:
        """
        從S3恢復數據庫備份。

        Args:
            s3_bucket: S3存儲桶名稱
            s3_key: S3對象鍵
            local_path: 本地恢復路徑，如果為None則使用當前數據庫路徑

        Returns:
            bool: 恢復是否成功
        """
        try:
            logger.info(f"開始從 S3 恢復數據庫: s3://{s3_bucket}/{s3_key}")

            # 創建boto3客戶端
            s3_client = boto3.client("s3")

            # 確定本地路徑
            if local_path is None:
                local_path = self.db_path

            # 關閉當前連接
            if self.conn:
                self.conn.close()

            # 從S3下載文件
            s3_client.download_file(s3_bucket, s3_key, local_path)

            # 重新連接到恢復的數據庫
            self.conn = duckdb.connect(local_path)
            self._install_extensions()

            logger.info(f"數據庫恢復成功: {local_path}")
            return True

        except Exception as e:
            logger.error(f"從S3恢復數據庫失敗: {e}")
            return False

    def list_s3_files(self, s3_bucket: str, s3_prefix: str = "") -> List[str]:
        """
        列出S3存儲桶中的文件。

        Args:
            s3_bucket: S3存儲桶名稱
            s3_prefix: S3前綴過濾器

        Returns:
            List[str]: 文件列表
        """
        try:
            logger.info(f"列出 S3 存儲桶 {s3_bucket} 中的文件，前綴: {s3_prefix}")

            # 創建boto3客戶端
            s3_client = boto3.client("s3")

            # 列出對象
            response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

            files = []
            if "Contents" in response:
                files = [obj["Key"] for obj in response["Contents"]]

            logger.info(f"找到 {len(files)} 個文件")
            return files

        except Exception as e:
            logger.error(f"列出S3文件失敗: {e}")
            return []

    def get_jobs(
        self, filters: Dict = None, limit: int = 1000, include_inactive: bool = False
    ) -> pd.DataFrame:
        """
        從數據庫獲取職缺，可選擇性地使用過濾條件。

        參數:
            filters: 用於過濾的列-值對字典。默認為None。
            limit: 返回的最大職缺數量。默認為1000。
            include_inactive: 是否包含已下架的職缺。默認為False。

        返回:
            pd.DataFrame: 包含職缺的DataFrame。
        """
        # 始終使用結構化模式
        # query = "SELECT * FROM jobs"
        query = "SELECT * FROM news_jobs"
        params = []

        conditions = []
        if not include_inactive:
            conditions.append("status = 'active'")

        if filters:
            for column, value in filters.items():
                conditions.append(f"{column} = ?")
                params.append(value)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f" LIMIT {limit}"

        result = self.conn.execute(query, params).fetchdf()
        return result

    def insert_jobs(self, jobs: List[Dict]) -> int:
        """
        將職缺數據保存到news_jobs表中。

        實作細節：
        1. 將職缺數據轉換為DataFrame
        2. 確保news_jobs表存在並具有所有必要的列
        3. 註冊臨時表
        4. 使用INSERT ON CONFLICT DO UPDATE插入或更新數據

        參數:
            jobs: 職缺數據列表

        返回:
            int: 插入到資料庫的職缺數量
        """
        # 檢查是否有數據需要保存
        if not jobs:
            logger.warning("沒有職缺數據可保存到資料庫")
            return 0

        try:
            # 1. 將資料轉換為DataFrame
            df = pd.DataFrame(jobs)

            # 3. 註冊臨時表
            logger.info("註冊臨時表 temp_jobs")
            self.conn.register("temp_jobs", df)

            # 準備列名稱字串,用於SQL語句
            df_columns = df.columns.tolist()
            columns_str = ", ".join(df_columns)

            # 4. 將資料插入或更新至news_jobs表
            logger.info("開始將資料寫入 news_jobs 表")

            # 使用標準的 INSERT 語句，如果有衝突則更新
            update_columns = []
            for col in df_columns:
                if col != "jobNo":  # 不更新主鍵
                    update_columns.append(f"{col} = excluded.{col}")

            update_str = ", ".join(update_columns)

            self.conn.execute(
                "CREATE OR REPLACE TABLE news_jobs AS SELECT * FROM temp_jobs"
            )
            #
            # self.conn.execute(f"""
            #     INSERT INTO news_jobs ({columns_str})
            #     SELECT {columns_str} FROM temp_jobs
            #     ON CONFLICT (jobNo) DO UPDATE SET
            #     {update_str}
            # """)
            logger.info("使用 INSERT ON CONFLICT DO UPDATE 完成資料寫入")

            num_inserted = len(df)

            # 查詢資料庫中的總記錄數
            total_records = self.conn.execute(
                "SELECT COUNT(*) FROM news_jobs"
            ).fetchone()[0]
            logger.info(f"已將 {num_inserted} 筆職缺數據保存到資料庫")
            logger.info(f"資料庫目前共有 {total_records} 筆記錄")

            return num_inserted

        except Exception as e:
            logger.error(f"插入職缺數據時發生錯誤: {e}")
            return 0

    def export_recent_jobs(self):
        """
        將 news_jobs 數據表中最新的職缺發布信息導出為 JSON 文件。

        此函數從表中檢索最多 50 條最新記錄，按相關的日期/時間列排序，
        如果找不到日期列則按 jobNo 列排序。
        數據以 JSON 格式導出，輸出文件保存在基礎目錄下的指定輸出目錄中。

        :raises sqlite3.Error: 查詢數據庫或訪問表時出現問題。
        :raises FileNotFoundError: 創建輸出目錄或保存文件時出現問題。

        :return: None
        """
        logger.info(f"開始導出最近職缺數據，時間: {datetime.now()}")

        # 數據庫路徑
        db_path = DATABASE_PROCESSED_DATA_PATH
        logger.info(f"數據庫路徑: {db_path}")

        # 檢查news_jobs表是否存在
        table_exists = self.conn.execute(
            """
                                         SELECT name
                                         FROM sqlite_master
                                         WHERE type = 'table'
                                           AND name = 'news_jobs'
                                         """
        ).fetchone()

        if not table_exists:
            logger.error("數據庫中不存在news_jobs表。")
            self.conn.close()
            return

        # 獲取表結構
        logger.info("獲取news_jobs表結構...")
        columns = self.conn.execute(
            """
                PRAGMA table_info(news_jobs)
            """
        ).fetchdf()

        logger.debug("表結構:")
        logger.debug(f"\n{columns}")

        # 查找可能的日期/時間欄位
        date_columns = []
        for _, row in columns.iterrows():
            col_name = row["name"]
            # 檢查欄位名稱是否包含日期相關關鍵字
            if any(
                keyword in col_name.lower()
                for keyword in ["date", "time", "created", "updated", "timestamp"]
            ):
                date_columns.append(col_name)

        logger.info(f"可能的日期/時間欄位: {date_columns}")

        # 如果找不到日期欄位，則使用jobNo作為排序依據（假設較大的jobNo表示較新的記錄）
        sort_column = date_columns[0] if date_columns else "jobNo"
        sort_order = "DESC" if date_columns else "DESC"

        logger.info(f"使用 {sort_column} {sort_order} 排序")

        # 查詢最近50筆記錄
        logger.info("查詢最近50筆記錄...")
        recent_jobs = self.conn.execute(
            f"""
            SELECT * FROM news_jobs
            ORDER BY {sort_column} {sort_order}
            LIMIT 50
        """
        ).fetchdf()

        logger.info(f"獲取到 {len(recent_jobs)} 筆記錄")

        # 將DataFrame直接轉換為JSON字符串
        jobs_list = recent_jobs.to_json(orient="records", force_ascii=False, indent=2)

        # 創建輸出目錄（如果不存在）
        output_dir = BASE_DIR / "data" / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        # 生成輸出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"recent_jobs_{timestamp}.json"

        # 將結果保存為JSON文件
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(jobs_list)

        logger.info(f"已將最近50筆職缺數據保存至: {output_file}")

        # 關閉數據庫連接
        self.conn.close()
        logger.info("數據庫連接已關閉")
        logger.info(f"完成時間: {datetime.now()}")

    def close(self):
        """
        關閉數據庫連接。
        """
        if self.conn:
            self.conn.close()
            logger.info("數據庫連接已關閉")


# 使用示例
if __name__ == "__main__":
    # 創建DuckDB管理器實例
    duckdb_manager = DuckDBManager()

    # 配置AWS認證（可選，會自動嘗試從環境變數讀取）
    # duckdb_manager.configure_aws_credentials()

    # duckdb_manager.test_upload_s3()

    #
    # # 示例：導出數據到S3
    duckdb_manager._install_extensions()
    success = duckdb_manager.export_to_s3_parquet(
        table_name="news_jobs", s3_path="jobs.parquet"
    )
    #
    # 示例：從S3讀取數據
    # df = duckdb_manager.read_from_s3_parquet('s3://<bucket>/jobs.parquet')
    # logger.info(df)

    # 示例：備份數據庫到S3
    # backup_success = duckdb_manager.backup_database_to_s3('your-backup-bucket')

    # 原有功能
    # duckdb_manager.export_recent_jobs()
    duckdb_manager.close()
