from apscheduler.schedulers.blocking import BlockingScheduler
import logging
from datetime import datetime

from apps.crawler.crawler_v2 import CrawlerV2
from config.settings import CRAWLER_SCHEDULE_DAILY_CRAWL_HOUR,CRAWLER_SCHEDULE_DAILY_CRAWL_MINUTE,CRAWLER_KEYWORDS
from src.transfer.transfer_mongodb_to_duckdb import transfer_data
from src.database.duckdb_manager import DuckDBManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)

logger = logging.getLogger(__name__)

scheduler = BlockingScheduler()

@scheduler.scheduled_job('cron', hour=CRAWLER_SCHEDULE_DAILY_CRAWL_HOUR, minute=CRAWLER_SCHEDULE_DAILY_CRAWL_MINUTE)
def scheduled_task():
    run_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"排程任务启动时间：{run_time}，准备创建爬虫实例")
    crawler = CrawlerV2()

    logger.info(f"执行爬虫，关键词：{CRAWLER_KEYWORDS}")
    jobs = crawler.run(keywords=CRAWLER_KEYWORDS)

    logger.info(f"共爬取到 {len(jobs)} 条职位数据")

    logger.info("开始迁移数据到 duckdb")
    transfer_data(days_ago=0, limit=0)
    logger.info("数据迁移完成")

    duckdb_manager = DuckDBManager()
    duckdb_manager._install_extensions()
    duckdb_manager.export_to_s3_parquet(
        table_name='news_jobs',
        s3_path='jobs.parquet'
    )
    duckdb_manager.close()

if __name__ == "__main__":
    logger.info("调度器启动")
    scheduler.start()