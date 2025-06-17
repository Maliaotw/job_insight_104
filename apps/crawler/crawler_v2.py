"""
104人力銀行職缺爬蟲模組 (V2)

此模組是爬蟲系統的新版主入口點，使用重構後的架構來爬取104人力銀行網站的職缺資料。
重構後的架構遵循以下設計原則：
1. 單一職責原則 (SRP)：每個類只負責一項獨立功能
2. 開放/封閉原則 (OCP)：系統可以輕鬆擴展而無需修改現有程式碼
3. 依賴反轉原則 (DIP)：高層模組不依賴低層模組，兩者都依賴抽象
"""

from pathlib import Path
from typing import Dict, List, Optional, Union

from config.settings import logger
from apps.crawler.orchestrator import CrawlerOrchestrator
from apps.crawler.storage import MongoDBJobStorage, FileJobStorage


class CrawlerV2:
    """
    104人力銀行職缺爬蟲類 (V2)
    
    此類是爬蟲系統的新版主入口點，使用協調器模式來組織爬蟲流程。
    它提供了簡單的介面來啟動爬蟲，隱藏了內部的複雜性。
    """
    
    def __init__(self, output_dir: Union[str, Path] = None):
        """
        初始化爬蟲
        
        參數:
            output_dir: 保存爬取數據的目錄。默認為None，將使用專案的預設目錄。
        """
        logger.info("初始化 CrawlerV2 爬蟲實例")
        
        # 設定輸出目錄
        self.output_dir = self._setup_output_directory(output_dir) if output_dir else None
        
        # 初始化儲存器
        self.storage = MongoDBJobStorage()
        
        # 初始化協調器
        self.orchestrator = CrawlerOrchestrator(
            storage=self.storage,
            output_dir=self.output_dir
        )
    
    def _setup_output_directory(self, output_dir: Union[str, Path]) -> Path:
        """
        設定並創建輸出目錄
        
        參數:
            output_dir: 輸出目錄路徑
            
        返回:
            Path: 輸出目錄路徑對象
        """
        directory = Path(output_dir) if isinstance(output_dir, str) else output_dir
        logger.debug(f"設定輸出目錄: {directory}")
        directory.mkdir(parents=True, exist_ok=True)
        logger.debug("確保輸出目錄存在")
        return directory
    
    def run(self, keywords: List[str]) -> List[Dict]:
        """
        執行爬蟲的主入口方法
        
        參數:
            keywords: 要搜索的關鍵字列表
            
        返回:
            List[Dict]: 所有爬取到的職缺數據
        """
        logger.info(f"開始執行爬蟲，搜索關鍵字: {keywords}")
        
        try:
            # 使用協調器執行爬蟲
            crawled_jobs = self.orchestrator.run(keywords)
            
            logger.info(f"爬蟲執行完成，共獲取 {len(crawled_jobs)} 筆職缺數據")
            return crawled_jobs
        except Exception as e:
            logger.error(f"爬蟲執行過程中發生錯誤: {e}")
            raise
        finally:
            # 確保資源正確關閉
            self.close()
    
    def close(self) -> None:
        """
        關閉爬蟲，釋放資源
        """
        if hasattr(self, 'orchestrator'):
            self.orchestrator.close()
        logger.info("爬蟲已關閉")


# 使用範例
if __name__ == "__main__":
    # 創建爬蟲實例
    crawler = CrawlerV2()
    
    # 執行爬蟲
    jobs = crawler.run(keywords=['Python', 'django', 'fastapi', 'flask', 'DevOps', 'SRE', 'K8S', 'JAVA'])
    
    # 輸出結果
    print(f"共爬取到 {len(jobs)} 筆職缺資料")