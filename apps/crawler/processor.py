"""
職缺資料處理模組

此模組負責處理爬取到的職缺資料，包括資料清洗、轉換和合併等操作。
遵循單一職責原則 (SRP)，將資料處理邏輯與爬蟲邏輯分離。
"""

from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from config.settings import logger
from src.utils.text_processing import split_link_field, split_city_district
from apps.crawler.constants import JOB_STATUS_ACTIVE, JOB_STATUS_INACTIVE


class JobDataProcessor:
    """
    職缺資料處理器
    
    負責處理爬取到的原始職缺資料，包括：
    1. 合併相同職缺的多個關鍵字
    2. 處理職缺上下架狀態
    3. 處理職缺字段（link和地址）
    4. 添加元數據（爬取日期、狀態等）
    """
    
    def __init__(self):
        """初始化職缺資料處理器"""
        logger.info("初始化職缺資料處理器")
    
    def process_jobs(self, jobs: List[Dict], existing_jobs: Optional[Dict[str, str]] = None) -> List[Dict]:
        """
        處理職缺資料的主方法
        
        參數:
            jobs: 原始職缺資料列表
            existing_jobs: 現有職缺ID和狀態的映射字典
            
        返回:
            List[Dict]: 處理後的職缺資料列表
        """
        if not jobs:
            logger.warning("沒有職缺資料需要處理")
            return []
        
        logger.info(f"開始處理 {len(jobs)} 筆職缺資料")
        
        # 步驟1: 合併相同職缺的多個關鍵字
        deduplicated_jobs = self._merge_job_keywords(jobs)
        logger.info(f"合併後的唯一職缺數量: {len(deduplicated_jobs)}")
        
        # 步驟2: 處理職缺上下架狀態
        if existing_jobs:
            self._process_job_status(deduplicated_jobs, existing_jobs)
        
        # 步驟3: 處理職缺字段（link和地址）
        self._process_job_fields(deduplicated_jobs)
        
        logger.info(f"職缺資料處理完成，共處理 {len(deduplicated_jobs)} 筆職缺")
        return deduplicated_jobs
    
    def add_metadata(self, jobs: List[Dict], keyword: str = "") -> None:
        """
        為職缺資料添加元數據
        
        參數:
            jobs: 職缺資料列表
            keyword: 搜索關鍵字
        """
        if not jobs:
            return
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        for job in jobs:
            job['crawl_date'] = current_date
            job['status'] = JOB_STATUS_ACTIVE  # 預設為上架狀態
            job['last_update_date'] = current_date
            
            # 如果提供了關鍵字，添加到職缺資料中
            if keyword:
                job['search_keyword'] = keyword
            
            # 從jobAddrNo和jobAddrNoDesc提取地區信息
            job['area_code'] = job.get('jobAddrNo', '')
            job['area_name'] = job.get('jobAddrNoDesc', '')
    
    def _merge_job_keywords(self, jobs: List[Dict]) -> List[Dict]:
        """
        合併相同職缺的多個關鍵字
        
        參數:
            jobs: 原始職缺資料列表
            
        返回:
            List[Dict]: 合併後的職缺資料列表
        """
        if not jobs:
            return []
        
        logger.info("開始合併相同職缺的多個關鍵字")
        
        # 按jobNo分組，合併相同職缺的search_keyword
        job_by_id_map = {}
        for job in jobs:
            job_id = job.get('jobNo')
            if not job_id:
                continue
                
            if job_id in job_by_id_map:
                # 如果這個職缺已經存在，將關鍵字添加到列表中
                existing_job = job_by_id_map[job_id]
                existing_keywords = existing_job.get('search_keyword', [])
                new_keyword = job.get('search_keyword')
                
                # 如果existing_keywords是字符串，轉換為列表
                if isinstance(existing_keywords, str):
                    existing_keywords = [existing_keywords]
                elif not existing_keywords:
                    existing_keywords = []
                
                # 如果新關鍵字存在且不在列表中，添加它
                if new_keyword and new_keyword not in existing_keywords:
                    existing_keywords.append(new_keyword)
                
                # 更新職缺的search_keyword字段
                existing_job['search_keyword'] = existing_keywords
            else:
                # 如果這是一個新職缺，將其添加到分組中
                # 將search_keyword轉換為列表
                if 'search_keyword' in job and isinstance(job['search_keyword'], str):
                    job['search_keyword'] = [job['search_keyword']]
                elif 'search_keyword' not in job:
                    job['search_keyword'] = []
                    
                job_by_id_map[job_id] = job
        
        # 創建新的職缺列表，每個職缺只出現一次，但包含所有關鍵字
        deduplicated_jobs = list(job_by_id_map.values())
        logger.info(f"合併前職缺數量: {len(jobs)}, 合併後: {len(deduplicated_jobs)}")
        
        return deduplicated_jobs
    
    def _process_job_status(self, jobs: List[Dict], existing_jobs: Dict[str, str]) -> None:
        """
        處理職缺上下架狀態
        
        參數:
            jobs: 職缺資料列表
            existing_jobs: 現有職缺ID和狀態的映射字典
        """
        if not jobs or not existing_jobs:
            return
        
        logger.info("開始處理職缺上下架狀態")
        today = datetime.now().strftime("%Y-%m-%d")
        reactivated_count = 0
        
        for job in jobs:
            job_id = job.get('jobNo')
            if not job_id:
                continue
                
            if job_id in existing_jobs:
                # 更新最後更新日期
                job['last_update_date'] = today
                
                # 如果職缺之前是下架狀態，現在又出現了，將其重新激活
                if existing_jobs[job_id] == JOB_STATUS_INACTIVE:
                    job['status'] = JOB_STATUS_ACTIVE
                    # 清除下架日期
                    job.pop('delisted_date', None)
                    reactivated_count += 1
            else:
                # 新職缺，設置最後更新日期
                job['last_update_date'] = today
        
        if reactivated_count > 0:
            logger.info(f"重新激活了 {reactivated_count} 筆之前下架的職缺")
    
    def _process_job_fields(self, jobs: List[Dict]) -> None:
        """
        處理職缺字段，包括link和地址
        
        參數:
            jobs: 職缺資料列表
        """
        if not jobs:
            return
        
        # 處理link字段，分割為applyAnalyze、job和cust
        logger.info("處理link字段，分割為applyAnalyze、job和cust")
        successfully_processed_links = 0
        
        for job in jobs:
            if 'link' in job and job['link']:
                try:
                    # 檢查link是否已經是字典格式
                    if isinstance(job['link'], dict):
                        # 如果已經是字典，直接提取值
                        apply_analyze_url = job['link'].get('applyAnalyze', '')
                        job_detail_url = job['link'].get('job', '')
                        company_url = job['link'].get('cust', '')
                        
                        # 添加協議前綴
                        if apply_analyze_url and not apply_analyze_url.startswith(("http:", "https:")):
                            apply_analyze_url = "https:" + apply_analyze_url
                        if job_detail_url and not job_detail_url.startswith(("http:", "https:")):
                            job_detail_url = "https:" + job_detail_url
                        if company_url and not company_url.startswith(("http:", "https:")):
                            company_url = "https:" + company_url
                    else:
                        # 如果是字符串，使用分割函數
                        apply_analyze_url, job_detail_url, company_url = split_link_field(job['link'])
                    
                    # 更新職缺字段
                    job['applyAnalyze'] = apply_analyze_url
                    job['job'] = job_detail_url
                    job['cust'] = company_url
                    successfully_processed_links += 1
                except Exception as e:
                    logger.error(f"處理職缺 {job.get('jobNo', 'unknown')} 的link字段時出錯: {e}")
        
        logger.info(f"成功處理 {successfully_processed_links} 筆職缺的link字段")
        
        # 處理jobAddrNoDesc字段，分割為city和district
        logger.info("處理jobAddrNoDesc字段，分割為city和district")
        successfully_processed_addresses = 0
        
        for job in jobs:
            if 'jobAddrNoDesc' in job and job['jobAddrNoDesc']:
                try:
                    city, district = split_city_district(job['jobAddrNoDesc'])
                    job['city'] = city
                    job['district'] = district
                    successfully_processed_addresses += 1
                except Exception as e:
                    logger.error(f"處理職缺 {job.get('jobNo', 'unknown')} 的jobAddrNoDesc字段時出錯: {e}")
        
        logger.info(f"成功處理 {successfully_processed_addresses} 筆職缺的jobAddrNoDesc字段")