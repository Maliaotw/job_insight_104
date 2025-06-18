"""
HTTP 請求適配器模組

此模組提供了一個通用的 HTTP 請求適配器，基於 httpx 庫實現。
它支持同步和非同步請求，並提供了重試、錯誤處理和請求配置等功能。

實作細節：
1. 支持同步和非同步 HTTP 請求
2. 內建重試機制，可處理常見的網路錯誤
3. 支持 429 Too Many Requests 錯誤處理
4. 可配置的請求頭、超時和其他參數
5. 與專案日誌系統集成
"""

import asyncio
import logging
import random
import time
from typing import Dict, List, Union, TypeVar

import httpx

# 導入專案日誌
from config.settings import logger

# 定義響應類型
T = TypeVar("T")


class HttpAdapter:
    """
    通用 HTTP 請求適配器類

    此類提供了一個統一的接口來發送 HTTP 請求，支持同步和非同步操作。
    它處理常見的錯誤情況，提供重試機制，並允許自定義請求配置。

    實作細節：
    - 使用 httpx 庫作為底層 HTTP 客戶端
    - 支持 GET、POST、PUT、DELETE 等常見 HTTP 方法
    - 提供重試機制，可自定義重試次數和延遲
    - 處理 429 Too Many Requests 錯誤，遵循 Retry-After 頭部
    - 可配置請求頭、超時、代理等參數
    """

    # 默認 User-Agent 列表
    DEFAULT_USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    ]

    def __init__(
        self,
        base_url: str = "",
        headers: Dict[str, str] = None,
        timeout: Union[float, httpx.Timeout] = 30.0,
        max_retries: int = 3,
        min_retry_delay: float = 1.0,
        max_retry_delay: float = 5.0,
        user_agents: List[str] = None,
        rotate_user_agent: bool = True,
        verify_ssl: bool = True,
        http2: bool = False,
        follow_redirects: bool = True,
        proxy: str = None,
        cookies: Dict[str, str] = None,
        logger_instance: logging.Logger = None,
    ):
        """
        初始化 HTTP 適配器

        參數:
            base_url: 基礎 URL，所有請求都會以此為前綴
            headers: 默認請求頭
            timeout: 請求超時時間（秒）
            max_retries: 最大重試次數
            min_retry_delay: 重試最小延遲時間（秒）
            max_retry_delay: 重試最大延遲時間（秒）
            user_agents: 自定義 User-Agent 列表
            rotate_user_agent: 是否輪換 User-Agent
            verify_ssl: 是否驗證 SSL 證書
            http2: 是否啟用 HTTP/2
            follow_redirects: 是否自動跟隨重定向
            proxy: 代理服務器 URL
            cookies: 默認 cookies
            logger_instance: 自定義日誌實例
        """
        # 設置日誌實例 (必須先初始化，因為其他方法會使用)
        self.logger = logger_instance or logger

        # 設置基本屬性
        self.base_url = base_url
        self.timeout = (
            timeout if isinstance(timeout, httpx.Timeout) else httpx.Timeout(timeout)
        )
        self.max_retries = max_retries
        self.min_retry_delay = min_retry_delay
        self.max_retry_delay = max_retry_delay
        self.verify_ssl = verify_ssl
        self.http2 = http2
        self.follow_redirects = follow_redirects
        self.proxy = proxy
        self.cookies = cookies or {}

        # 設置 User-Agent 相關屬性
        self.user_agents = user_agents or self.DEFAULT_USER_AGENTS
        self.rotate_user_agent = rotate_user_agent

        # 設置默認請求頭
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        if headers:
            self.headers.update(headers)

        # 如果啟用 User-Agent 輪換，設置初始 User-Agent
        if self.rotate_user_agent:
            self.update_user_agent()

        # 請求計數器和重試計數器
        self.request_count = 0
        self.retry_count = 0

        # 同步客戶端實例（延遲初始化）
        self._sync_client = None

    def update_user_agent(self) -> None:
        """
        隨機更新 User-Agent
        """
        if not self.user_agents:
            return

        user_agent = random.choice(self.user_agents)
        self.headers["User-Agent"] = user_agent
        self.logger.debug(f"更新 User-Agent: {user_agent[:30]}...")

    @property
    def sync_client(self) -> httpx.Client:
        """
        獲取同步 HTTP 客戶端實例

        返回:
            httpx.Client: 同步 HTTP 客戶端
        """
        if self._sync_client is None or self._sync_client.is_closed:
            self._sync_client = httpx.Client(
                base_url=self.base_url,
                headers=self.headers,
                timeout=self.timeout,
                verify=self.verify_ssl,
                http2=self.http2,
                follow_redirects=self.follow_redirects,
                proxy=self.proxy,
                cookies=self.cookies,
            )
        return self._sync_client

    def close(self) -> None:
        """
        關閉同步 HTTP 客戶端
        """
        if self._sync_client and not self._sync_client.is_closed:
            self._sync_client.close()

    def __enter__(self) -> "HttpAdapter":
        """
        支持 with 語句的上下文管理
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        退出上下文時關閉客戶端
        """
        self.close()

    def _build_url(self, url: str) -> str:
        """
        構建完整的 URL

        參數:
            url: 相對或絕對 URL

        返回:
            str: 完整的 URL
        """
        # 如果 URL 已經是絕對 URL，直接返回
        if url.startswith(("http://", "https://")):
            return url

        # 否則，將其與 base_url 組合
        if self.base_url:
            if self.base_url.endswith("/") and url.startswith("/"):
                return f"{self.base_url}{url[1:]}"
            elif not self.base_url.endswith("/") and not url.startswith("/"):
                return f"{self.base_url}/{url}"
            else:
                return f"{self.base_url}{url}"
        else:
            return url

    def _handle_response(
        self, response: httpx.Response, error_context: str = "請求"
    ) -> Dict:
        """
        處理 HTTP 響應

        參數:
            response: HTTP 響應對象
            error_context: 錯誤日誌的上下文描述

        返回:
            Dict: 響應數據或錯誤信息
        """
        try:
            # 檢查是否為 JSON 響應
            content_type = response.headers.get("content-type", "")
            if "application/json" in content_type:
                return response.json()
            else:
                # 非 JSON 響應，返回文本內容和狀態碼
                return {
                    "status_code": response.status_code,
                    "content": response.text,
                    "headers": dict(response.headers),
                }
        except Exception as e:
            self.logger.error(f"處理{error_context}響應時出錯: {e}")
            return {
                "error": f"處理響應錯誤: {str(e)}",
                "status_code": response.status_code,
                "content": response.text[:1000],  # 只返回前 1000 個字符，避免過大
            }

    def request(
        self,
        method: str,
        url: str,
        params: Dict = None,
        data: Dict = None,
        json: Dict = None,
        headers: Dict = None,
        cookies: Dict = None,
        timeout: Union[float, httpx.Timeout] = None,
        error_context: str = "請求",
    ) -> Dict:
        """
        發送同步 HTTP 請求

        參數:
            method: HTTP 方法 (GET, POST, PUT, DELETE 等)
            url: 請求 URL
            params: URL 參數
            data: 表單數據
            json: JSON 數據
            headers: 請求頭
            cookies: 請求 cookies
            timeout: 請求超時
            error_context: 錯誤日誌的上下文描述

        返回:
            Dict: 響應數據或錯誤信息
        """
        # 更新請求計數器
        self.request_count += 1

        # 每 5 次請求更新一次 User-Agent
        if self.rotate_user_agent and self.request_count % 5 == 0:
            self.update_user_agent()

        # 構建完整 URL
        full_url = self._build_url(url)

        # 合併請求頭
        merged_headers = self.headers.copy()
        if headers:
            merged_headers.update(headers)

        # 設置超時
        request_timeout = timeout or self.timeout

        retries = 0
        while retries < self.max_retries:
            try:
                # 添加重試延遲
                if retries > 0:
                    delay = random.uniform(
                        self.min_retry_delay * retries, self.max_retry_delay * retries
                    )
                    self.logger.info(f"重試前等待 {delay:.2f} 秒...")
                    time.sleep(delay)

                self.logger.debug(
                    f"發送 {method} 請求至 {full_url} (嘗試 {retries + 1}/{self.max_retries})"
                )

                # 發送請求
                response = self.sync_client.request(
                    method=method,
                    url=full_url,
                    params=params,
                    data=data,
                    json=json,
                    headers=merged_headers,
                    cookies=cookies,
                    timeout=request_timeout,
                )

                # 檢查是否被封鎖 (403 Forbidden)
                if response.status_code == 403:
                    self.logger.warning(f"請求被拒絕 (403 Forbidden)，可能被網站封鎖")
                    if self.rotate_user_agent:
                        self.update_user_agent()
                    retries += 1
                    self.retry_count += 1
                    continue

                # 檢查是否達到請求限制 (429 Too Many Requests)
                if response.status_code == 429:
                    # 從響應頭獲取需要等待的時間
                    wait_time = int(response.headers.get("Retry-After", 60))
                    self.logger.warning(
                        f"請求頻率過高 (429 Too Many Requests)，需要等待 {wait_time} 秒"
                    )

                    # 等待指定時間後重試
                    time.sleep(wait_time)
                    retries += 1
                    self.retry_count += 1
                    continue

                # 檢查其他 HTTP 錯誤
                response.raise_for_status()

                # 處理響應
                self.logger.debug(f"請求成功，狀態碼: {response.status_code}")
                return self._handle_response(response, error_context)

            except httpx.HTTPStatusError as e:
                self.logger.error(f"{error_context}時發生 HTTP 狀態錯誤: {e}")
                if retries < self.max_retries - 1:
                    retries += 1
                    self.retry_count += 1
                    self.logger.info(f"準備重試 ({retries}/{self.max_retries})...")
                else:
                    raise Exception("請求錯誤")

            except httpx.RequestError as e:
                self.logger.error(f"{error_context}時發生請求錯誤: {e}")
                if retries < self.max_retries - 1:
                    retries += 1
                    self.retry_count += 1
                    self.logger.info(f"準備重試 ({retries}/{self.max_retries})...")
                else:
                    raise Exception("請求錯誤")

            except Exception as e:
                self.logger.error(f"{error_context}時發生錯誤: {e}")
                if retries < self.max_retries - 1:
                    retries += 1
                    self.retry_count += 1
                    self.logger.info(f"準備重試 ({retries}/{self.max_retries})...")
                else:

                    raise Exception(f"未預期錯誤: {str(e)}")

        # 如果所有重試都失敗
        self.logger.error(f"{error_context}失敗，已重試 {self.max_retries} 次")
        raise Exception(f"達到最大重試次數，{error_context}失敗")

    def get(self, url: str, **kwargs) -> Dict:
        """
        發送 GET 請求

        參數:
            url: 請求 URL
            **kwargs: 其他請求參數

        返回:
            Dict: 響應數據或錯誤信息
        """
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> Dict:
        """
        發送 POST 請求

        參數:
            url: 請求 URL
            **kwargs: 其他請求參數

        返回:
            Dict: 響應數據或錯誤信息
        """
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs) -> Dict:
        """
        發送 PUT 請求

        參數:
            url: 請求 URL
            **kwargs: 其他請求參數

        返回:
            Dict: 響應數據或錯誤信息
        """
        return self.request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs) -> Dict:
        """
        發送 DELETE 請求

        參數:
            url: 請求 URL
            **kwargs: 其他請求參數

        返回:
            Dict: 響應數據或錯誤信息
        """
        return self.request("DELETE", url, **kwargs)

    async def async_request(
        self,
        method: str,
        url: str,
        params: Dict = None,
        data: Dict = None,
        json: Dict = None,
        headers: Dict = None,
        cookies: Dict = None,
        timeout: Union[float, httpx.Timeout] = None,
        error_context: str = "請求",
    ) -> Dict:
        """
        發送非同步 HTTP 請求

        參數:
            method: HTTP 方法 (GET, POST, PUT, DELETE 等)
            url: 請求 URL
            params: URL 參數
            data: 表單數據
            json: JSON 數據
            headers: 請求頭
            cookies: 請求 cookies
            timeout: 請求超時
            error_context: 錯誤日誌的上下文描述

        返回:
            Dict: 響應數據或錯誤信息
        """
        # 更新請求計數器
        self.request_count += 1

        # 每 5 次請求更新一次 User-Agent
        if self.rotate_user_agent and self.request_count % 5 == 0:
            self.update_user_agent()

        # 構建完整 URL
        full_url = self._build_url(url)

        # 合併請求頭
        merged_headers = self.headers.copy()
        if headers:
            merged_headers.update(headers)

        # 設置超時
        request_timeout = timeout or self.timeout

        retries = 0
        while retries < self.max_retries:
            try:
                # 添加重試延遲
                if retries > 0:
                    delay = random.uniform(
                        self.min_retry_delay * retries, self.max_retry_delay * retries
                    )
                    self.logger.info(f"重試前等待 {delay:.2f} 秒...")
                    await asyncio.sleep(delay)

                self.logger.debug(
                    f"發送非同步 {method} 請求至 {full_url} (嘗試 {retries + 1}/{self.max_retries})"
                )

                # 創建非同步客戶端並發送請求
                async with httpx.AsyncClient(
                    base_url=self.base_url,
                    headers=merged_headers,
                    timeout=request_timeout,
                    verify=self.verify_ssl,
                    http2=self.http2,
                    follow_redirects=self.follow_redirects,
                    proxy=self.proxy,
                    cookies=cookies or self.cookies,
                ) as client:
                    response = await client.request(
                        method=method, url=full_url, params=params, data=data, json=json
                    )

                    # 檢查是否被封鎖 (403 Forbidden)
                    if response.status_code == 403:
                        self.logger.warning(
                            f"請求被拒絕 (403 Forbidden)，可能被網站封鎖"
                        )
                        if self.rotate_user_agent:
                            self.update_user_agent()
                        retries += 1
                        self.retry_count += 1
                        continue

                    # 檢查是否達到請求限制 (429 Too Many Requests)
                    if response.status_code == 429:
                        # 從響應頭獲取需要等待的時間
                        wait_time = int(response.headers.get("Retry-After", 60))
                        self.logger.warning(
                            f"請求頻率過高 (429 Too Many Requests)，需要等待 {wait_time} 秒"
                        )

                        # 等待指定時間後重試
                        await asyncio.sleep(wait_time)
                        retries += 1
                        self.retry_count += 1
                        continue

                    # 檢查其他 HTTP 錯誤
                    response.raise_for_status()

                    # 處理響應
                    self.logger.debug(f"請求成功，狀態碼: {response.status_code}")
                    return self._handle_response(response, error_context)

            except httpx.HTTPStatusError as e:
                self.logger.error(f"{error_context}時發生 HTTP 狀態錯誤: {e}")
                if retries < self.max_retries - 1:
                    retries += 1
                    self.retry_count += 1
                    self.logger.info(f"準備重試 ({retries}/{self.max_retries})...")
                else:
                    return {"error": f"HTTP 狀態錯誤: {str(e)}"}

            except httpx.RequestError as e:
                self.logger.error(f"{error_context}時發生請求錯誤: {e}")
                if retries < self.max_retries - 1:
                    retries += 1
                    self.retry_count += 1
                    self.logger.info(f"準備重試 ({retries}/{self.max_retries})...")
                else:
                    return {"error": f"請求錯誤: {str(e)}"}

            except Exception as e:
                self.logger.error(f"{error_context}時發生錯誤: {e}")
                if retries < self.max_retries - 1:
                    retries += 1
                    self.retry_count += 1
                    self.logger.info(f"準備重試 ({retries}/{self.max_retries})...")
                else:
                    return {"error": f"未預期錯誤: {str(e)}"}

        # 如果所有重試都失敗
        self.logger.error(f"{error_context}失敗，已重試 {self.max_retries} 次")
        return {"error": f"達到最大重試次數，{error_context}失敗"}

    async def async_get(self, url: str, **kwargs) -> Dict:
        """
        發送非同步 GET 請求

        參數:
            url: 請求 URL
            **kwargs: 其他請求參數

        返回:
            Dict: 響應數據或錯誤信息
        """
        return await self.async_request("GET", url, **kwargs)

    async def async_post(self, url: str, **kwargs) -> Dict:
        """
        發送非同步 POST 請求

        參數:
            url: 請求 URL
            **kwargs: 其他請求參數

        返回:
            Dict: 響應數據或錯誤信息
        """
        return await self.async_request("POST", url, **kwargs)

    async def async_put(self, url: str, **kwargs) -> Dict:
        """
        發送非同步 PUT 請求

        參數:
            url: 請求 URL
            **kwargs: 其他請求參數

        返回:
            Dict: 響應數據或錯誤信息
        """
        return await self.async_request("PUT", url, **kwargs)

    async def async_delete(self, url: str, **kwargs) -> Dict:
        """
        發送非同步 DELETE 請求

        參數:
            url: 請求 URL
            **kwargs: 其他請求參數

        返回:
            Dict: 響應數據或錯誤信息
        """
        return await self.async_request("DELETE", url, **kwargs)


class AsyncHttpAdapter:
    """
    非同步 HTTP 請求適配器類

    此類專門用於非同步 HTTP 請求，提供更簡潔的接口。
    它是 HttpAdapter 的非同步版本，專注於非同步操作。

    實作細節：
    - 使用 httpx.AsyncClient 作為底層 HTTP 客戶端
    - 支持所有常見的 HTTP 方法
    - 提供與 HttpAdapter 相同的重試和錯誤處理機制
    - 專為非同步環境設計
    """

    def __init__(
        self,
        base_url: str = "",
        headers: Dict[str, str] = None,
        timeout: Union[float, httpx.Timeout] = 30.0,
        max_retries: int = 3,
        min_retry_delay: float = 1.0,
        max_retry_delay: float = 5.0,
        user_agents: List[str] = None,
        rotate_user_agent: bool = True,
        verify_ssl: bool = True,
        http2: bool = False,
        follow_redirects: bool = True,
        proxy: str = None,
        cookies: Dict[str, str] = None,
        logger_instance: logging.Logger = None,
    ):
        """
        初始化非同步 HTTP 適配器

        參數與 HttpAdapter 相同
        """
        # 創建內部 HttpAdapter 實例
        self._adapter = HttpAdapter(
            base_url=base_url,
            headers=headers,
            timeout=timeout,
            max_retries=max_retries,
            min_retry_delay=min_retry_delay,
            max_retry_delay=max_retry_delay,
            user_agents=user_agents,
            rotate_user_agent=rotate_user_agent,
            verify_ssl=verify_ssl,
            http2=http2,
            follow_redirects=follow_redirects,
            proxy=proxy,
            cookies=cookies,
            logger_instance=logger_instance,
        )

    @property
    def request_count(self) -> int:
        """獲取請求計數"""
        return self._adapter.request_count

    @property
    def retry_count(self) -> int:
        """獲取重試計數"""
        return self._adapter.retry_count

    def update_user_agent(self) -> None:
        """隨機更新 User-Agent"""
        self._adapter.update_user_agent()

    async def request(self, method: str, url: str, **kwargs) -> Dict:
        """
        發送非同步 HTTP 請求

        參數:
            method: HTTP 方法
            url: 請求 URL
            **kwargs: 其他請求參數

        返回:
            Dict: 響應數據或錯誤信息
        """
        return await self._adapter.async_request(method, url, **kwargs)

    async def get(self, url: str, **kwargs) -> Dict:
        """發送非同步 GET 請求"""
        return await self._adapter.async_get(url, **kwargs)

    async def post(self, url: str, **kwargs) -> Dict:
        """發送非同步 POST 請求"""
        return await self._adapter.async_post(url, **kwargs)

    async def put(self, url: str, **kwargs) -> Dict:
        """發送非同步 PUT 請求"""
        return await self._adapter.async_put(url, **kwargs)

    async def delete(self, url: str, **kwargs) -> Dict:
        """發送非同步 DELETE 請求"""
        return await self._adapter.async_delete(url, **kwargs)


# 使用範例
async def example_usage():
    """
    展示如何使用 HttpAdapter 和 AsyncHttpAdapter
    """
    # 同步請求範例
    sync_adapter = HttpAdapter(base_url="https://api.example.com", max_retries=3)

    # 使用 with 語句自動關閉客戶端
    with sync_adapter as adapter:
        # 發送 GET 請求
        response = adapter.get("/users", params={"page": 1})
        print(f"同步 GET 響應: {response}")

        # 發送 POST 請求
        response = adapter.post("/users", json={"name": "Test User"})
        print(f"同步 POST 響應: {response}")

    # 非同步請求範例
    async_adapter = AsyncHttpAdapter(base_url="https://api.example.com", max_retries=3)

    # 發送非同步 GET 請求
    response = await async_adapter.get("/users", params={"page": 1})
    print(f"非同步 GET 響應: {response}")

    # 發送非同步 POST 請求
    response = await async_adapter.post("/users", json={"name": "Test User"})
    print(f"非同步 POST 響應: {response}")


if __name__ == "__main__":
    # 運行非同步範例
    asyncio.run(example_usage())
