import re, asyncio, os
from typing import List, Dict, Any, Optional
from playwright.async_api import Page, Browser
from bs4 import BeautifulSoup
from urllib.parse import urlparse

class GenericBankParser:
    """
    Базовый абстрактный класс для всех банковских парсеров.
    """
    name: str = "GenericBank"
    url: str = ""
    timeout: int = 120
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    config: Dict[str, Any] = {}
    
    def __init__(self, config: Dict[str, Any] = None):
        if config:
            self.config = config
            self.timeout = int(config.get('timeout', self.timeout))  # Время ожидания загрузки
            self.user_agent = config.get('user_agent', self.user_agent)
        self.AllUrls: Dict[str, str] = {}   # {product_name: product_url}

    async def fetch_page(self, browser: Browser, url: str, timeout: Optional[int] = None) -> Optional[str]:
        timeout = timeout or self.timeout
        page = None
        try:
            context = await browser.new_context(user_agent=self.user_agent) if self.user_agent else await browser.new_context()
            page = await context.new_page()
            await page.goto(url, timeout=timeout*1000, wait_until='domcontentloaded')
            # attempt to trigger lazy load
            try:
                await page.wait_for_load_state('networkidle', timeout=5000)
            except Exception:
                pass
            for _ in range(3):
                await page.evaluate('window.scrollBy(0, document.body.scrollHeight/4)')
                await asyncio.sleep(0.5)
            html = await page.content()
            await page.close()
            await context.close()
            return html
        except Exception as e:
            print(f"[ERROR] Fetch_page {self.name} {url}: {e}")
            try:
                if page:
                    await page.close()
            except Exception:
                pass
            return None

    # ------------------------------------------------------------
    # Основной процесс парсинга
    # ------------------------------------------------------------
    async def parse(self, browser: Browser) -> List[Dict[str,Any]]:
        print(f"[INFO] [{self.name}] Start parse")
        products = []
        
        # Шаг 1. Открываем главную страницу
        # 1) load main page and find all urls
        main_html = await self.fetch_page(browser, self.url, timeout=self.timeout)
        if not main_html:
            return products

        try:
            products = await self.parse_detail(browser, main_html)
        except Exception as e:
            print(f"[ERROR] [{self.name}] Failed parse: {e}")

        print(f"[INFO] [{self.name}] Stop parse. Total records: {len(products)}")
        return products
                                                      