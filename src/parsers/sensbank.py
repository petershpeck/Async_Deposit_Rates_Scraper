from ..generic import GenericBankParser
import re
from typing import Dict, List, Any, Optional

import aiohttp
import asyncio
import pdfplumber
import io

from bs4 import BeautifulSoup
from playwright.async_api import Page, Browser
import pandas as pd

from urllib.parse import urlparse

class SensbankParser(GenericBankParser):
    name: str = r'Sensbank'
    full_name: str = r'АТ "СЕНС БАНК"'
    nkb: int = 272
    group_1: str = 'Державний'
    url_dep: str = r'https://deposits.privatbank.ua/static/app/open.htm'
    url: str = r'https://sensebank.ua/deposits'
    AllUrls: Dict[str,str] = {} #None #[]

    def __init__(self, config=None):
        cfg = config or {}
        self.session: Optional[aiohttp.ClientSession] = None
        super().__init__(cfg)

    async def __aenter__(self):
        await self.create_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_session()
    
    async def create_session(self):
        """Создает асинхронную сессию"""
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)
    
    async def close_session(self):
        """Закрывает асинхронную сессию"""
        if self.session:
            await self.session.close()

    async def download_pdf(self, url: str) -> Optional[bytes]:
        """
        Асинхронно загружает PDF из интернета
        """
        if not self.session:
            await self.create_session()
        
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                return await response.read()
        except Exception as e:
            print(f"Ошибка при загрузке PDF: {e}")
            return None

    async def parse_rates_from_pdf(self, pdf_content: bytes) -> List[Dict[str, Any]]:
        """
        Извлекает данных из PDF (выполняется в отдельном потоке)
        """
#        print(pdf_content)
        loop = asyncio.get_event_loop()
        
        def sync_extract_text():
#            text = ""
            result: List[Dict[str, Any]] = []
            try:
                with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
#                    for page in pdf.pages:
#                        page_text = page.extract_text()
#                        if page_text:
#                            text += page_text + "\n"
                     page = pdf.pages[0]

                    # Шаг 3. Извлекаем таблицы с указанной страницы
                     tables = page.extract_tables()
                     if not tables:
                         print(f"[ERROR] Таблицы не найдены на странице {page_number + 1}")
                         return None
                     table = tables[1]

                     df = pd.DataFrame(table[1:], columns=table[0])
                     # Убедимся в названиях столбцов
                     # ['Термін', 'UAH', 'USD', 'EUR']
                     df.columns = ["term_raw", "UAH", "USD", "EUR"]
#                     print(f"df= {df}")
                     # Шаг 4. Преобразуем в список словарей
                     for _, row in df.iterrows():
                         # Извлекаем только первую цифру из term

                         term_raw = str(row.get("term_raw", "")).strip()  # гарантируем строку

                         if not term_raw:
                             continue  # пропускаем пустую строку

                         match = re.search(r"(\d+)", term_raw)
                         if not match:
                             continue  # если нет цифры, пропускаем

#                         if row["term_raw"]:
#                             match = re.search(r"(\d+)", row["term_raw"])
#                         if not match:
#                             continue
                         term = int(match.group(1))
#                         print(f"term= {term}")
                         # Проходим по валютам
                         for currency in ["UAH", "USD", "EUR"]:
                             rate_raw = str(row[currency]).strip().replace("%", "").replace(",", ".")
#                             print(f"rate_raw= {rate_raw}")
                             if rate_raw == "" or rate_raw.lower() == "nan":
                                 continue
                             try:
                                 rate = float(rate_raw)
                                 print(f"rate= {rate}")
                             except ValueError:
                                 continue

                             result.append({
                                 "term": term,
                                 "currency": currency,
                                 "rate": rate
                             })
                             print(f"result= {result}")
                     return result

            except Exception as e:
                print(f"Ошибка при извлечении данных из PDF: {e}")
            return result
        
        return await loop.run_in_executor(None, sync_extract_text)

    async def parse_rates_from_pdf_new(self, pdf_content: bytes) -> List[Dict[str, Any]]:
        """
        Извлекает данных  из PDF (выполняется в отдельном потоке)
        """
        result: List[Dict[str, Any]] = []
        try:
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                 page = pdf.pages[0]
                # Шаг 3. Извлекаем таблицы с указанной страницы
                 tables = page.extract_tables()
                 if not tables:
                     print(f"Таблицы не найдены на странице {page_number + 1}")
                     return None
                 table = tables[1]


                 df = pd.DataFrame(table[1:], columns=table[0])

                 # Убедимся в названиях столбцов
                 # ['Термін', 'UAH', 'USD', 'EUR']
                 df.columns = ["term_raw", "UAH", "USD", "EUR"]

                 # Шаг 4. Преобразуем в список словарей
                 for _, row in df.iterrows():
                     # Извлекаем только первую цифру из term
                     match = re.search(r"(\d+)", row["term_raw"])
                     if not match:
                         continue
                     term = int(match.group(1))

                     # Проходим по валютам
                     for currency in ["UAH", "USD", "EUR"]:
                         rate_raw = str(row[currency]).strip().replace("%", "").replace(",", ".")
                         if rate_raw == "" or rate_raw.lower() == "nan":
                             continue
                         try:
                             rate = float(rate_raw)
                         except ValueError:
                             continue

                         result.append({
                             "term": term,
                             "currency": currency,
                             "rate": rate
                         })

                 return result

        except Exception as e:
            print(f"Ошибка при извлечении данных из PDF: {e}")
        return result

    async def extract_allurls(self, html: str) -> Dict[str, str]:
        """
        Парсим главную страницу и возвращаем словарь {title: absolute_url}.
        Использует self.url для формирования абсолютных ссылок.
        """
        result: Dict[str, str] = {}
        try:
            soup = BeautifulSoup(html, "html.parser")

            section = soup.find("section", class_="deposit-list")
            if not section:
                return {}

            articles = section.find_all("article", class_="deposit-card")

            for article in articles:
                # заголовок
                h3_tag = article.find("h3", class_="base-title")
                title = h3_tag.get_text(strip=True) if h3_tag else None

                d_tag = article.find("div", class_="deposit-card__content text")
                term = d_tag.get_text(strip=True) if d_tag else None
                if not term or not term.startswith("На термін від"):
                    continue

                # первая ссылка внутри карточки
                a_tag = article.find("a", href=True)
                link = a_tag["href"].strip() if a_tag else None

                if not link or not title:
                    continue

                # если относительная — собрать абсолютный URL
                if link.startswith("/"):
                    base = urlparse(self.url)
                    link = f"{base.scheme}://{base.netloc}{link}"

                result[title] = link

        except Exception as e:
            # Логируем ошибку и возвращаем то, что успели собрать
            print(f"[ERROR] [{self.name}] extract_allurls failed: {e}")
        return result

    async def dep_info(self, url: str) -> List[Dict[str, Any]]:
        """
        Основной метод для получения данных о депозитных ставках
        """
        print(f"[INFO] Load PDF -> {url}")
        pdf_content = await self.download_pdf(url)
        await self.close_session()

        if not pdf_content:
            print(f"[WARNING] Failed to load PDF ({url})")
            return []
                
        print("Парсинг данных...")
        rates_data = await self.parse_rates_from_pdf(pdf_content)
        
        return rates_data

    async def parse_detail(self, browser: Browser, main_html: str) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []

        # Формируем AllUrls (await т.к. метод асинхронный)
        try:
            self.AllUrls = await self.extract_allurls(main_html)
        except Exception as e:
            print(f"[ERROR] [{self.name}] extract_allurls raised: {e}")
            return result

        if not self.AllUrls:
            print(f"[WARNING] [{self.name}] No deposit products found.")
            return result

        # Перебираем словарь {product_name: product_url}
        for idx, (product_name, product_url) in enumerate(self.AllUrls.items(), start=1):
            print(f"[DEBUG] [{self.name}] ({idx}/{len(self.AllUrls)}) Processing '{product_name}' -> {product_url}")
            try:
                html = await self.fetch_page(browser, product_url, timeout=self.timeout)
                if not html:
                    print(f"[WARN] [{self.name}] Empty html for {product_name}")
                    continue

                pattern = r'<a href="(/upload/PASPORT_PRODUKTA_.*?.pdf)".*?>Паспорт продукта.*?'+product_name+'</a>'
                match = re.search(pattern,html)
                if match:
                    link = match.group(1)
                    # если относительная — собрать абсолютный URL
                    if link.startswith("/"):
                        base = urlparse(self.url)
                        link = f"{base.scheme}://{base.netloc}{link}"
                else:
                    print(f"[WARN] [{self.name}] not found pdf-link for {product_name}")
                    continue                    

                infos = await self.dep_info(link)   # await т.к. dep_info — async
                if not infos:
                    print(f"[WARN] [{self.name}] infos returned empty for {product_name}")
                    continue

                # нормализуем записи и добавляем метаданные
                for inf in infos:
                    if isinstance(inf, dict):
                        inf["bank"] = self.name
                        inf["full_name"] = self.full_name
                        inf["nkb"] = self.nkb
                        inf["group_1"] = self.group_1
                        inf["product"] = product_name
                        inf["source_url"] = product_url
                        result.append(inf)

            except Exception as e:
                print(f"[ERROR] [{self.name}] Error processing {product_name}: {e}")

        return result
