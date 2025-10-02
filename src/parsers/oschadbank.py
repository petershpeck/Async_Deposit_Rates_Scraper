from ..generic import GenericBankParser
import re
from typing import Dict, List, Any
from bs4 import BeautifulSoup
from playwright.async_api import Page, Browser
from urllib.parse import urlparse

# Карта валют для поиска в колонках и в скобках
currency_map = {
    r"(грн|гривня)": "UAH",
    r"(usd|долар\s*сша)": "USD",
    r"(eur|євро)": "EUR",
}

def detect_columns(soup: BeautifulSoup) -> List[str]:
    """Определение названий колонок по <th>"""
    return [th.get_text(strip=True).lower() for th in soup.find_all("th")]

def find_currency_in_text(text: str) -> str:
    """Поиск валюты в тексте по словарю currency_map"""
    text_lower = text.lower()
    for pattern, currency in currency_map.items():
        if re.search(pattern, text_lower):
            return currency
    return None

def parse_table(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Парсинг таблицы с динамическим определением валют и режимов"""
    headers = detect_columns(soup)
    results = []

    # Определяем валютные колонки по заголовкам
    currency_columns = {}
    for idx, h in enumerate(headers):
        curr = find_currency_in_text(h)
        if curr:
            currency_columns[idx] = curr

    # Режим, когда у нас только term и rate или нет валютных колонок
    simple_mode = len(currency_columns) == 0

    for row in soup.select("tbody tr"):
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) < 2:
            continue

        term_raw = cols[0]

        # Извлекаем число месяцев — первое число до слова "місяц"
        term_match = re.search(r"(\d+)\s*", term_raw.lower())
        term = term_match.group(1) if term_match else term_raw

        if simple_mode:
            # Валюта внутри первой колонки, например "12 місяців (грн)"
            currency = find_currency_in_text(term_raw)
            rate_raw = cols[1].replace(',', '.').replace('%', '').strip()
            try:
                rate = float(rate_raw)
            except ValueError:
                rate = None
            results.append({
                "term": term,
                "currency": currency,
                "rate": rate
            })
        else:
            # Несколько валютных колонок
            for idx, cell in enumerate(cols[1:], start=1):
                if idx in currency_columns:
                    currency = currency_columns[idx]
                    rate_raw = cell.replace(',', '.').replace('%', '').strip()
                    try:
                        rate = float(rate_raw)
                    except ValueError:
                        rate = None
                    results.append({
                        "term": term,
                        "currency": currency,
                        "rate": rate
                    })

    return results

class OschadbankParser(GenericBankParser):
    name: str = r'Oschadbank'
    full_name: str = r'АТ "Ощадбанк"'
    nkb: int = 6
    group_1: str = 'Державний'
    url: str = r'https://www.oschadbank.ua/deposits'
    AllUrls: Dict[str,str] = {} #None #[]

    def __init__(self, config=None):
        cfg = config or {}
        super().__init__(cfg)

    async def extract_allurls(self, html: str) -> Dict[str, str]:
        """
        Парсим главную страницу и возвращаем словарь {title: absolute_url}.
        Использует self.url для формирования абсолютных ссылок.
        """
        result: Dict[str, str] = {}
        try:
            soup = BeautifulSoup(html, "html.parser")

            section = soup.find("section", class_="all-private-deposits")
            if not section:
                return {}

            articles = section.find_all("article", class_="all-private-deposits-card")

            for article in articles:
                # заголовок
                h3_tag = article.find("h3", class_="base-title")
                title = h3_tag.get_text(strip=True) if h3_tag else None

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

    async def dep_info(self, html: str) -> List[Dict[str, Any]]:
        """
        Ищем section.block-table-rates и парсим таблицу внутри (через parse_table).
        Возвращаем список словарей [{'term':..., 'currency':..., 'rate':...}, ...]
        """
        try:
            soup = BeautifulSoup(html, "html.parser")
            section = soup.find("section", class_="block-table-rates")
            if not section:
                # можно логировать
                # print(f"[WARN] [{self.name}] Block not found <section class='block-table-rates'>")
                return []
            return parse_table(section)  # parse_table ожидает BeautifulSoup-объект
        except Exception as e:
            print(f"[ERROR] [{self.name}] dep_info failed: {e}")
            return []

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

                infos = await self.dep_info(html)   # await т.к. dep_info — async
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
