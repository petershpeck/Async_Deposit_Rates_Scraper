from ..generic import GenericBankParser
import re
from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup
from playwright.async_api import Page, Browser
from urllib.parse import urlparse

class PumbParser(GenericBankParser):
    name: str = r'Pumb'
    full_name: str = r'АТ "ПУМБ"'
    nkb: int = 115
    group_1: str = 'Приватний'
    url: str = r'https://persona.pumb.ua/deposits'
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
        base = urlparse(self.url)
        # Регулярное выражение для удаления комментариев
        html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)

        try:
            # Найдём все блоки с депозитами
            blocks = re.findall(r'<div class="deposit-list-card.*?<\/div>\s*<\/div>.*?<div.*?<\/div>\s*<\/div>', html, re.S)
#            print(blocks)
            for block in blocks:
                display = re.search(r'style="display: none;"', block, re.S)
                if display:
                    continue
                # название без слова "Депозит"
                name_match = re.search(r'<div class="deposit-list-title"[^>]*>(.*?)</div>', block, re.S)
                if not name_match:
                    continue
                name = re.sub(r'^\s*Депозит\s*', '', name_match.group(1).strip())
                if name == 'МаніБокс':
                    continue

                # ссылка "Детальніше"
                link_match = re.search(
                    r'<a\s+href="([^"]+)"[^>]*>\s*Детальніше\s*</a>',
                    block,
                    re.S
                )
#                print(link_match)
                if not link_match:
                    continue
                link = link_match.group(1).strip()

                # нормализуем ссылки
                if link.startswith("/"):
                    link = f"{base.scheme}://{base.netloc}{link}"
                elif link.startswith("http"):
                    link = url
                else:
                    link = f"{base.scheme}://{base.netloc}/{link.lstrip('/')}"
                result[name] = link

        except Exception as e:
            # Логируем ошибку и возвращаем то, что успели собрать
            print(f"[ERROR] [{self.name}] extract_allurls failed: {e}")
            return []
        return result

    async def dep_info(self, html: str) -> List[Dict[str, Any]]:
        """
        Ищем  и парсим таблицу внутри 
        Возвращаем список словарей [{'term':..., 'currency':..., 'rate':...}, ...]
        """
        # Список для хранения результатов
        results: List[Dict[str, Any]] = []
        # Маппинг названий валют в ISO
        currency_map = {
            "Гривня": "UAH",
            "Долар США": "USD",
            "Євро": "EUR"
        }
        curr = {}

        try:
            block = re.search(r'<section class="line-tab tabs-wr deposit-rates">.*?<\/section>', html, re.S)
            htm = block.group(0)

            soup = BeautifulSoup(htm, "html.parser")
            
            # Находим все <a ... data-id="..."><span>Валюта</span></a>
            for a in soup.select("div.tabs-btns-wr a[data-id]"):
                data_id = a.get("data-id")
                span = a.find("span")
                # достаём только прямой текст, игнорируя теги <sup>, <i>, и т.п.
                span_text = "".join(span.find_all(text=True, recursive=False)).strip()
                iso = currency_map.get(span_text)
                if iso:
                    curr[data_id] = iso

            # Находим все блоки tab-pane
            for tab in soup.select("div.tab-pane"):
                data_id = tab.get("data-id")
                currency = curr.get(data_id)
                if not currency:
                    continue
            
                # берём только те header-row, что НЕ находятся внутри transparent-table
                headers = []
                for hdr in tab.select(".row.header-row"):
                    if hdr.find_parent("div", class_="transparent-table"):
                        continue  # пропускаем "прозрачные" таблицы

                # Заголовки (term)
                    terms = []
                    for col in hdr.select(".col"):
                        text = col.get_text(" ", strip=True)
                        m = re.search(r'(\d+)\s*міс', text)
                        if m:
                            terms.append(m.group(1))

#                    print(terms)
            
                    # Ставки (rate)
                    # ищем соответствующую строку со ставками
                    for row in tab.select(".row:not(.header-row)"):
                        if row.find_parent("div", class_="transparent-table"):
                            continue
            
                        rates = []
                        for col in row.select(".col"):
                            text = col.get_text(" ", strip=True)
                            if "%" in text:
                                rate = text.replace("%", "").replace(",", ".").strip()
                                rates.append(rate)
            
                        # соединяем terms и rates по индексам
                        for term, rate in zip(terms, rates):
                            results.append({
                                "currency": currency,
                                "term": term,
                                "rate": rate
                            })

            return results
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
#                print(infos)
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
