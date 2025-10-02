from ..generic import GenericBankParser
import re
from typing import Dict, List, Any
#from bs4 import BeautifulSoup
from playwright.async_api import Page, Browser
from urllib.parse import urlparse

class UkreximbankParser(GenericBankParser):
    name: str = r'UkrEximBank'
    full_name: str = r'АТ "Укрексімбанк"'
    nkb: int = 2
    group_1: str = 'Державний'
    url: str = r'https://www.eximb.com/ua/business/pryvatnym-klientam/pryvatnym-klientam-depozyty/'
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
            pattern = re.compile(
#                r'<a\s+href="([^"]+)"\s+class="direction-item wide-item">.*?<h3 class="direction-text">\s*Депозит\s+«([^»]+)».*?</h3>',
                r'<a\s+href="([^"]+)"\s+class="direction-item wide-item">.*?<h3 class="direction-text">\s*(.*?)\s*</h3>',
                re.DOTALL
            )

            matches = pattern.findall(html)
            if not matches:
                return result

            base = urlparse(self.url)

            for url, full_text in matches:
                # чистим пробелы и переносы
                full_text = re.sub(r'\s+', ' ', full_text).strip()

                # фильтруем по ключевым словам
                if "не залучаються" in full_text.lower() or "калькулятор" in full_text.lower():
                    continue

                # достаём описание из кавычек «...»
                m = re.search(r'«([^»]+)»', full_text)
                if m:
                    desc = m.group(1).strip()
                    # нормализуем ссылки
                    if url.startswith("/"):
                        link = f"{base.scheme}://{base.netloc}{url}"
                    elif url.startswith("http"):
                        link = url
                    else:
                        link = f"{base.scheme}://{base.netloc}/{url.lstrip('/')}"
                    result[desc] = link

        except Exception as e:
            # Логируем ошибку и возвращаем то, что успели собрать
            print(f"[ERROR] [{self.name}] extract_allurls failed: {e}")
        return result

    async def dep_info(self, html: str) -> List[Dict[str, Any]]:
        """
        Ищем  и парсим таблицу внутри 
        Возвращаем список словарей [{'term':..., 'currency':..., 'rate':...}, ...]
        """
        try:
            # 1. достаём содержимое div.additional-info text-block
            div_match = re.search(
                r'<div class="additional-info text-block">(.*?)</div>',
                html,
                re.DOTALL
            )
            if not div_match:
                raise ValueError("[ERROR] Таблица не найдена в <div class='additional-info text-block'>")

            div_html = div_match.group(1)

            # 2. достаём заголовки таблицы (<th>...</th>)
            headers = re.findall(r'<th[^>]*>(.*?)</th>', div_html, re.DOTALL)
#            headers = [re.sub(r'<.*?>', '', h).strip().lower() for h in headers]

            currencies = [h.strip().lower() for h in headers[1:]]  # пропускаем "строк"

            # маппинг валют
            cur_map = {
                "гривня": "UAH",
                "долар сша": "USD",
                "євро": "EUR"
            }
            currencies = [cur_map[c] for c in currencies]

            # 2. достаём строки <tr>...</tr>
            rows = re.findall(r'<tr>(.*?)</tr>', html, re.DOTALL)

            result: List[Dict[str, Any]] = []

            for row in rows:
                cols = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
                if not cols:
                    continue

                term_text = re.sub(r'<.*?>', '', cols[0]).strip()  # чистый текст
                rates = [re.sub(r'<.*?>', '', c).strip() for c in cols[1:]]
                rates = [r.replace("\xa0", "").replace("%", "") for r in rates]

                # извлекаем дни (пример: "93 - 183 дні")
                m = re.search(r'(\d+)\s*-\s*(\d+)', term_text)
                if not m:
                    continue
                d1, d2 = map(int, m.groups())
#                m1, m2 = d1 // 30, d2 // 30
                if d1==93 and d2==183:
                    m1, m2 = 3, 6
                elif d1==184 and d2==367:
                    m1, m2 = 7, 12
                elif d1==368 and d2==3650:
                    m1, m2 = 13, 121
                else:
                    m1, m2 = 0, 0

                for month in (m1,m2): #range(m1, m2 + 1):
                    for cur, rate in zip(currencies, rates):
                        result.append({
                            "term": month,
                            "currency": cur,
                            "rate": float(rate.replace("&nbsp;", ""))
                        })
            return result
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
                print(infos)
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
