from ..generic import GenericBankParser
from typing import Dict, List, Any
import json
import re
from playwright.async_api import Browser

class PrivatbankParser(GenericBankParser):
    name: str = r'Privat'
    full_name: str = r'АТ КБ "ПриватБанк"'
    nkb: int = 46
    group_1: str = 'Державний'
    url_dep: str = r'https://deposits.privatbank.ua/static/app/open.htm'
    url: str = r'https://deposits.privatbank.ua/static/app/js/programs.js'

    def __init__(self, config=None):
        cfg = config or {}
        super().__init__(cfg)

    async def parse_detail(self, browser: Browser, main_html: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []

        # используем регулярку, чтобы вытащить содержимое массива
        match = re.search(r'var programs\s*=\s*(\[.*?\]);', main_html, re.DOTALL)
        if match:
            programs_json = match.group(1)
            programs = json.loads(programs_json)

        for program in programs:
            if program.get("code") in ('DEN0','DENK','DDND','DPSG','DPR0'):
                product_name = program.get("name")
                for rate_info in program.get("rates", []):
                    term = rate_info.get("duration")
                    for currency, value in rate_info.get("curr", {}).items():
                        rows.append({
                            "product": product_name,
                            "term": term,
                            "currency": currency.upper(),
                            "rate": value.get("rate"),
                            "bank": self.name,
                            "full_name": self.full_name,
                            "nkb": self.nkb,
                            "group_1": self.group_1,
                            "source_url": self.url_dep
                        })

        return rows
