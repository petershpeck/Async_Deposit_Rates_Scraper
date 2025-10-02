import asyncio, configparser, importlib, os
from playwright.async_api import async_playwright
from typing import Dict
from .xlsx import save_all_to_xlsx

CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config.env'))

def load_config(path=CONFIG_PATH):
    cp = configparser.ConfigParser(interpolation=None)
    cp.read(path, encoding='utf-8')
    return cp

def build_parser_instances(cp) -> Dict[str, object]:
    instances = {}
    general = cp['GENERAL'] if 'GENERAL' in cp else {}
    for section in cp.sections():
        if section == 'GENERAL':
            continue
        conf = cp[section]
        active = conf.get('active', 'False')
        if active.lower() not in ('1','true','yes','on'):
            continue
        # assemble config dict
        cfg = {}
        # override general timeout/user_agent if not present
        cfg['timeout'] = conf.get('timeout', general.get('timeout'))
        cfg['user_agent'] = conf.get('user_agent', general.get('user_agent'))
        # dynamic import parser module from src.parsers.<section_lower>
        mod_name = section.lower().replace(' ','')
        try:
            mod = importlib.import_module(f'src.parsers.{mod_name}')
            class_name = section.title().replace(' ','').replace('-','') + 'Parser'
            ParserClass = getattr(mod, class_name)
            instances[section] = ParserClass(cfg)
        except Exception as e:
            print(f"[WARN] Could not import parser for {section}: {e}")
    return instances

async def run_all():
    cp = load_config()
    print(f"[INFO] Config loaded")
    parsers = build_parser_instances(cp)
    print(f"[INFO] Parsers builded")
    max_thread = int(cp['GENERAL'].get('max_thread', 3)) if 'GENERAL' in cp else 3
    semaphore = asyncio.Semaphore(max_thread)
    all_results = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox','--disable-gpu'])

        async def run_parser(name, parser):
            async with semaphore:
                try:
                    print(f"[INFO] Starting {name}")
                    products = await parser.parse(browser)
                    all_results[name] = products
                    print(f"[INFO] Finished {name} -> {len(products)} items")
                except Exception as e:
                    print(f"[ERROR] {name} failed: {e}")
                    all_results[name] = []

        tasks = [run_parser(name, parser) for name, parser in parsers.items()]
        await asyncio.gather(*tasks)
        await browser.close()

    # save excel
    out_file = cp['GENERAL'].get('output_file', 'output/Deposit_Rate_Data.xlsx') if 'GENERAL' in cp else 'output/Deposit_Rate_Data.xlsx'
    save_all_to_xlsx(all_results, out_file)

if __name__ == '__main__':
    asyncio.run(run_all())
