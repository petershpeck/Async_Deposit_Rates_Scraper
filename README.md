# Async Deposit Rates Scraper v.10 (Playwright, headless, enhanced)

Особливості:
- Налаштувати тймаут завантаження сторінок
- Очікування конкретного CSS-селектора с даними
- stealth-режим для обходу захисту від ботів
- скролл сторінки для підвантаження ледачих елементів
## Налаштування
```bash
python -m venv .venv
# Linux
source .venv/bin/activate
# Windows:
.venv\Scripts\activate
pip install -r requirements.txt
playwright install
```
## Запуск
Заповнити config.env
```
python -m src.main
```

Результат: файл `output/Deposit_Rate_Data.xlsx`.
