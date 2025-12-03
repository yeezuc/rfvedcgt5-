# Telegram Schedule Bot

Бот для отображения расписания уроков и контрольных из Google Sheets.

## Возможности

- Команды `/today`, `/tomorrow`, `/week`, `/nextweek`, `/date`
- Просмотр контрольных: `/exams`, `/exams_week`, `/exams_nextweek`
- Подписка на уведомления по группе `/subscribe` / `/unsubscribe`
- Админ‑панель с рассылкой и перезагрузкой данных

## Быстрый старт

1. Клонируйте репозиторий или распакуйте этот архив в папку проекта.
2. Создайте и активируйте виртуальное окружение (рекомендуется):

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux / macOS
   .venv\Scripts\activate   # Windows
   ```

3. Установите зависимости:

   ```bash
   pip install -r requirements.txt
   ```

4. Создайте файл `.env` рядом с `bot.py` на основе `.env.example`
   и заполните:
   - `BOT_TOKEN` — токен Telegram‑бота
   - `SPREADSHEET_ID` — ID таблицы Google Sheets
   - `GOOGLE_CREDS_JSON_PATH` **или** `GOOGLE_CREDS_JSON_CONTENT`
   - при необходимости другие переменные

5. Запустите бота:

   ```bash
   python bot.py
   ```

## Заливка на GitHub

1. Создайте новый репозиторий на GitHub (без файлов).
2. В локальной папке проекта выполните:

   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   git branch -M main
   git remote add origin git@github.com:YOUR_USERNAME/YOUR_REPO.git
   git push -u origin main
   ```

3. Замените `YOUR_USERNAME/YOUR_REPO` на свой путь к репозиторию.

