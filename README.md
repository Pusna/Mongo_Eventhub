# 🍃 EventHub Pro (MongoDB)
 
Навчальний проєкт: платформа подій (користувачі, події, реєстрації, відгуки) на MongoDB 7.0. Кожен розділ завдання показує окрему можливість MongoDB: індекси, транзакції, реплікацію, шардинг, change streams, GridFS, Atlas та інше.
 
---
 
## 🛠️ Технологічний стек
 
- MongoDB 7.0 (replica set із 3 вузлів) у Docker та Docker Compose
- Python 3.11: PyMongo, FastAPI, pydantic-settings
- pytest та pytest-cov для тестів
---
 
## 🏗️ Архітектура та вимоги
 
Replica set `rs0` складається з трьох вузлів: `mongo-primary`, `mongo-secondary1`, `mongo-secondary2`.
 
Основна база називається `eventhub`, у ній колекції `users`, `events`, `registrations`, `reviews` та `audit_log`.
 
| Розділ завдання | Файл | Що робить |
|---|---|---|
| 1-2. Схема та інфраструктура | `app/config.py`, `app/repository.py`, `app/schemas.py`, `docker-compose.yml`, `scripts/` | Підключення, індекси, валідація даних, запуск вузлів |
| 3. Базові операції | `app/operations.py`, `app/seed.py` | Створення, пошук, реєстрація в транзакції, відгуки |
| 4. Індекси | `app/optimizer.py` | Індекси, `explain`, профайлер |
| 5. Транзакції | `app/transactions.py` | Атомарна реєстрація, оптимістичне блокування |
| 6. Агрегації | `app/analytics.py` | Статистика, рекомендації, вітрини |
| 7. Реплікація | `app/replication.py` | Статус, read/write concern, failover |
| 8. Шардинг | `app/sharding.py`, `app/sharding_setup.py` | Розподіл даних між шардами, зони |
| 9. Change Streams | `app/streams.py` | Підписка на зміни, resume token |
| 10. GridFS | `app/files.py` | Зберігання файлів, мініатюри |
| 11. Atlas | `app/atlas.py`, `atlas/functions/` | Atlas Search, функція для тригера |
| 12. Моніторинг | `app/monitoring.py` | Метрики, бекап, сповіщення |
| 13. Тести | `tests/` | pytest |
| 14. API та Docker | `app/main.py`, `Dockerfile`, `k8s/` | FastAPI, образ, маніфести Kubernetes |
 
---
 
## 📁 Структура проєкту
 
```
Mongo_Eventhub/
├── app/               код за розділами завдання
├── atlas/functions/   функція для Atlas Trigger
├── k8s/               маніфести Kubernetes
├── scripts/           init-replica.js, init-sharding.js
├── tests/             тести (pytest)
├── docker-compose.yml
├── Dockerfile
├── pytest.ini
├── requirements.txt
├── .env.example
└── README.md
```
 
---
 
## 🚀 Налаштування та запуск
 
### 1️⃣ Віртуальне середовище та залежності
 
```
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```
 
### 2️⃣ Файл `.env`
 
Скопіюйте шаблон і замініть паролі `change_me_*`:
 
```
copy .env.example .env
```
 
- `MONGO_ADMIN_PASSWORD`, `MONGO_APP_PASSWORD`: обов'язково
- `GRAFANA_PASSWORD`: лише для профілю `monitoring`
- `ATLAS_URI`: необов'язково, для розділу 11 (розкоментуйте рядок і впишіть свій рядок підключення)
Решта значень у шаблоні підходить для локального запуску. Інші налаштування (`MONGO_TEST_DB`, ключі Atlas API, SMTP, Slack, `CORS_ORIGINS` тощо) мають значення за замовчуванням в `app/config.py`. Файл `.env` не потрапляє в git.
 
### 3️⃣ Файл hosts
 
Додайте у файл hosts (Windows: `C:\Windows\System32\drivers\etc\hosts`, потрібні права адміністратора) рядок:
 
```
127.0.0.1 mongo-primary mongo-secondary1 mongo-secondary2
```
 
Це потрібно, щоб драйвер бачив усі вузли replica set за іменами.
 
### 4️⃣ Запуск кластера
 
Контейнер `mongo-init` один раз налаштує replica set і створить користувачів, потім зупиниться:
 
```
docker compose up -d
```
 
### 5️⃣ Тестові дані
 
Повторний запуск нічого не дублює:
 
```
python -m app.seed
```
 
### 6️⃣ API
 
Запустіть і відкрийте `http://localhost:8000/docs`:
 
```
python -m uvicorn app.main:app --port 8000
```
 
### 🧩 Шардинг (необов'язково)
 
```
docker compose --profile sharding up -d
python -m app.sharding_setup
```
 
Підніме config server, два шарди та `mongos` (порт 27025).
 
---
 
## 🔐 Доступ і ролі
 
| Користувач | База | Права |
|---|---|---|
| `admin` | `admin` | `root`, `clusterAdmin` (адміністрування кластера) |
| `app_user` | `eventhub` | `readWrite`, `dbAdmin` (робота застосунку) |
 
Паролі задаються у `.env`. Застосунок підключається як `app_user`, рядок підключення збирає `app/config.py`.
 
---
 
## ✅ Тести
 
```
python -m pytest -q
```
 
Тести працюють в окремій базі `eventhub_test`, робоча база не змінюється.
 
