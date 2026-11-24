# Лабораторна 2 (Мілстоун 2): Шардинг

**Мета:** побудувати розподілене key-value сховище, навчитися розкладати дані по кількох вузлах через сервіс-координатор, який маршрутизує кожен ключ на відповідний шард.

**Статус:** реалізовано повний максимум (**15/15 балів**: 10 базових + 5 розширених).

---

## Архітектура та файли
- [openapi.yaml](file:///D:/Потрібне/5%20курс/ХТС/Lab_2_Sharding/openapi.yaml) — OpenAPI 3.1 специфікація (реєстрація таблиць зі складеним ключем + `EXISTS`, `CREATE`, `READ`, `DELETE`). Живий Swagger доступний на `http://localhost:8000/docs`.
- [coordinator/app.py](file:///D:/Потрібне/5%20курс/ХТС/Lab_2_Sharding/coordinator/app.py) та [coordinator/hashing.py](file:///D:/Потрібне/5%20курс/ХТС/Lab_2_Sharding/coordinator/hashing.py) — координатор із `ConsistentHashRing` (`vnodes = 100`, SHA-256) і режимом `modulo` для порівняння.
- [shard/app.py](file:///D:/Потрібне/5%20курс/ХТС/Lab_2_Sharding/shard/app.py) та [shard/storage.py](file:///D:/Потрібне/5%20курс/ХТС/Lab_2_Sharding/shard/storage.py) — вузол-шард на SQLite (WAL mode) з автореєстрацією при старті та підтримкою `partition_key + sort_key`.
- [docker-compose.yml](file:///D:/Потрібне/5%20курс/ХТС/Lab_2_Sharding/docker-compose.yml) — координатор + 2 шарди (`shard-1`, `shard-2`) + профіль `scale` для додавання `shard-3`.
- [.github/workflows/deploy.yml](file:///D:/Потрібне/5%20курс/ХТС/Lab_2_Sharding/.github/workflows/deploy.yml) — GitHub Actions CI/CD (тести, збірка/пуш образів у GHCR, автодеплой на EC2).
- [scripts/compare_hashing.py](file:///D:/Потрібне/5%20курс/ХТС/Lab_2_Sharding/scripts/compare_hashing.py) — експеримент на 10 000 ключів (порівняння `hash % N` vs `ConsistentHashRing`).
- [tests/test_sharding.py](file:///D:/Потрібне/5%20курс/ХТС/Lab_2_Sharding/tests/test_sharding.py) — автотести.

---

## Запуск і демонстрація на захисті

### 1. Запуск кластера
```powershell
cd "D:\Потрібне\5 курс\ХТС\Lab_2_Sharding"
docker compose up -d --build
```
Перевірити зареєстровані шарди:
```powershell
curl.exe http://localhost:8000/shards
```

### 2. Перевірка CRUD і складеного ключа (PowerShell)
```powershell
# Реєстрація таблиці зі складеним ключем
Invoke-RestMethod -Method Post http://localhost:8000/tables -ContentType "application/json" -Body '{"name":"orders","partition_key":"user_id","sort_key":"order_id"}'

# CREATE
Invoke-RestMethod -Method Post http://localhost:8000/tables/orders/records -ContentType "application/json" -Body '{"partition_key":"user-42","sort_key":"2026-11-25#002","value":{"total":300}}'
Invoke-RestMethod -Method Post http://localhost:8000/tables/orders/records -ContentType "application/json" -Body '{"partition_key":"user-42","sort_key":"2026-11-25#001","value":{"total":150}}'

# EXISTS
Invoke-RestMethod "http://localhost:8000/tables/orders/records/user-42/exists?sort_key=2026-11-25%23001"

# READ (усі замовлення user-42, відсортовані за sort_key)
Invoke-RestMethod "http://localhost:8000/tables/orders/records/user-42"

# DELETE
Invoke-RestMethod -Method Delete "http://localhost:8000/tables/orders/records/user-42?sort_key=2026-11-25%23001"
```

### 3. Експеримент consistent hashing (2 → 3 шарди)
```powershell
python scripts/compare_hashing.py
```
Додати 3-й живий шард у Docker Compose:
```powershell
docker compose --profile scale up -d shard-3
```

---

## Чекліст балів (15/15)
- [x] **Специфікація API (2 бали):** `openapi.yaml` + `/docs`.
- [x] **Координатор + `EXISTS`/`CREATE`/`READ`/`DELETE` (4 бали):** `coordinator/app.py`.
- [x] **Шардинг запису + автореєстрація шардів (3 бали):** `POST /shards/register` при старті кожного шарда.
- [x] **Шардинг читання (1 бал):** `READ` / `EXISTS` маршрутизуються тільки на цільовий шард.
- [x] **Consistent hashing з віртуальними вузлами (2 бали):** `ConsistentHashRing` (`vnodes=100`) + `scripts/compare_hashing.py`.
- [x] **Складений первинний ключ (2 бали):** `partition_key` + `sort_key` (шардинг за `partition_key`, сортування за `sort_key`).
- [x] **GitHub Action автодеплою (1 бал):** `.github/workflows/deploy.yml`.

---

## Відповіді на контрольні питання до захисту
1. **Види масштабування:**
   - *Вертикальне (Scale Up)* — збільшення CPU/RAM/диска одного сервера. Просте в розробці, не ламає код, але має фізичну стелю ціни й заліза та лишає єдину точку відмови (SPOF).
   - *Горизонтальне (Scale Out)* — додавання нових вузлів (шардів) у кластер. Дає майже необмежене зростання місткості й пропускної здатності, але ускладнює маршрутизацію, реплікацію та міжвузлові запити.
2. **Типи шардингу:**
   - *Hash-based* (`hash(key) % N` або Consistent Hashing) — рівномірно розмазує ключі по вузлах; ідеальний для точкових KV-запитів, гірший для діапазонних сканів.
   - *Range-based* (діапазони `A-F`, `G-M`) — швидкі range-запити (`BETWEEN`), але ризик перекосу (hot shard на свіжих датах).
   - *Directory/Lookup-based* — окрема таблиця відповідності `key -> shard_id`; максимальна гнучкість, але таблиця стає вузьким місцем.
3. **Consistent Hashing vs `hash % N`:** при `hash % N` додавання 3-го шарда до 2 змінює залишок від ділення майже для **~67%** ключів (при великому $N$ — майже 100%). У Consistent Hashing із кільцем та віртуальними вузлами при переході $2 \to 3$ шардів переїжджає лише близько **$1/3 \approx 33\%$** ключів, а vnodes забезпечують рівномірний розподіл навантаження.
4. **Як працює `HashMap`:** масив бакетів + хеш-функція `index = hash(key) % capacity`. Колізії вирішуються ланцюжками (separate chaining) або відкритою адресацією. При перевищенні `load_factor` (зазвичай ~0.75) відбувається `rehashing` — виділяється вдвічі більший масив і перераховуються позиції всіх ключів.
5. **Навіщо `partition key` і `sort key`:** `partition_key` визначає, **на який шард** потрапить запис (усі записи з однаковим `partition_key`, наприклад усі замовлення одного користувача, лежать разом). `sort_key` упорядковує записи **всередині** однієї партиції на диску (B-Tree у SQLite), що дозволяє швидко робити range-запити (останні замовлення за датою) без сканування всього кластера.
6. **Хороший ключ для шардингу та hot key:** хороший ключ має високу кардинальність (багато унікальних значень) і рівномірний розподіл запитів (`user_id`, `device_id`, UUID). Поганий ключ — наприклад, `country` або `status`. Якщо весь трафік іде на один ключ (*hot key / hot partition*), один шард перевантажується до 100% CPU/IOPS, поки решта кластера простоює (рішення: додавання випадкового суфікса / write sharding або кеш перед координатором).
