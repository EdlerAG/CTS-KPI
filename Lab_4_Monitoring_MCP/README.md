# Лабораторна 4 (Мілстоун 4): Моніторинг, відмови та MCP

**Мета:** зробити кластер повністю спостережуваним (Prometheus, Grafana, структуровані JSON-логи з `trace_id`, алерти, `/health`, `/version`), розвʼязувати конфлікти через версіонування + Last-Write-Wins (LWW), реалізувати фонове ребалансування та відкрити сховище AI-агентам через MCP.

**Статус:** реалізовано повний максимум (**15/15 балів**: 10 базових + 5 розширених).

---

## Архітектура та файли
- [coordinator/app.py](file:///D:/Потрібне/5%20курс/ХТС/Lab_4_Monitoring_MCP/coordinator/app.py) — Prometheus `/metrics`, JSON-логи з `X-Trace-Id`, `/health`, `/version`, LWW-кворуми й фонове ребалансування (`/admin/rebalance`).
- [shard/app.py](file:///D:/Потрібне/5%20курс/ХТС/Lab_4_Monitoring_MCP/shard/app.py) та [shard/storage.py](file:///D:/Потрібне/5%20курс/ХТС/Lab_4_Monitoring_MCP/shard/storage.py) — LWW за `(version_ts_ns, node_id)` + tombstones для `DELETE`, метрики `kv_shard_keys_count`, `kv_replication_lag_offsets`, `kv_replication_lag_seconds`, `/health`, `/version`.
- [monitoring/prometheus/prometheus.yml](file:///D:/Потрібне/5%20курс/ХТС/Lab_4_Monitoring_MCP/monitoring/prometheus/prometheus.yml) та [alerts.yml](file:///D:/Потрібне/5%20курс/ХТС/Lab_4_Monitoring_MCP/monitoring/prometheus/alerts.yml) — збір метрик та 3 правила алертів (`NodeDown`, `HighP99Latency`, `HighReplicationLag`).
- [monitoring/grafana/dashboards/kv_cluster.json](file:///D:/Потрібне/5%20курс/ХТС/Lab_4_Monitoring_MCP/monitoring/grafana/dashboards/kv_cluster.json) — авто-підключений дашборд у Grafana (`http://localhost:3000`, логін/пароль `admin` / `admin`).
- [mcp_server/server.py](file:///D:/Потрібне/5%20курс/ХТС/Lab_4_Monitoring_MCP/mcp_server/server.py) — MCP-сервер (`EXISTS`, `CREATE`, `READ`, `DELETE` + read-only ресурс `kv://cluster/health`), підтримує `stdio` і `streamable-http` (`http://localhost:8020/mcp`).
- [docker-compose.yml](file:///D:/Потрібне/5%20курс/ХТС/Lab_4_Monitoring_MCP/docker-compose.yml) — повний стек (WAL, координатор, 6 реплік, Prometheus, Grafana, MCP-сервер + профіль `scale` для `shard-3`).

---

## Запуск і демонстрація на захисті

### 1. Запуск усього стека
```powershell
cd "D:\Потрібне\5 курс\ХТС\Lab_4_Monitoring_MCP"
docker compose up -d --build
```
- **API Координатора / Swagger:** `http://localhost:8000/docs`
- **Health та Version:** `http://localhost:8000/health` та `http://localhost:8000/version`
- **Prometheus + Алерти:** `http://localhost:9090/alerts`
- **Grafana Дашборд:** `http://localhost:3000` (`admin` / `admin`) → Dashboards → `KV Store Cluster Dashboard (Lab 4)`
- **MCP Server (Streamable HTTP):** `http://localhost:8020/mcp`

### 2. Наповнити графіки трафіком і перевірити `trace_id`
```powershell
python scripts/generate_traffic.py
# Знайти X-Trace-Id із відповіді та прослідкувати його через усі контейнери:
docker compose logs | Select-String "trace-id-з-відповіді"
```

### 3. Перевірка MCP-сервера через MCP-клієнт
Конфіг для підключення в Claude Desktop / Claude Code / Q CLI / Antigravity (`stdio`):
```json
{
  "mcpServers": {
    "kpi-kv-store": {
      "command": "python",
      "args": [
        "D:\\Потрібне\\5 курс\\ХТС\\Lab_4_Monitoring_MCP\\mcp_server\\server.py",
        "--transport",
        "stdio"
      ],
      "env": {
        "COORDINATOR_URL": "http://localhost:8000"
      }
    }
  }
}
```
Або для **n8n** (MCP Client Tool node → HTTP Streamable): `http://localhost:8020/mcp` (або `http://host.docker.internal:8020/mcp`, якщо n8n у Docker).  
Швидкий автотест живого MCP-виклику:
```powershell
python scripts/test_mcp_client.py
```

### 4. Демо алерта `NodeDown` і фонового ребалансування (`shard-3`)
```powershell
# 1) Алерт NodeDown у Prometheus:
docker compose stop shard-1-follower-2
# Через 15 секунд на http://localhost:9090/alerts загориться FIRING NodeDown
docker compose start shard-1-follower-2

# 2) Фонове ребалансування при додаванні 3-го шарда:
docker compose --profile scale up -d
Invoke-RestMethod http://localhost:8000/admin/rebalance/status
```

---

## Чекліст балів (15/15)
- [x] **Метрики (`p50`/`p95`/`p99`, RPS, error rate) (2 бали):** Prometheus `/metrics` на координаторі та всіх шардах.
- [x] **Структуровані JSON-логи з `trace_id` (1 бал):** наскрізний `X-Trace-Id` через coordinator → shard → wal-service.
- [x] **Дашборд (2 бали):** Grafana (стан кластера, лаг реплікації, розподіл ключів по шардах, p50/p95/p99).
- [x] **Версіонування + Last-Write-Wins (2 бали):** `(version_ts_ns, node_id)` + tombstones.
- [x] **MCP-сервер (`EXISTS`, `CREATE`, `READ`, `DELETE`) (3 бали):** `mcp_server/server.py`.
- [x] **Алерти (2 бали):** `monitoring/prometheus/alerts.yml` (`NodeDown`, `HighP99Latency`, `HighReplicationLag`).
- [x] **`/health` та `/version` (1 бал):** на координаторі, шардах і WAL.
- [x] **Фонове ребалансування шардів (2 бали):** автоматична міграція ключів у фоні при появі `shard-3`.

---

## Відповіді на контрольні питання до захисту
1. **Які метрики збираємо і навіщо:** метод RED (Rate — RPS для оцінки навантаження; Errors — частка `4xx`/`5xx` для детекції збоїв; Duration — гістограма затримок `p50`/`p95`/`p99`), а також кластерні метрики (`kv_node_up` — доступність вузлів, `kv_replication_lag_*` — свіжість реплік, `kv_shard_keys_count` — рівномірність шардингу).
2. **Що показує `p99`, чого не показує середнє (`avg`):** середнє приховує викиди — якщо 99% запитів ідуть за 5 мс, а 1% зависають на 5 секунд, середнє буде ~55 мс (ніби все добре), а `p99 = 5000 мс` одразу покаже, що найповільніший 1% користувачів страждає від хвостової затримки (*tail latency*).
3. **Як дізнаємося, що вузол впав, і скільки часу це займає:** через періодичний health-probe координатора кожні 2 сек (`timeout = 1.5s`) + Prometheus scrape кожні 5 сек + `for: 15s` в алерті. Тобто маршрутизатор прибирає вузол із читань за ~2–3.5 с, а критичний алерт спрацьовує через ~15–20 с (захист від коротких мережевих миготінь / false positives).
4. **Що таке Last-Write-Wins (LWW) і в чому небезпека:** при конфлікті двох конкурентних записів перемагає запис із більшим timestamp (`version_ts_ns`). Небезпека: 1) **тиха втрата даних** (один із конкурентних записів мовчки перезаписується без помилки для клієнта), 2) **розсинхронізація фізичних годинників (clock skew / NTP)** між вузлами, через що логічно пізніший запис із відсталим годинником може програти старішому.
5. **Навіщо `trace_id` у логах:** у розподіленій системі один запит клієнта проходить через координатор, лідер/фолловери й WAL-сервіс. Без спільного `trace_id` неможливо зібрати разом логи одного проблемного запиту серед тисяч паралельних рядків з різних контейнерів.
6. **Які інструменти віддали агенту через MCP, а які ні:** віддали лише базові операції над даними (`EXISTS`, `CREATE`, `READ`, `DELETE`) та read-only ресурс `kv://cluster/health`. Свідомо **не віддали** адмін-операції (`rebalance`, видалення таблиць, керування шардами/репліками) за принципом найменших привілеїв. Агент може видаляти записи через `DELETE`, що фіксується LWW-tombstone і логами з `trace_id`; для жорсткого прод-захисту це додатково обмежують ролями/підтвердженням користувача.
7. **Чим ребалансування відрізняється від автоскейлінгу:**
   - *Автоскейлінг* — це **зміна кількості обчислювальних ресурсів/вузлів** (додавання/видалення інстансів або шардів за метрикою навантаження, CPU або кількості ключів).
   - *Ребалансування* — це **переміщення вже існуючих даних (ключів)** між шардами після зміни топології (щоб новий шард реально забрав свою частку ключів зі старих шардів і розвантажив їх).
