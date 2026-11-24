"""Чатбот над даними KV-сховища через MCP-сервер з Лаби 4 (із посиланням на джерело)."""

import asyncio
import json
import os
import re
import sys
from typing import Any

import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

MCP_SERVER_SCRIPT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "Lab_4_Monitoring_MCP", "mcp_server", "server.py")
)


def _load_local_env() -> None:
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


_load_local_env()


def _format_source(table: str, key: str, data: dict[str, Any]) -> str:
    shard = data.get("shard_id", "?")
    replica = data.get("selected_replica") or data.get("leader") or "?"
    ver = data.get("version_ts_ns", "?")
    return f"[Джерело: таблиця='{table}', ключ='{key}', шард='{shard}', репліка='{replica}', версія={ver}]"


async def _call_llm_gemini(question: str, schema_hint: str) -> dict[str, Any] | None:
    """Якщо задано GEMINI_API_KEY, перетворює природне питання у структурований MCP-виклик."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return None
    prompt = (
        "Ти маршрутизатор запитів до KV-сховища. Поверни ТІЛЬКИ JSON без markdown з полями: "
        '{"tool": "READ|EXISTS|CREATE|DELETE|HEALTH", "table": "...", "partition_key": "...", "value": null}.\n'
        f"Доступні таблиці: {schema_hint}.\nЗапит: {question}"
    )
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(url, json={"contents": [{"parts": [{"text": prompt}]}]})
        if resp.status_code != 200:
            return None
        text = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
        m = re.search(r"\{.*\}", text, re.DOTALL)
        return json.loads(m.group(0)) if m else None


def _parse_intent_rule_based(question: str, known_tables: list[str]) -> dict[str, Any]:
    """Локальний парсер українських і англійських запитів без потреби в платному API-ключі."""
    q = question.strip()
    ql = q.lower()

    if any(w in ql for w in ["стан", "кластер", "репліки", "health", "shards", "шард"]):
        return {"tool": "HEALTH"}

    # Визначаємо таблицю (якщо згадана в запиті, інакше першу наявну або telemetry_demo)
    default_table = known_tables[0] if known_tables else "telemetry_demo"
    table = default_table
    m_tbl = re.search(r"(?:таблиц[яіюї]|table)\s+['\"]?([a-zA-Z0-9_-]+)['\"]?", q, re.IGNORECASE)
    if m_tbl:
        table = m_tbl.group(1)
    else:
        for t in known_tables:
            if t.lower() in ql:
                table = t
                break

    # Пошук CREATE / запису
    if any(w in ql for w in ["додай", "створи", "запиши", "онови", "create", "put"]):
        m_kv = re.search(
            r"(?:ключ|key)\s+['\"]?([a-zA-Z0-9_.-]+)['\"]?.*?(?:значенням?|value)\s+(.+?)(?:\s+(?:у|в)\s+таблиц[юі]\s+[a-zA-Z0-9_-]+|$)",
            q,
            re.IGNORECASE,
        )
        if m_kv:
            pk = m_kv.group(1)
            raw_val = m_kv.group(2).strip()
            try:
                val = json.loads(raw_val)
            except Exception:
                val = {"text": raw_val.strip("'\"")}
            return {"tool": "CREATE", "table": table, "partition_key": pk, "value": val}

    # Витягуємо ключ із фрази
    m_key = re.search(r"(?:ключ[ау]?|key|для|pro|про)\s+['\"]?([a-zA-Z0-9_.-]+)['\"]?", q, re.IGNORECASE)
    if not m_key:
        tokens = re.findall(r"[a-zA-Z0-9_.-]+", q)
        pk = tokens[-1] if tokens else "item-1"
    else:
        pk = m_key.group(1)

    if any(w in ql for w in ["видали", "прибери", "delete", "remove"]):
        return {"tool": "DELETE", "table": table, "partition_key": pk}
    if any(w in ql for w in ["існує", "перевір", "чи є", "exists"]):
        return {"tool": "EXISTS", "table": table, "partition_key": pk}
    return {"tool": "READ", "table": table, "partition_key": pk}


async def answer_question(session: ClientSession, question: str, coordinator_url: str) -> str:
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            tbl_resp = await client.get(f"{coordinator_url}/tables")
            known_tables = [t["name"] for t in tbl_resp.json().get("tables", [])]
        except Exception:
            known_tables = []

    intent = await _call_llm_gemini(question, ", ".join(known_tables) or "telemetry_demo")
    if not intent:
        intent = _parse_intent_rule_based(question, known_tables)

    tool = intent.get("tool", "READ")
    if tool == "HEALTH":
        res = await session.read_resource("kv://cluster/health")
        raw_text = res.contents[0].text
        info = json.loads(raw_text)
        shards_summary = {sid: len(reps) for sid, reps in info.get("shards", {}).items()}
        return f"Кластер активний. Шарди та кількість реплік: {shards_summary}.\n[Джерело: MCP resource 'kv://cluster/health']"

    table = intent.get("table", "telemetry_demo")
    pk = intent.get("partition_key", "item-1")

    if tool == "CREATE":
        res = await session.call_tool("CREATE", arguments={"params": {"table": table, "partition_key": pk, "value": intent.get("value")}})
        payload = json.loads(res.content[0].text)
        data = payload.get("data", {})
        return f"Запис '{pk}' збережено в таблиці '{table}'.\n{_format_source(table, pk, data)}"

    if tool == "EXISTS":
        res = await session.call_tool("EXISTS", arguments={"params": {"table": table, "partition_key": pk}})
        payload = json.loads(res.content[0].text)
        data = payload.get("data", {})
        found = "ТАК, запис існує" if data.get("exists") else "НІ, запис відсутній"
        return f"Перевірка ключа '{pk}': {found}.\n{_format_source(table, pk, data)}"

    if tool == "DELETE":
        res = await session.call_tool("DELETE", arguments={"params": {"table": table, "partition_key": pk}})
        payload = json.loads(res.content[0].text)
        data = payload.get("data", {})
        if payload.get("status") == 404:
            return f"Не вдалося видалити '{pk}': ключ не знайдено в таблиці '{table}'.\n[Джерело: таблиця='{table}', ключ='{pk}', статус=404]"
        return f"Запис '{pk}' видалено (створено LWW tombstone).\n{_format_source(table, pk, data)}"

    # READ за замовчуванням
    res = await session.call_tool("READ", arguments={"params": {"table": table, "partition_key": pk}})
    payload = json.loads(res.content[0].text)
    data = payload.get("data", {})
    if payload.get("status") == 404:
        return f"Запис для ключа '{pk}' у таблиці '{table}' не знайдено.\n[Джерело: таблиця='{table}', ключ='{pk}', статус=404]"
    items = data.get("items", [])
    return f"Дані для ключа '{pk}': {json.dumps(items, ensure_ascii=False)}\n{_format_source(table, pk, data)}"


async def run_chat(single_queries: list[str] | None = None) -> None:
    coordinator_url = os.getenv("COORDINATOR_URL", "http://localhost:8000").rstrip("/")
    server_params = StdioServerParameters(
        command=sys.executable,
        args=[MCP_SERVER_SCRIPT, "--transport", "stdio"],
        env={**os.environ, "COORDINATOR_URL": coordinator_url},
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            if single_queries:
                for q in single_queries:
                    print(f"\nКористувач: {q}")
                    ans = await answer_question(session, q, coordinator_url)
                    print(f"Чатбот: {ans}")
                return

            print("MCP Чатбот над KV-сховищем запущено (введіть 'exit' для виходу).")
            while True:
                q = input("\nВи: ").strip()
                if not q or q.lower() in ("exit", "quit", "вихід"):
                    break
                ans = await answer_question(session, q, coordinator_url)
                print(f"Бот: {ans}")


if __name__ == "__main__":
    queries = sys.argv[1:] if len(sys.argv) > 1 else None
    asyncio.run(run_chat(queries))
