"""MCP-сервер поверх розподіленого key-value сховища (Мілстоун 4).

Експонує інструменти EXISTS, CREATE, READ, DELETE (та безпечні read-only довідники)
через офіційний MCP SDK (FastMCP), підтримує stdio та streamable-http/sse.
Адмін-інструменти (ребалансування, видалення таблиць/шардів) свідомо НЕ відкриті агенту.
"""

import argparse
import json
import os
from typing import Any

import httpx
from pydantic import BaseModel, Field

try:
    from mcp.server.mcpserver import MCPServer

    mcp = MCPServer("kpi-kv-store-mcp")
    _IS_MCP_V2 = True
except ImportError:
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("kpi-kv-store-mcp", host="0.0.0.0", port=int(os.getenv("MCP_PORT", "8020")))
    _IS_MCP_V2 = False

COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8000").rstrip("/")


class ExistsInput(BaseModel):
    table: str = Field(..., description="Назва таблиці в KV-сховищі")
    partition_key: str = Field(..., description="Основний partition key запису")
    sort_key: str | None = Field(default=None, description="Опціональний sort key")


class CreateInput(BaseModel):
    table: str = Field(..., description="Назва таблиці (буде створена автоматично, якщо не існує)")
    partition_key: str = Field(..., description="Основний partition key запису")
    value: Any = Field(..., description="JSON-значення запису")
    sort_key: str | None = Field(default=None, description="Опціональний sort key")


class ReadInput(BaseModel):
    table: str = Field(..., description="Назва таблиці")
    partition_key: str = Field(..., description="Основний partition key")
    sort_key: str | None = Field(default=None, description="Опціональний sort key (без нього повернуться всі записи партиції)")


class DeleteInput(BaseModel):
    table: str = Field(..., description="Назва таблиці")
    partition_key: str = Field(..., description="Основний partition key")
    sort_key: str | None = Field(default=None, description="Опціональний sort key")


@mcp.tool(name="EXISTS", description="Перевірити, чи існує запис за ключем у розподіленому KV-сховищі")
async def kv_exists(params: ExistsInput) -> str:
    q: dict[str, str] = {}
    if params.sort_key is not None:
        q["sort_key"] = params.sort_key
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(
            f"{COORDINATOR_URL}/tables/{params.table}/records/{params.partition_key}/exists",
            params=q,
        )
        return json.dumps({"status": resp.status_code, "data": resp.json()}, ensure_ascii=False)


@mcp.tool(name="CREATE", description="Зберегти або оновити пару ключ-значення (LWW) у KV-сховищі")
async def kv_create(params: CreateInput) -> str:
    async with httpx.AsyncClient(timeout=5.0) as client:
        # Автоматично реєструємо таблицю, якщо її ще немає
        table_check = await client.get(f"{COORDINATOR_URL}/tables")
        existing_names = {t["name"] for t in table_check.json().get("tables", [])}
        if params.table not in existing_names:
            table_def: dict[str, Any] = {"name": params.table, "partition_key": "partition_key"}
            if params.sort_key is not None:
                table_def["sort_key"] = "sort_key"
            await client.post(f"{COORDINATOR_URL}/tables", json=table_def)

        resp = await client.post(
            f"{COORDINATOR_URL}/tables/{params.table}/records",
            json={
                "partition_key": params.partition_key,
                "sort_key": params.sort_key,
                "value": params.value,
            },
        )
        return json.dumps({"status": resp.status_code, "data": resp.json()}, ensure_ascii=False)


@mcp.tool(name="READ", description="Прочитати запис або всі записи партиції з KV-сховища")
async def kv_read(params: ReadInput) -> str:
    q: dict[str, str] = {}
    if params.sort_key is not None:
        q["sort_key"] = params.sort_key
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(
            f"{COORDINATOR_URL}/tables/{params.table}/records/{params.partition_key}",
            params=q,
        )
        return json.dumps({"status": resp.status_code, "data": resp.json()}, ensure_ascii=False)


@mcp.tool(name="DELETE", description="Видалити запис за ключем із KV-сховища (створює LWW tombstone)")
async def kv_delete(params: DeleteInput) -> str:
    q: dict[str, str] = {}
    if params.sort_key is not None:
        q["sort_key"] = params.sort_key
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.delete(
            f"{COORDINATOR_URL}/tables/{params.table}/records/{params.partition_key}",
            params=q,
        )
        return json.dumps({"status": resp.status_code, "data": resp.json()}, ensure_ascii=False)


@mcp.resource("kv://cluster/health")
async def cluster_health_resource() -> str:
    """Повертає поточний стан кластера та реплік (read-only ресурс)."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(f"{COORDINATOR_URL}/shards")
        return json.dumps(resp.json(), ensure_ascii=False, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KPI KV Store MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default=os.getenv("MCP_TRANSPORT", "stdio"),
    )
    args = parser.parse_args()
    if _IS_MCP_V2 and args.transport != "stdio":
        mcp.run(transport=args.transport, host="0.0.0.0", port=int(os.getenv("MCP_PORT", "8020")))
    else:
        mcp.run(transport=args.transport)
