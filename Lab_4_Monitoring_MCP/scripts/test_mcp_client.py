"""Автоматична перевірка MCP-сервера через офіційний MCP Client (stdio)."""

import asyncio
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def main() -> None:
    server_script = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mcp_server", "server.py"))
    server_params = StdioServerParameters(
        command=sys.executable,
        args=[server_script, "--transport", "stdio"],
        env={**os.environ, "COORDINATOR_URL": os.getenv("COORDINATOR_URL", "http://localhost:8000")},
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            print("MCP Tools:", [t.name for t in tools.tools])

            res_create = await session.call_tool(
                "CREATE",
                arguments={"params": {"table": "mcp_demo", "partition_key": "item-1", "value": {"msg": "hello from MCP"}}},
            )
            print("CREATE:", res_create.content[0].text)

            res_exists = await session.call_tool(
                "EXISTS",
                arguments={"params": {"table": "mcp_demo", "partition_key": "item-1"}},
            )
            print("EXISTS:", res_exists.content[0].text)

            res_read = await session.call_tool(
                "READ",
                arguments={"params": {"table": "mcp_demo", "partition_key": "item-1"}},
            )
            print("READ:", res_read.content[0].text)

            res_del = await session.call_tool(
                "DELETE",
                arguments={"params": {"table": "mcp_demo", "partition_key": "item-1"}},
            )
            print("DELETE:", res_del.content[0].text)


if __name__ == "__main__":
    asyncio.run(main())
