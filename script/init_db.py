import asyncio

from alembic import command
from alembic.config import Config


async def migrate(action: str = "upgrade", revision: str = "head") -> None:
    alembic_cfg = Config("alembic.ini")

    if action == "upgrade":
        await asyncio.to_thread(command.upgrade, alembic_cfg, revision)
    elif action == "downgrade":
        await asyncio.to_thread(command.downgrade, alembic_cfg, revision)
