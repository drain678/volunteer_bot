import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from consumer.api.router import router
from consumer.app import main


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:

    task = asyncio.create_task(main())

    yield
    task.cancel()


def create_app() -> FastAPI:
    app = FastAPI(docs_url="/swagger", lifespan=lifespan)
    app.include_router(router)
    return app
