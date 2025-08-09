# main.py
import asyncio
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from api.handlers import router as api_router
from manager.task_manager import task_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # == Startup ==
    # Здесь можно было бы прогреть что-то, проверить конфиг и т.п.
    yield
    # == Shutdown ==
    # Останавливаем все таски аккуратно (аналог graceful shutdown в Go)
    await task_manager.stop_all()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Twitch Viewers API v1",
        docs_url="/api/swagger",       # как в Go
        redoc_url=None,
        openapi_url="/api/swagger.json",
        lifespan=lifespan,
    )

    # CORS (как в Go: cfg.CORS.AllowOrigins/AllowMethods)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors.allowed_origins or ["*"],
        allow_methods=settings.cors.allowed_methods or ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )

    # Подключаем наши эндпоинты
    app.include_router(api_router)
    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(settings.server.port))
