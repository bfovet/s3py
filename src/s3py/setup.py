from collections.abc import Callable
from contextlib import _AsyncGeneratorContextManager, asynccontextmanager
from typing import Any, AsyncGenerator

import anyio.to_thread
from fastapi import FastAPI, APIRouter

from s3py.database import engine, Base


# database
async def create_tables() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# application
async def set_threadpool_tokens(num_tokens: int = 100) -> None:
    limiter = anyio.to_thread.current_default_thread_limiter()
    limiter.total_tokens = num_tokens


def lifespan_factory(
    create_tables_on_start: bool = True,
) -> Callable[[FastAPI], _AsyncGeneratorContextManager[Any]]:
    """Factory to create a lifespan async context manager for a FastAPI app."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator:
        from asyncio import Event

        initialization_complete = Event()
        # FIXME: https://github.com/Kludex/fastapi-tips?tab=readme-ov-file#6-use-lifespan-state-instead-of-appstate
        # app.state.initialization_complete = initialization_complete

        await set_threadpool_tokens()

        try:
            if create_tables_on_start:
                await create_tables()

            initialization_complete.set()

            yield {"initialization_complete": initialization_complete}
        finally:
            pass

    return lifespan


# application
def create_application(
    router: APIRouter,
    create_tables_on_start: bool = True,
    lifespan: Callable[[FastAPI], _AsyncGeneratorContextManager[Any]] | None = None,
    **kwargs: Any,
) -> FastAPI:
    """Creates and configures a FastAPI application based on the provided settings.

    This function initializes a FastAPI application and configures it with various settings
    and handlers based on the type of the `settings` object provided.

    Parameters
    ----------
    router : APIRouter
        The APIRouter object containing the routes to be included in the FastAPI application.

    create_tables_on_start : bool
        A flag to indicate whether to create database tables on application startup.
        Defaults to True.

    **kwargs
        Additional keyword arguments passed directly to the FastAPI constructor.

    Returns
    -------
    FastAPI
        A fully configured FastAPI application instance.

    The function configures the FastAPI application with different features and behaviors
    based on the provided settings. It includes setting up database connections, and
    customizing the API documentation based on the environment settings.
    """
    # before creating the application
    to_update = {"title": "FastAPI app"}
    kwargs.update(to_update)

    # Use custom lifespan if provided, otherwise use default factory
    if lifespan is None:
        lifespan = lifespan_factory(create_tables_on_start=create_tables_on_start)

    application = FastAPI(lifespan=lifespan, **kwargs)
    application.include_router(router)

    return application
