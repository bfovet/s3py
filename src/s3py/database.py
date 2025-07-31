from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass


class Base(DeclarativeBase, MappedAsDataclass):
    pass


# TODO: move these to settings
DATABASE_URI = "upload.db"
DATABASE_PREFIX = "sqlite+aiosqlite:///"
DATABASE_URL = f"{DATABASE_PREFIX}{DATABASE_URI}"


engine = create_async_engine(DATABASE_URL, echo=False, future=True, connect_args={"check_same_thread": False})
local_session = async_sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with local_session() as db:
        yield db
