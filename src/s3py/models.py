from pydantic import BaseModel
from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, relationship, mapped_column

from s3py.database import Base


class HealthCheck(BaseModel):
    """Response model to validate and return when performing a health check."""

    status: str = "OK"


class StartUploadRequest(BaseModel):
    filename: str
    content_type: str
    user_id: str


class UploadPartRequest(BaseModel):
    upload_id: str
    key: str
    part_number: int
    etag: str
    user_id: str


class Upload(Base):
    __tablename__ = "upload"

    upload_id: Mapped[str] = mapped_column(String(100), primary_key=True, index=True)
    user_id: Mapped[str] = mapped_column(String(100))
    key: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(100), nullable=False)
    parts: Mapped[list["Part"]] = relationship(
        "Part", back_populates="upload", cascade="all, delete-orphan"
    )


class Part(Base):
    __tablename__ = "part"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        nullable=False,
        unique=True,
        init=False,
    )
    upload_id: Mapped[str] = mapped_column(
        String(100), ForeignKey("upload.upload_id"), nullable=False, index=True
    )
    user_id: Mapped[str] = mapped_column(String(100))
    key: Mapped[str] = mapped_column(String(100), nullable=False)
    etag: Mapped[str] = mapped_column(String(100), nullable=False)
    part_number: Mapped[int] = mapped_column(Integer)
    upload: Mapped["Upload"] = relationship(back_populates="parts")
