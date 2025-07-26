import asyncio
import os
import random
import tempfile
import math
from pathlib import Path

import boto3
import httpx
from httpx import Response
from pydantic import BaseModel
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    ForeignKey,
    select,
)
from sqlalchemy.orm import sessionmaker, relationship, Mapped, declarative_base

S3_BUCKET_NAME = "python-test-bucket"
CHUNK_SIZE = 5 * 1024 * 1024  # 10MB


Base = declarative_base()


s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minio_user",
    aws_secret_access_key="minioadmin123",
)


class Upload(Base):
    __tablename__ = "upload"

    upload_id = Column(String(100), primary_key=True)
    user_id = Column(String(100))
    key = Column(String(100), nullable=False)
    status = Column(String(100), nullable=False)
    parts: Mapped[list["Part"]] = relationship(
        "Part", back_populates="upload", cascade="all, delete-orphan"
    )


class Part(Base):
    __tablename__ = "part"

    id = Column(Integer, primary_key=True, autoincrement=True)
    upload_id = Column(String(100), ForeignKey("upload.upload_id"), nullable=False)
    user_id = Column(String(100))
    key = Column(String(100), nullable=False)
    etag = Column(String(100), nullable=False)
    part_number = Column(Integer)
    upload: Mapped["Upload"] = relationship(back_populates="parts")


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


class CompleteUploadRequest(BaseModel):
    upload_id: str
    key: str
    user_id: str


engine = create_engine(
    "sqlite:////home/benjamin/projects/s3-file-upload/minio-upload-file/upload.db",
    echo=False,
)  # echo=True for SQL logging

# Create tables
Base.metadata.create_all(engine)

# Create session
Session = sessionmaker(bind=engine)
session = Session()


def start_upload(request: StartUploadRequest) -> dict[str, str]:
    """
    Start a new multipart upload process:

    - Creates a multipart upload in S3
    - Records the upload session in the database
    - Returns the upload ID and key for later operations

    The client should use this endpoint before uploading any file parts.
    """
    response = s3_client.create_multipart_upload(
        Bucket=S3_BUCKET_NAME, Key=request.filename, ContentType=request.content_type
    )

    session.add(
        Upload(
            user_id=request.user_id,
            key=response["Key"],
            upload_id=response["UploadId"],
            parts=[],
            status="initiated",
        )
    )
    session.commit()

    return {"upload_id": response["UploadId"], "key": response["Key"]}


def get_signed_url(upload_id: str, key: str, part_number: int) -> dict[str, str]:
    """
    Generate a pre-signed URL for uploading a specific part:

    - Creates a temporary URL valid for 1 hour
    - Client can use this URL to upload the part directly to S3
    - Part number must be between 1 and 10,000

    The client should request a new URL for each part they need to upload.
    """
    signed_url = s3_client.generate_presigned_url(
        "upload_part",
        Params={
            "Bucket": S3_BUCKET_NAME,
            "Key": key,
            "PartNumber": part_number,
            "UploadId": upload_id,
        },
        ExpiresIn=3600,
    )

    return {"signed_url": signed_url}


def upload_part(request: UploadPartRequest) -> dict[str, bool]:
    """
    Record a part that was successfully uploaded to S3:

    - Updates the database with the ETag returned from S3
    - Tracks the part number for later assembly
    - Sets the upload status to "in-progress"

    The client should call this after successfully uploading a part using the pre-signed URL.
    """
    result = session.execute(
        select(Upload).where(
            Upload.upload_id == request.upload_id,
            Upload.key == request.key,
            Upload.user_id == request.user_id,
        )
    ).one_or_none()

    if result is not None:
        upload = result[0]
        part = Part(
            upload_id=request.upload_id,
            user_id=request.user_id,
            key=request.key,
            etag=request.etag,
            part_number=request.part_number,
            upload=upload,
        )
        session.add(part)
        session.commit()

        upload.status = "in-progress"
        upload.parts.append(part)
        session.commit()
        # session.execute(update(Upload, values={upload.status: "in-progress",
        #                                        }))
    # else: 404 error

    return {"success": True}


def complete_upload(request: CompleteUploadRequest) -> dict[str, str]:
    result = session.execute(
        select(Upload).where(
            Upload.upload_id == request.upload_id,
            Upload.key == request.key,
            Upload.user_id == request.user_id,
        )
    ).one_or_none()

    if result is None:
        raise ValueError("Upload session not found or no parts uploaded")
        # raise HTTPException(status_code=404,
        #                     detail="Upload session not found or no parts uploaded")

    upload = result[0]
    sorted_parts = sorted(upload.parts, key=lambda x: x.part_number)

    print(f"Total parts to complete: {len(sorted_parts)}")

    # Validate parts before attempting to complete
    try:
        parts_response = s3_client.list_parts(
            Bucket=S3_BUCKET_NAME, Key=request.key, UploadId=request.upload_id
        )

        s3_parts = {
            part["PartNumber"]: part for part in parts_response.get("Parts", []) # type: ignore
        }
        print(f"Parts found in S3: {len(s3_parts)}")

        # Validate each part
        for part in sorted_parts:
            if part.part_number not in s3_parts:
                raise ValueError(f"Part {part.part_number} not found in S3")

            s3_part = s3_parts[part.part_number]
            part_size = s3_part["Size"] # type: ignore

            # Check minimum size requirement (5MB except for last part)
            min_size = 5 * 1024 * 1024  # 5MB
            is_last_part = part.part_number == max(p.part_number for p in sorted_parts)

            if part_size < min_size and not is_last_part:
                raise ValueError(
                    f"Part {part.part_number} is too small ({part_size} bytes). "
                    f"Minimum size is {min_size} bytes except for the last part."
                )

            print(f"Part {part.part_number}: {part_size} bytes, ETag={part.etag}")

        payload = {
            "Bucket": S3_BUCKET_NAME,
            "Key": request.key,
            "UploadId": request.upload_id,
            "MultipartUpload": {
                "Parts": [
                    {"ETag": part.etag, "PartNumber": part.part_number}
                    for part in sorted_parts
                ]
            },
        }
        response = s3_client.complete_multipart_upload(**payload)

        upload.upload_id = request.upload_id
        upload.status = "completed"
        session.commit()

        return {
            "message": "Upload completed successfully",
            "location": response["Location"],
            "key": response["Key"],
        }
    except Exception as e:
        print(f"Error completing multipart upload: {e}")
        try:
            s3_client.abort_multipart_upload(
                Bucket=S3_BUCKET_NAME, Key=request.key, UploadId=request.upload_id
            )
            print("Multipart upload aborted")
        except Exception as abort_error:
            print(f"Error aborting upload: {abort_error}")
        raise


class ChunkedBinaryReader:
    def __init__(self, filename, chunk_size=CHUNK_SIZE):
        self.filename = filename
        self.chunk_size = chunk_size
        self.file = None

    def __enter__(self):
        self.file = open(self.filename, "rb")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()

    def read_chunks(self):
        if self.file is None:
            raise RuntimeError("File not opened. Use this class with a 'with' statement.")

        while True:
            chunk = self.file.read(self.chunk_size)
            if not chunk:
                break
            yield chunk


def generate_random_file():
    size_mb = 1024
    extension = "bin"

    # Calculate the file size in bytes
    file_size = size_mb * 1024 * 1024

    # Generate a chunk of random bytes to write to the file
    chunk_size = 1024 * 1024
    chunk = bytearray(random.getrandbits(8) for _ in range(chunk_size))

    # Create a temporary directory and file path
    temp_dir = tempfile.mkdtemp()
    file_path = os.path.join(temp_dir, "random_file.bin")

    # Write the random data to the file until it reaches the desired size
    with open(file_path, "wb") as f:
        while os.path.getsize(file_path) < file_size:
            f.write(chunk)

    # Move the file to the desired location with the specified extension
    final_path = f"./file.{extension}"
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    os.replace(file_path, final_path)


def sync_upload():
    if not Path("file.bin").exists():
        generate_random_file()

    file_to_upload = Path("file.bin")
    file_size = file_to_upload.stat().st_size
    num_chunks = math.ceil(file_size / CHUNK_SIZE)

    upload_request = StartUploadRequest(
        filename="file.bin", content_type="application/octet-stream", user_id="user123"
    )
    start_upload_response = start_upload(upload_request)

    with ChunkedBinaryReader(file_to_upload, CHUNK_SIZE) as reader:
        for index, chunk in enumerate(reader.read_chunks(), start=1):
            # print(f"chunk_size={len(chunk)}")
            # print(f"index={index}/{num_chunks}")
            signed_url = get_signed_url(
                start_upload_response["upload_id"], start_upload_response["key"], index
            )

            # print(f"signed_url={signed_url}")

            # async with httpx.AsyncClient() as client:
            #     response = await client.put(url=signed_url["signed_url"], content=chunk, timeout=None)

            with httpx.Client() as client:
                response = client.put(
                    url=signed_url["signed_url"], content=chunk, timeout=None
                )

            if response.status_code != httpx.codes.OK:
                response.raise_for_status()

            etag = response.headers["etag"]
            # print(f"etag={etag}")

            # Notify backend
            result = upload_part(
                UploadPartRequest(
                    upload_id=start_upload_response["upload_id"],
                    key=start_upload_response["key"],
                    part_number=index,
                    etag=etag,
                    user_id="user123",
                )
            )

            progress = round((index / num_chunks) * 100)
            print(f"Uploading...{progress}%")

    response = complete_upload(
        CompleteUploadRequest(
            upload_id=start_upload_response["upload_id"],
            key=start_upload_response["key"],
            user_id="user123",
        )
    )

    print(response)


async def upload_to_presigned_url(presigned_url: str,
                                  chunk: bytes) -> Response:
    async with httpx.AsyncClient() as client:
        response = await client.put(url=presigned_url, content=chunk, timeout=None)

    return response


async def async_upload():
    if not Path("file.bin").exists():
        generate_random_file()

    file_to_upload = Path("file.bin")
    file_size = file_to_upload.stat().st_size
    num_chunks = math.ceil(file_size / CHUNK_SIZE)

    upload_request = StartUploadRequest(
        filename="file.bin", content_type="application/octet-stream", user_id="user123"
    )
    start_upload_response = start_upload(upload_request)

    async with asyncio.TaskGroup() as tg:
        tasks = []

        with ChunkedBinaryReader(file_to_upload, CHUNK_SIZE) as reader:
            for index, chunk in enumerate(reader.read_chunks(), start=1):
                signed_url = get_signed_url(
                    start_upload_response["upload_id"], start_upload_response["key"], index
                )

                presigned_url = signed_url["signed_url"]
                print(f"Chunk {index} presigned URL generated: {presigned_url}")

                task = tg.create_task(
                    upload_to_presigned_url(presigned_url, chunk)
                )
                tasks.append(task)

        upload_results = [await task for task in tasks]

    etags = []
    for index, httpx_upload_response in enumerate(upload_results, start=1):
        tag_from_header = httpx_upload_response.headers.get("ETag")
        etags.append({"ETag": tag_from_header, "PartNumber": index})
        print(f"Chunk {index} ETag: {tag_from_header}")
        result = upload_part(
            UploadPartRequest(
                upload_id=start_upload_response["upload_id"],
                key=start_upload_response["key"],
                part_number=index,
                etag=tag_from_header,
                user_id="user123",
            )
        )

    response = complete_upload(
        CompleteUploadRequest(
            upload_id=start_upload_response["upload_id"],
            key=start_upload_response["key"],
            user_id="user123",
        )
    )

    print(response)


if __name__ == "__main__":
    asyncio.run(async_upload())
    # sync_upload()
