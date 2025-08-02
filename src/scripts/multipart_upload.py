import asyncio
import json
import math
from pathlib import Path

import httpx

from client.s3py_client import Client
from client.s3py_client.errors import UnexpectedStatus
from client.s3py_client.models import (
    StartUploadRequest,
    UploadPartRequest,
    CompleteUploadRequest,
    StartUploadResponse,
    PresignedUrlResponse,
    UploadPartResponse,
    CompleteUploadResponse,
    UploadStatus,
)
from client.s3py_client.api.files import (
    get_uploads_api_v1_uploads_get,
    get_last_part_api_v1_uploads_upload_id_last_part_get,
    start_upload_api_v1_start_upload_post,
    get_presigned_url_api_v1_presigned_url_get,
    upload_part_api_v1_upload_part_post,
    complete_upload_api_v1_complete_upload_post,
)

CHUNK_SIZE = 50 * 1024 * 1024  # 5MB
MAX_CONCURRENT_UPLOADS = 5  # Limit concurrent uploads


class ChunkedBinaryReader:
    """
    Read binary files in chunks.

    Example:
        with ChunkedBinaryReader(filename, CHUNK_SIZE) as reader:
            for index, chunk in enumerate(reader.read_chunks()):
                print(f"Chunk {index}: {chunk}")
    """

    def __init__(self, filename: str | Path, chunk_size: int = CHUNK_SIZE):
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
            raise RuntimeError(
                "File not opened. Use this class with a 'with' statement."
            )

        while True:
            chunk = self.file.read(self.chunk_size)
            if not chunk:
                break
            yield chunk

    def read_chunks_batch(self, offset: int = 0, batch_size: int = 1) -> list[bytes]:
        if self.file is None:
            raise RuntimeError(
                "File not opened. Use this class with a 'with' statement."
            )

        # Calculate byte offset and seek to position
        byte_offset = offset * self.chunk_size
        self.file.seek(byte_offset)

        chunks = []
        for _ in range(batch_size):
            chunk = self.file.read(self.chunk_size)
            if not chunk:
                break
            chunks.append(chunk)

        return chunks


async def start_upload(client: Client) -> StartUploadResponse:
    response = await start_upload_api_v1_start_upload_post.asyncio(
        client=client,
        body=StartUploadRequest(
            filename="file_big.bin",
            content_type="application/octet-stream",
            user_id="minio_user",
        ),
    )
    return response


async def get_presigned_url(
    client: Client, upload_id: str, key: str, part_number: int
) -> PresignedUrlResponse:
    response = await get_presigned_url_api_v1_presigned_url_get.asyncio(
        client=client, upload_id=upload_id, key=key, part_number=part_number
    )
    return response


async def upload_to_presigned_url(presigned_url: str, chunk: bytes) -> httpx.Response:
    async with httpx.AsyncClient() as client:
        response = await client.put(url=presigned_url, content=chunk, timeout=None)

    return response


async def upload_part(
    client: Client, upload_id: str, key: str, part_number: int, etag: str, user_id: str
) -> UploadPartResponse:
    request = UploadPartRequest(
        upload_id=upload_id,
        key=key,
        part_number=part_number,
        etag=etag,
        user_id=user_id,
    )
    response = await upload_part_api_v1_upload_part_post.asyncio(
        client=client, body=request
    )
    return response


async def complete_upload(
    client: Client, upload_id: str, key: str, user_id: str
) -> CompleteUploadResponse:
    request = CompleteUploadRequest(upload_id=upload_id, key=key, user_id=user_id)
    response = await complete_upload_api_v1_complete_upload_post.asyncio(
        client=client, body=request
    )
    return response


async def main():
    # TODO: setup argparse
    filename_to_upload = "file.bin"
    file_to_upload = Path(filename_to_upload)
    file_size = file_to_upload.stat().st_size
    user_id = "minio_user"

    num_chunks_estimated = math.ceil(file_size / CHUNK_SIZE)
    print(
        f"file_size: {file_size >> 20} MB num_chunks (estimated): {num_chunks_estimated} chunk_size = {CHUNK_SIZE >> 20} MB"
    )

    async with Client(
        base_url="http://localhost:8000", raise_on_unexpected_status=True
    ) as client:
        # First check if there's an upload for that file that is not completed
        # in the database. If there are multiple, use the one with the most recent
        # created_at field and delete the others.
        last_part_number = 0

        in_progress_uploads = await get_uploads_api_v1_uploads_get.asyncio(
            client=client,
            key=filename_to_upload,
            upload_status=[UploadStatus.INITIATED, UploadStatus.IN_PROGRESS],
        )
        if in_progress_uploads:
            upload = in_progress_uploads[0]
            try:
                last_part = (
                    await get_last_part_api_v1_uploads_upload_id_last_part_get.asyncio(
                        client=client, upload_id=upload.upload_id
                    )
                )
                last_part_number = last_part.part_number
            except UnexpectedStatus as e:
                msg = json.loads(e.content.decode())["detail"]
                print(f"Error {e.status_code} : {msg}")
        else:
            upload = await start_upload(client)

        for batch_idx in range(0, num_chunks_estimated, MAX_CONCURRENT_UPLOADS):
            print(f"Processing batch {batch_idx // MAX_CONCURRENT_UPLOADS + 1}")
            with ChunkedBinaryReader(file_to_upload, CHUNK_SIZE) as reader:
                print("Reading file...")
                batch = reader.read_chunks_batch(batch_idx, MAX_CONCURRENT_UPLOADS)
                print(f"Read batch with size ({len(batch)} MB)")

            async with asyncio.TaskGroup() as tg:
                tasks = []
                for part_idx, chunk in enumerate(batch, start=batch_idx + 1):
                    if part_idx <= last_part_number:
                        print(f"Skipping already uploaded chunk {part_idx}")
                        continue

                    response = await get_presigned_url(
                        client, upload.upload_id, upload.key, part_idx
                    )
                    print(
                        f"Chunk {part_idx} presigned URL generated: {response.presigned_url}"
                    )

                    task = tg.create_task(
                        upload_to_presigned_url(response.presigned_url, chunk)
                    )
                    tasks.append(task)

                upload_responses = [await task for task in tasks]

            for index, upload_response in enumerate(
                upload_responses, start=batch_idx + last_part_number + 1
            ):
                tag_from_header = upload_response.headers.get("ETag")
                if tag_from_header is None:
                    raise ValueError(
                        f"ETag was not found in header, please delete upload with id '{upload.upload_id}' and restart"
                    )
                print(f"Chunk {index} ETag: {tag_from_header}")
                result = await upload_part(
                    client=client,
                    upload_id=upload.upload_id,
                    key=upload.key,
                    part_number=index,
                    etag=tag_from_header,
                    user_id=user_id,
                )

                if not result.success:
                    raise ValueError(f"Error registering uploaded part {index}")

        response = await complete_upload(client, upload.upload_id, upload.key, user_id)
        print(f"{response.message} : {response.location}")


if __name__ == "__main__":
    asyncio.run(main())
