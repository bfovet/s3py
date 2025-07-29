import asyncio
from pathlib import Path

import httpx

from client.s3py_client import Client
from client.s3py_client.models import (
    StartUploadRequest,
    UploadPartRequest,
    CompleteUploadRequest,
    StartUploadResponse,
    PresignedUrlResponse,
    UploadPartResponse,
    CompleteUploadResponse,
)
from client.s3py_client.api.files import (
    start_upload_api_v1_start_upload_post,
    get_presigned_url_api_v1_presigned_url_get,
    upload_part_api_v1_upload_part_post,
    complete_upload_api_v1_complete_upload_post,
)

CHUNK_SIZE = 5 * 1024 * 1024  # 5MB


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


async def start_upload(client: Client) -> StartUploadResponse:
    response = await start_upload_api_v1_start_upload_post.asyncio(
        client=client,
        body=StartUploadRequest(
            filename="file.bin",
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
    file_to_upload = Path("file.bin")
    user_id = "minio_user"

    async with Client(base_url="http://localhost:8000") as client:
        upload = await start_upload(client)

        async with asyncio.TaskGroup() as tg:
            tasks = []

            with ChunkedBinaryReader(file_to_upload, CHUNK_SIZE) as reader:
                for index, chunk in enumerate(reader.read_chunks(), start=1):
                    response = await get_presigned_url(
                        client, upload.upload_id, upload.key, index
                    )
                    print(
                        f"Chunk {index} presigned URL generated: {response.presigned_url}"
                    )

                    task = tg.create_task(
                        upload_to_presigned_url(response.presigned_url, chunk)
                    )
                    tasks.append(task)

            upload_results = [await task for task in tasks]

        for index, httpx_upload_response in enumerate(upload_results, start=1):
            tag_from_header = httpx_upload_response.headers.get("ETag")
            print(f"Chunk {index} ETag: {tag_from_header}")
            result = await upload_part(
                client=client,
                upload_id=upload.upload_id,
                key=upload.key,
                part_number=index,
                etag=tag_from_header,
                user_id=user_id,
            )

            # TODO: check result
            if not result.success:
                pass

        response = await complete_upload(client, upload.upload_id, upload.key, user_id)
        print(f"{response.message} : {response.location}")


if __name__ == "__main__":
    asyncio.run(main())
