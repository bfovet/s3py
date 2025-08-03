import argparse
import asyncio
import json
import logging
import math
import sys
from pathlib import Path
from typing import Generator

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
MAX_CONCURRENT_UPLOADS = 10  # Limit concurrent uploads


logger = logging.getLogger(__name__)


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

    def raise_if_file_closed(self):
        if self.file is None:
            raise RuntimeError(
                "File not opened. Use this class with a 'with' statement."
            )

    def read_chunks(
        self, offset: int = 0, batch_size: int = None
    ) -> Generator[bytes, None, None]:
        self.raise_if_file_closed()

        byte_offset = offset * self.chunk_size
        self.file.seek(byte_offset)

        chunks_read = 0
        while batch_size is None or chunks_read < batch_size:
            chunk = self.file.read(self.chunk_size)
            if not chunk:
                break
            yield chunk
            chunks_read += 1


async def start_upload(client: Client, filename: str) -> StartUploadResponse:
    logger.debug(f"Starting upload for file: XXX, user_id: XXX, content_type: XXX")
    response = await start_upload_api_v1_start_upload_post.asyncio(
        client=client,
        body=StartUploadRequest(
            filename=filename,
            content_type="application/octet-stream",
            user_id="minio_user",
        ),
    )
    logger.info(f"Started new upload - ID: {response.upload_id}, Key: {response.key}")
    return response


async def get_presigned_url(
    client: Client, upload_id: str, key: str, part_number: int
) -> PresignedUrlResponse:
    logger.debug(f"Getting presigned URL for upload_id: {upload_id}, part: {part_number}")
    response = await get_presigned_url_api_v1_presigned_url_get.asyncio(
        client=client, upload_id=upload_id, key=key, part_number=part_number
    )
    return response


async def upload_to_presigned_url(presigned_url: str, chunk: bytes) -> httpx.Response:
    logger.debug(f"Uploading chunk of {len(chunk)} bytes to presigned URL")
    async with httpx.AsyncClient() as client:
        response = await client.put(url=presigned_url, content=chunk, timeout=None)
    logger.debug(f"Upload response status: {response.status_code}")
    return response


async def upload_part(
    client: Client, upload_id: str, key: str, part_number: int, etag: str, user_id: str
) -> UploadPartResponse:
    logger.debug(f"Registering part {part_number} with ETag: {etag}")
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
    logger.info(f"Completing upload - ID: {upload_id}")
    request = CompleteUploadRequest(upload_id=upload_id, key=key, user_id=user_id)
    response = await complete_upload_api_v1_complete_upload_post.asyncio(
        client=client, body=request
    )
    return response


async def get_existing_or_start_upload(
    client: Client, filename_to_upload: str
) -> tuple[StartUploadResponse, int]:
    """
    Get an existing upload session or start a new one for the specified file.

    This function checks for existing upload sessions in INITIATED or IN_PROGRESS status
    for the given filename. If found, it retrieves the last uploaded part number to enable
    resuming the upload. If no existing upload is found, it starts a new upload session.

    Args:
        client (Client): The API client instance used to make requests to the upload service.
        filename_to_upload (str): The name/key of the file to upload or resume uploading.

    Returns:
        tuple[StartUploadResponse, int]: A tuple containing:
            - StartUploadResponse: The upload session object (either existing or newly created)
            - int: The last part number that was successfully uploaded (0 for new uploads)

    Raises:
        UnexpectedStatus: May be raised by underlying API calls if there are server errors
            or authentication issues. Error details are printed to stdout when retrieving
            the last part number fails.

    Example:
        ```python
        async with httpx.AsyncClient() as client:
            upload_session, last_part = await get_existing_or_start_upload(
                client, "my_large_file.zip"
            )

            # Resume upload from part number last_part + 1
            next_part = last_part + 1
            # ... continue with upload logic
        ```

    Note:
        - Only considers uploads with status INITIATED or IN_PROGRESS
        - If multiple matching uploads exist, uses the first one returned
        - Errors when fetching the last part number are handled gracefully by printing
          the error and continuing with last_part_number = 0
        - The function assumes the existence of helper functions: start_upload(),
          get_uploads_api_v1_uploads_get.asyncio(), and
          get_last_part_api_v1_uploads_upload_id_last_part_get.asyncio()
    """

    last_part_number = 0

    logger.info(f"Checking for existing uploads for file: {filename_to_upload}")
    in_progress_uploads = await get_uploads_api_v1_uploads_get.asyncio(
        client=client,
        key=filename_to_upload,
        upload_status=[UploadStatus.INITIATED, UploadStatus.IN_PROGRESS],
    )
    if in_progress_uploads:
        upload = in_progress_uploads[0]
        logger.info(f"Found existing upload: {upload.upload_id}")
        try:
            last_part = (
                await get_last_part_api_v1_uploads_upload_id_last_part_get.asyncio(
                    client=client, upload_id=upload.upload_id
                )
            )
            last_part_number = last_part.part_number
            logger.info(f"Last uploaded part: {last_part_number}")
        except UnexpectedStatus as e:
            msg = json.loads(e.content.decode())["detail"]
            logger.error(f"Error getting last part {e.status_code}: {msg}")
    else:
        logger.info("No existing uploads found, starting new upload")
        upload = await start_upload(client, filename_to_upload)

    return upload, last_part_number


async def upload_chunks(
    client: Client,
    file_to_upload: str | Path,
    upload: StartUploadResponse,
    last_part_number: int = 0,
    chunk_size: int = CHUNK_SIZE,
    batch_idx: int = 0,
    batch_size: int = None,
) -> list[httpx.Response]:
    """Upload file chunks to cloud storage using presigned URLs with concurrent processing.

    This function reads a file in chunks, generates presigned URLs for each chunk,
    and uploads them concurrently using asyncio TaskGroup. It supports resumable
    uploads by skipping already uploaded parts and can process files in batches.

    Args:
        client (Client): Client instance for communicating with the upload service
            to generate presigned URLs.
        file_to_upload (str | Path): Path to the file that will be uploaded in chunks.
        upload (StartUploadResponse): Response from starting the multipart upload,
            containing upload_id and key for the upload session.
        last_part_number (int, optional): The highest part number that has already
            been uploaded. Parts with numbers <= this value will be skipped.
            Defaults to 0 (no parts uploaded yet).
        chunk_size (int, optional): Size in bytes for each file chunk.
            Defaults to CHUNK_SIZE constant.
        batch_idx (int, optional): Starting index for batch processing. Used to
            offset part numbering when processing file in multiple batches.
            Defaults to 0.
        batch_size (int, optional): Maximum number of chunks to process in this
            batch. If None, processes all remaining chunks. Defaults to None.

    Returns:
        list[httpx.Response]: List of HTTP responses from the chunk upload operations,
            in the order they were awaited (may differ from upload order due to
            concurrent execution).

    Note:
        - Uses asyncio.TaskGroup for concurrent uploads with automatic error handling
        - Skips chunks that have already been uploaded (resumable upload support)
        - Part numbering starts from batch_idx + 1
        - Progress information is printed for skipped chunks and presigned URL generation
        - ChunkedBinaryReader is used for efficient file reading
        - All upload tasks are executed concurrently for better performance
        - If any upload task fails, all tasks in the group are cancelled
    """
    logger.debug(f"Starting chunk upload - batch_idx: {batch_idx}, batch_size: {batch_size}")

    async with asyncio.TaskGroup() as tg:
        tasks = []
        with ChunkedBinaryReader(file_to_upload, chunk_size) as reader:
            for part_idx, chunk in enumerate(
                reader.read_chunks(batch_idx, batch_size), start=batch_idx + 1
            ):
                if part_idx <= last_part_number:
                    logger.info(f"Skipping already uploaded chunk {part_idx}")
                    continue

                response = await get_presigned_url(
                    client, upload.upload_id, upload.key, part_idx
                )

                logger.info(f"Uploading chunk {part_idx} ({len(chunk)} bytes)")
                logger.debug(f"Chunk {part_idx} presigned URL: {response.presigned_url}")

                task = tg.create_task(
                    upload_to_presigned_url(response.presigned_url, chunk)
                )
                tasks.append(task)

        upload_responses = [await task for task in tasks]

    logger.debug(f"Completed uploading {len(upload_responses)} chunks")
    return upload_responses


async def register_uploaded_parts(
    client: Client,
    user_id: str,
    upload: StartUploadResponse,
    upload_responses: list[httpx.Response],
    start_offset: int = 0,
):
    """
    Register uploaded file parts with the server using ETags from upload responses.
    This function processes a list of HTTP responses from multipart file uploads,
    extracts ETags from response headers, and registers each part with the server
    for integrity verification and upload completion.

    Args:
        client (Client): Client instance for communicating with the upload service.
        user_id (str): Unique identifier for the user performing the upload.
        upload (StartUploadResponse): Response from starting the upload, containing
            upload_id and key for the multipart upload session.
        upload_responses (list[httpx.Response]): List of HTTP responses from
            individual file part uploads, each containing an ETag header.
        start_offset (int, optional): Starting part number offset for resumable
            uploads. This is either last_part_number for simple uploads or batch_idx
            when processing uploads in batches. Defaults to 0.

    Raises:
        ValueError: If ETag header is missing from any upload response. The error
            message includes instructions to delete the upload and restart.
        ValueError: If registration of any part fails (upload_part returns success=False).
            The error message indicates which part number failed.

    Note:
        - Parts are numbered sequentially starting from (batch_idx + last_part_number + 1)
        - ETags are required for data integrity verification
        - Function supports resumable uploads through last_part_number parameter
        - Progress information is printed for each registered chunk
        - All operations are asynchronous
    """
    logger.info(f"Registering {len(upload_responses)} uploaded parts")

    for index, upload_response in enumerate(
        upload_responses, start=start_offset + 1
    ):
        tag_from_header = upload_response.headers.get("ETag")
        if tag_from_header is None:
            raise ValueError(
                f"ETag was not found in header, please delete upload with id '{upload.upload_id}' and restart"
            )
        logger.debug(f"Chunk {index} ETag: {tag_from_header}")

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

        logger.info(f"Registered part {index}")


def setup_logging(log_level: str, log_file: str = None):
    """Configure logging for the application."""
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Set up root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Clear any existing handlers
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Suppress noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def parse_arguments():
    """Parse command line arguments for the multipart upload script."""
    parser = argparse.ArgumentParser(
        description="Upload large files using multipart upload with resumable capability and batched processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s file.bin
  %(prog)s --batched --max-concurrent 5 largefile.zip
  %(prog)s --user-id myuser --chunk-size 104857600 video.mp4
  %(prog)s --base-url https://api.example.com --content-type application/zip archive.zip
  %(prog)s --quiet --force-restart --batched stuck_upload.bin
        """,
    )

    # Required arguments
    parser.add_argument("file", type=Path, help="Path to the file to upload")

    # Server configuration
    parser.add_argument(
        "--base-url",
        default="https://localhost:8000",
        help=f"Base URL of the upload service (default: https://localhost:8000)",
    )

    parser.add_argument(
        "--user-id",
        default="minio_user",
        help=f"User ID for the upload (default: minio_user)",
    )

    # Upload configuration
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE,
        help=f"Size of each chunk in bytes (default: {CHUNK_SIZE} = 50MB)",
    )

    parser.add_argument(
        "--content-type",
        default="application/octet-stream",
        help="MIME type of the file being uploaded (default: application/octet-stream)",
    )

    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=MAX_CONCURRENT_UPLOADS,
        help=f"Maximum number of concurrent uploads per batch (default: {MAX_CONCURRENT_UPLOADS})",
    )

    # Upload behavior
    parser.add_argument(
        "--batched",
        action="store_true",
        help="Use batched upload mode for better memory efficiency with large files"
    )

    # Logging configuration
    parser.add_argument(
        "--log-level",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help="Set the logging level (default: INFO)"
    )

    return parser.parse_args()


async def upload_simple(args, client, upload, last_part_number):
    """Simple upload mode - uploads all chunks at once."""
    logger.info("Using simple upload mode")
    upload_responses = await upload_chunks(
        client, args.file, upload, last_part_number, CHUNK_SIZE
    )
    await register_uploaded_parts(
        client, "minio_user", upload, upload_responses, last_part_number
    )


async def upload_batched(args, client, upload, last_part_number):
    """Batched upload mode - processes file in chunks to limit concurrent uploads."""
    file_size = args.file.stat().st_size
    num_chunks = math.ceil(file_size / CHUNK_SIZE)

    logger.info("Using batched upload mode")
    logger.info(
        f"File size: {file_size >> 20}MB, Chunks: {num_chunks}, Chunk size: {CHUNK_SIZE >> 20}MB"
    )
    logger.info(f"Max concurrent uploads per batch: {MAX_CONCURRENT_UPLOADS}")

    for batch_idx in range(0, num_chunks, MAX_CONCURRENT_UPLOADS):
        batch_num = batch_idx // MAX_CONCURRENT_UPLOADS + 1
        remaining_chunks = min(MAX_CONCURRENT_UPLOADS, num_chunks - batch_idx)
        logger.info(f"Processing batch {batch_num} ({remaining_chunks} chunks)")
        upload_responses = await upload_chunks(
            client,
            args.file,
            upload,
            last_part_number,
            CHUNK_SIZE,
            batch_idx,
            MAX_CONCURRENT_UPLOADS,
        )
        await register_uploaded_parts(
            client, "minio_user", upload, upload_responses, batch_idx
        )


async def main():
    args = parse_arguments()

    # Set up logging first
    setup_logging(args.log_level)

    logger.info("Starting multipart upload script")
    logger.debug(f"Arguments: {vars(args)}")

    # Validate file exists
    if not args.file.exists():
        print(f"Error: File '{args.file}' does not exist", file=sys.stderr)
        sys.exit(1)

    if not args.file.is_file():
        print(f"Error: '{args.file}' is not a regular file", file=sys.stderr)
        sys.exit(1)

    filename_to_upload = args.file.name
    user_id = "minio_user"

    if args.log_level == "DEBUG":
        print(f"Uploading: {args.file}")
        print(f"File size: {args.file.stat().st_size} bytes")
        print(f"Chunk size: {CHUNK_SIZE} bytes")
        print(f"Base URL: https://localhost:8000")
        print(f"User ID: {user_id}")
        print(f"Content type: application/octet-stream")
        print(f"Upload mode: {'Batched' if args.batched else 'Simple'}")

    try:
        async with Client(
            base_url="http://localhost:8000", raise_on_unexpected_status=True
        ) as client:
            # First check if there's an upload for that file that is not completed
            # in the database. If there are multiple, use the one with the most recent
            # created_at field and delete the others.
            upload, last_part_number = await get_existing_or_start_upload(
                client, filename_to_upload
            )

            if args.batched:
                await upload_batched(args, client, upload, last_part_number)
            else:
                await upload_simple(args, client, upload, last_part_number)

            logger.info("Completing upload")
            response = await complete_upload(client, upload.upload_id, upload.key, user_id)
            logger.info(f"✅ {response.message}")
            if response.location:
                logger.info(f"File location: {response.location}")
    except KeyboardInterrupt:
        logger.info("\n❌ Upload cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.info(f"❌ Upload failed: {e}")
        if args.log_level == "DEBUG":
            import traceback
            traceback.print_exc()
        sys.exit(1)


async def main_batched():
    # TODO: setup argparse
    filename_to_upload = "file.bin"
    file_to_upload = Path(filename_to_upload)
    file_size = file_to_upload.stat().st_size
    user_id = "minio_user"

    num_chunks = math.ceil(file_size / CHUNK_SIZE)
    print(
        f"file_size={file_size >> 20}MB num_chunks={num_chunks} chunk_size={CHUNK_SIZE >> 20}MB"
    )

    async with Client(
        base_url="http://localhost:8000", raise_on_unexpected_status=True
    ) as client:
        # First check if there's an upload for that file that is not completed
        # in the database. If there are multiple, use the one with the most recent
        # created_at field and delete the others.
        upload, last_part_number = await get_existing_or_start_upload(
            client, filename_to_upload
        )

        for batch_idx in range(0, num_chunks, MAX_CONCURRENT_UPLOADS):
            print(f"Processing batch {batch_idx // MAX_CONCURRENT_UPLOADS + 1}")
            upload_responses = await upload_chunks(
                client,
                file_to_upload,
                upload,
                last_part_number,
                CHUNK_SIZE,
                batch_idx,
                MAX_CONCURRENT_UPLOADS,
            )
            await register_uploaded_parts(
                client, user_id, upload, upload_responses, last_part_number, batch_idx
            )

        response = await complete_upload(client, upload.upload_id, upload.key, user_id)
        print(f"{response.message} : {response.location}")


if __name__ == "__main__":
    asyncio.run(main())
