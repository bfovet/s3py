from typing import Annotated

from fastapi import APIRouter, HTTPException
from fastapi.params import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from starlette.status import HTTP_201_CREATED, HTTP_200_OK

from s3py.database import get_db
from s3py.models import (
    StartUploadRequest,
    UploadPartRequest,
    Upload,
    Part,
    CompleteUploadRequest,
)
from s3py.s3 import s3_client, S3_BUCKET_NAME

router = APIRouter(tags=["files"])


@router.post(
    "/start-upload",
    summary="Initialize a multipart file upload",
    description="Creates a new multipart upload session in S3 and records it in the database",
    status_code=HTTP_201_CREATED,
)
async def start_upload(
    request: StartUploadRequest, db: Annotated[AsyncSession, Depends(get_db)]
) -> dict[str, str]:
    """
    Start a new multipart upload process:

    - Creates a multipart upload in S3
    - Records the upload session in the database
    - Returns the upload ID and key for later operations

    The client should use this endpoint before uploading any file parts.
    """
    # TODO: needs to be async
    response = s3_client.create_multipart_upload(
        Bucket=S3_BUCKET_NAME, Key=request.filename, ContentType=request.content_type
    )

    try:
        upload = Upload(
            user_id=request.user_id,
            key=response["Key"],
            upload_id=response["UploadId"],
            parts=[],
            status="initiated",
        )
        db.add(upload)
        await db.commit()
        await db.refresh(upload)
    except Exception as e:
        await db.rollback()
        raise e

    return {"upload_id": response["UploadId"], "key": response["Key"]}


@router.get(
    "/presigned-url",
    summary="Get a presigned URL for part upload",
    description="Generates a presigned URL that allows direct upload to S3",
    status_code=HTTP_200_OK,
)
def get_presigned_url(upload_id: str, key: str, part_number: int) -> dict[str, str]:
    """
    Generate a presigned URL for uploading a specific part:

    - Creates a temporary URL valid for 1 hour
    - Client can use this URL to upload the part directly to S3
    - Part number must be between 1 and 10,000

    The client should request a new URL for each part they need to upload.
    """
    # TODO: needs to be async
    presigned_url = s3_client.generate_presigned_url(
        "upload_part",
        Params={
            "Bucket": S3_BUCKET_NAME,
            "Key": key,
            "PartNumber": part_number,
            "UploadId": upload_id,
        },
        ExpiresIn=3600,
    )

    return {"presigned_url": presigned_url}


@router.post(
    "/upload-part",
    summary="Record a successfully uploaded part",
    description="Updates the database with information about an uploaded part",
    status_code=HTTP_201_CREATED,
)
async def upload_part(
    request: UploadPartRequest, db: Annotated[AsyncSession, Depends(get_db)]
) -> dict[str, bool]:
    """
    Record a part that was successfully uploaded to S3:

    - Updates the database with the ETag returned from S3
    - Tracks the part number for later assembly
    - Sets the upload status to "in-progress"

    The client should call this after successfully uploading a part using the presigned URL.
    """
    result = await db.execute(
        select(Upload).where(
            Upload.upload_id == request.upload_id,
            Upload.key == request.key,
            Upload.user_id == request.user_id,
        )
    )

    upload = result.scalar_one_or_none()
    if upload is None:
        raise HTTPException(status_code=404, detail="Upload not found")

    part = Part(
        upload_id=request.upload_id,
        user_id=request.user_id,
        key=request.key,
        etag=request.etag,
        part_number=request.part_number,
        upload=upload,
    )
    db.add(part)
    await db.commit()
    await db.refresh(part)

    upload.status = "in-progress"
    await db.commit()
    await db.refresh(upload)

    return {"success": True}


@router.post(
    "/complete-upload",
    summary="Complete the multipart upload",
    description="Finalizes the multipart upload by combining all parts in S3",
    status_code=HTTP_201_CREATED,
)
async def complete_upload(
    request: CompleteUploadRequest, db: Annotated[AsyncSession, Depends(get_db)]
) -> dict[str, str]:
    """
    Complete a multipart upload by combining all uploaded parts:

    - Retrieves all parts from the database
    - Sends a complete request to S3 with all part ETags
    - Updates the upload status to "completed"
    - Returns the final S3 location of the assembled file

    This should be called after all parts have been successfully uploaded.
    """
    result = await db.execute(
        select(Upload)
        .options(selectinload(Upload.parts))  # Eagerly load the parts relationship
        .where(
            Upload.upload_id == request.upload_id,
            Upload.key == request.key,
            Upload.user_id == request.user_id,
        )
    )

    upload = result.scalar_one_or_none()
    if upload is None:
        raise HTTPException(
            status_code=404, detail="Upload not found or no parts uploaded"
        )

    sorted_parts = sorted(upload.parts, key=lambda x: x.part_number)

    # Validate parts before attempting to complete
    try:
        # TODO: needs to be async
        parts_response = s3_client.list_parts(
            Bucket=S3_BUCKET_NAME, Key=request.key, UploadId=request.upload_id
        )

        s3_parts = {
            part["PartNumber"]: part  # type: ignore
            for part in parts_response.get("Parts", [])
        }

        # Validate each part
        for part in sorted_parts:
            if part.part_number not in s3_parts:
                raise ValueError(f"Part {part.part_number} not found in S3")

            s3_part = s3_parts[part.part_number]
            part_size = s3_part["Size"]  # type: ignore

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

        # TODO: needs to be async
        response = s3_client.complete_multipart_upload(**payload)

        upload.status = "completed"
        await db.commit()
        await db.refresh(upload)

        return {
            "message": "Upload completed successfully",
            "location": response["Location"],
            "key": response["Key"],
        }
    except Exception as e:
        print(f"Error completing multipart upload: {e}")
        try:
            # TODO: needs to be async
            s3_client.abort_multipart_upload(
                Bucket=S3_BUCKET_NAME, Key=request.key, UploadId=request.upload_id
            )
            print("Multipart upload aborted")
        except Exception as abort_error:
            print(f"Error aborting upload: {abort_error}")
        raise
