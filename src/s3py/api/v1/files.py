from typing import Annotated

from fastapi import APIRouter, HTTPException
from fastapi.params import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.status import HTTP_201_CREATED, HTTP_200_OK

from s3py.database import get_db
from s3py.models import StartUploadRequest, UploadPartRequest, Upload, Part
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

    curr = result.one_or_none()
    if curr is None:
        raise HTTPException(status_code=404, detail="Upload not found")

    upload = curr[0]
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
    upload.parts.append(part)
    await db.commit()
    await db.refresh(upload)

    return {"success": True}
