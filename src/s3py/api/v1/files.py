from typing import Annotated

from fastapi import APIRouter
from fastapi.params import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.status import HTTP_201_CREATED

from s3py.database import get_db
from s3py.models import StartUploadRequest, Upload
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
