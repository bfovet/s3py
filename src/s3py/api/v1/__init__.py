from fastapi import APIRouter

from s3py.api.v1.files import router as files_router

router = APIRouter(prefix="/v1")
router.include_router(files_router)
