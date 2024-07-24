from fastapi import APIRouter, HTTPException, Depends
from app.dependencies import verify_token
from app.services.s3 import (
    list_bucket_contents,
    delete_csv,
)

router = APIRouter(
    prefix="/database",
    tags=["database"],
    dependencies=[Depends(verify_token)],
)


@router.get("/")
def list_bucket_contents_router():
    try:
        return list_bucket_contents()
    except HTTPException as e:
        raise e


@router.delete("/")
def delete_csv_router(file_name: str):
    try:
        return delete_csv(file_name)
    except HTTPException as e:
        raise e