from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from app.dependencies import verify_token
from app.services.s3 import (
    list_bucket_contents,
    delete_csv,
    upload_csv
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


@router.post("/upload")
def upload_file_router(file: UploadFile = File(...)):
    try:
        file_content = file.file.read()
        file_name = file.filename
        return upload_csv(file_name, file_content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {e}")
    finally:
        file.file.close()