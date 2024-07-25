import io

from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from fastapi.responses import StreamingResponse
from app.dependencies import verify_token
from app.services.s3 import (
    list_bucket_contents,
    delete_csv,
    upload_csv,
    read_csv
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


@router.post("/")
def upload_file_router(file: UploadFile = File(...)):
    try:
        file_content = file.file.read()
        file_name = file.filename
        return upload_csv(file_name, file_content)
    except HTTPException as e:
        raise e
    finally:
        file.file.close()


@router.get("/download")
def download_csv_router(file_name: str):
    try:
        csv_content = read_csv(file_name)
        csv_stream = io.StringIO(csv_content)

        response = StreamingResponse(csv_stream, media_type="text/csv")
        response.headers["Content-Dispostion"] = f"attachment; filename={file_name}"
        return response
    except HTTPException as e:
        raise e
