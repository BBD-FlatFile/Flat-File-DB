from fastapi import APIRouter, UploadFile, File, HTTPException
from typing import Optional
from app.services.csv_handling import (
    get_all_transactions,
    get_by_id,
    get_transactions_by_description,
    get_transactions_by_date,
    sort_transactions,
    add_transaction,
    update_transaction,
    delete_transaction,
    reconcile_transactions
)
from app.services.S3_handling import upload_file_to_s3


router = APIRouter(
    prefix="/transactions",
    tags=["transactions"]
)


@router.get("/")
def get_all_transactions_route(file_name: str):
    try:
        return get_all_transactions(file_name)
    except HTTPException as e:
        raise e


@router.get("/get_by_id")
def get_by_id_route(file_name: str, id: int):
    try:
        return get_by_id(file_name, id)
    except HTTPException as e:
        raise e


@router.get("/get_by_description")
def get_transactions_by_description_route(file_name: str, description: str):
    try:
        return get_transactions_by_description(file_name, description)
    except HTTPException as e:
        raise e


@router.get("/get_by_date")
def get_transactions_by_date_route(file_name: str, start_date: str, end_date: Optional[str] = None):
    try:
        return get_transactions_by_date(file_name, start_date, end_date)
    except HTTPException as e:
        raise e


@router.get("/sort")
def sort_transactions_route(file_name: str, sort_by: str, order: str):
    try:
        return sort_transactions(file_name, sort_by, order)
    except HTTPException as e:
        raise e


@router.post("/")
def add_transaction_route(file_name: str, transaction_id: int, bank: str, date: str, amount: float, description: str):
    try:
        return add_transaction(file_name, transaction_id, bank, date, amount, description)
    except HTTPException as e:
        raise e


@router.put("/")
def update_transaction_route(file_name: str, transaction_id: int, bank: Optional[str] = None, date: Optional[str] = None, amount: Optional[float] = None, description: Optional[str] = None):
    try:
        return update_transaction(file_name, transaction_id, bank, date, amount, description)
    except HTTPException as e:
        raise e


@router.delete("/")
def delete_transaction_route(file_name: str, transaction_id: int):
    try:
        return delete_transaction(file_name, transaction_id)
    except HTTPException as e:
        raise e


@router.post("/upload_to_s3")
async def upload_to_s3(file: UploadFile = File(...)):
    bucket_name = "flat-file-state-bucket"
    try:
        response = upload_file_to_s3(file, bucket_name)
        return response
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")

@router.get("/reconcile")
def reconcile_transactions_route(file_1: str, file_2: str):
    try:
        return reconcile_transactions(file_1, file_2)
    except HTTPException as e:
        raise e
