from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from typing import Optional
import io
from app.dependencies import verify_token
from app.services.csv_handling import (
    get_all_transactions,
    get_by_id,
    get_transactions_by_description,
    get_transactions_by_date,
    sort_transactions,
    add_transaction,
    update_transaction,
    delete_transaction,
    reconcile_transactions,
    reconcile_export
)

router = APIRouter(
    prefix="/transactions",
    tags=["transactions"],
    dependencies=[Depends(verify_token)],
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
def add_transaction_route(file_name: str, bank: str, date: str, amount: float, description: str, transaction_id: Optional[int] = None):
    try:
        return add_transaction(file_name, bank, date, amount, description, transaction_id)
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


@router.get("/reconcile")
def reconcile_transactions_route(file_1: str, file_2: str):
    try:
        return reconcile_transactions(file_1, file_2)
    except HTTPException as e:
        raise e


@router.get("/reconcile-csv")
def reconcile_transactions_route(file_1: str, file_2: str):
    try:
        csv_content = reconcile_export(file_1, file_2)
        csv_stream = io.StringIO(csv_content)

        response = StreamingResponse(csv_stream, media_type="text/csv")
        response.headers["Content-Dispostion"] = "attachment; filename=reconcile.csv"
        return response
    except HTTPException as e:
        raise e
