import pandas as pd
import numpy as np
import json
import io
from datetime import datetime
from fastapi import HTTPException
from app.services.s3 import (
    read_csv,
    update_csv,
)


def get_all_transactions(filepath):
    csv_content = read_csv(filepath)
    df = pd.read_csv(io.StringIO(csv_content))
    transactions = df.to_dict(orient="records")
    return {"transactions": transactions}


def get_transactions_by_description(filepath, description):
    csv_content = read_csv(filepath)
    df = pd.read_csv(io.StringIO(csv_content))

    filtered_df = df[df["description"] == description]

    if filtered_df.empty:
        raise HTTPException(status_code=404, detail=f"No transactions with description '{description}'")

    transactions = filtered_df.to_dict(orient="records")
    return {"transactions": transactions}


def get_transactions_by_date(filepath, start_date, end_date=None):
    try:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Start date must be in the format yyyy-mm-dd")

    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    else:
        try:
            end_date = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="End date must be in the format yyyy-mm-dd")

    csv_content = read_csv(filepath)
    df = pd.read_csv(io.StringIO(csv_content))

    df['date'] = pd.to_datetime(df['date'])

    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    filtered_df = df.loc[mask]

    if filtered_df.empty:
        raise HTTPException(status_code=404, detail=f"No transactions found between {start_date} and {end_date}")

    filtered_df['date'] = filtered_df['date'].dt.strftime("%Y-%m-%d")

    transactions = filtered_df.to_dict(orient='records')
    return {"transactions": transactions}


def get_by_id(filepath, transaction_id):
    csv_content = read_csv(filepath)
    df = pd.read_csv(io.StringIO(csv_content))

    result = df[df["transaction_id"] == transaction_id]
    if not result.empty:
        transaction = result.iloc[0].to_dict()
        return {"transaction": transaction}
    else:
        raise HTTPException(status_code=404, detail=f"No transaction found with ID {transaction_id}")


def sort_transactions(filepath, sort_by, order):

    if sort_by not in ["bank", "amount", "description", "date"]:
        raise HTTPException(status_code=400, detail="sort_by must be one of 'amount', 'description', or 'date'")

    if order not in ["ascending", "descending"]:
        raise HTTPException(status_code=400, detail="order must be either 'ascending' or 'descending'")

    csv_content = read_csv(filepath)
    df = pd.read_csv(io.StringIO(csv_content))

    ascending = True if order == "ascending" else False

    sorted_df = df.sort_values(by=sort_by, ascending=ascending)

    transactions = sorted_df.to_dict(orient="records")
    return {"transactions": transactions}


def add_transaction(filepath, bank, date, amount, description, transaction_id=None):
    if not all([bank, date, amount, description]):
        raise HTTPException(status_code=400, detail="All fields must be provided")

    if len(bank) >= 50:
        raise HTTPException(status_code=400, detail="bank name must be less than 50 characters")

    if transaction_id is not None:
        try:
            transaction_id = int(transaction_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Transaction ID must be an integer")

    try:
        date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Date must be in the format yyyy-mm-dd")

    try:
        amount = round(float(amount), 2)
    except ValueError:
        raise HTTPException(status_code=400, detail="Amount must be a float")

    if len(description) >= 50:
        raise HTTPException(status_code=400, detail="Description must be less than 50 characters")

    csv_content = read_csv(filepath)
    df = pd.read_csv(io.StringIO(csv_content))

    if transaction_id is None:
        if df.empty:
            transaction_id = 1
        else:
            transaction_id = df["transaction_id"].max() + 1
    else:
        if transaction_id in df["transaction_id"].values:
            raise HTTPException(status_code=409, detail=f"Transaction ID {transaction_id} already exists")

    new_transaction = {
        "transaction_id": transaction_id,
        "bank": bank,
        "date": date,
        "amount": amount,
        "description": description
    }

    new_transaction_df = pd.DataFrame([new_transaction])
    updated_df = pd.concat([df, new_transaction_df], ignore_index=True)

    csv_string = updated_df.to_csv(index=False)

    success = update_csv(filepath, csv_string)

    transactions = updated_df.to_dict(orient="records")
    if success:
        return {"transactions": transactions}
    else:
        raise HTTPException(status_code=500, detail="Unable to add to csv")


def update_transaction(filepath, transaction_id, bank=None, date=None, amount=None, description=None):
    if bank is None and date is None and amount is None and description is None:
        raise HTTPException(status_code=400,
                            detail="At least one field (bank, date, amount, description) must be provided for update")

    csv_content = read_csv(filepath)
    df = pd.read_csv(io.StringIO(csv_content))

    if transaction_id not in df['transaction_id'].values:
        raise HTTPException(status_code=404, detail=f"Transaction ID {transaction_id} not found")

    if bank is not None:
        if len(bank) >= 50:
            raise HTTPException(status_code=400, detail="bank name must be less than 50 characters")

    if date is not None:
        print(date)
        try:
            pd.to_datetime(date, format="%Y-%m-%d", errors='raise')
        except ValueError:
            raise HTTPException(status_code=400, detail="Date must be in the format yyyy-mm-dd")

    if amount is not None:
        try:
            amount = round(float(amount), 2)
        except ValueError:
            raise HTTPException(status_code=400, detail="Amount must be a float")

    if description is not None:
        if len(description) >= 50:
            raise HTTPException(status_code=400, detail="Description must be less than 50 characters")

    if date is not None:
        date = pd.to_datetime(date).strftime("%Y-%m-%d")
        df.loc[df['transaction_id'] == transaction_id, 'date'] = date

    if amount is not None:
        amount = float(amount)
        df.loc[df['transaction_id'] == transaction_id, 'amount'] = amount

    if description is not None:
        df.loc[df['transaction_id'] == transaction_id, 'description'] = description

    if bank is not None:
        df.loc[df['transaction_id'] == transaction_id, 'bank'] = bank

    csv_string = df.to_csv(index=False)

    success = update_csv(filepath, csv_string)

    if success:
        updated_transaction = df[df['transaction_id'] == transaction_id].to_dict(orient='records')[0]
        return {"transaction": updated_transaction}
    else:
        raise HTTPException(status_code=500, detail="Unable to update csv")


def delete_transaction(filepath, transaction_id):
    csv_content = read_csv(filepath)
    df = pd.read_csv(io.StringIO(csv_content))

    index_to_delete = df.index[df["transaction_id"] == transaction_id].tolist()

    if not index_to_delete:
        raise HTTPException(status_code=404, detail=f"Transaction ID {transaction_id} not found")

    df.drop(index_to_delete, inplace=True)

    csv_string = df.to_csv(index=False)

    success = update_csv(filepath, csv_string)

    if success:
        return {"detail": f"Transaction ID {transaction_id} has been deleted"}
    else:
        raise HTTPException(status_code=500, detail="Unable to delete transaction")


def reconcile_transactions(file1, file2):
    csv1 = read_csv(file1)
    df1 = pd.read_csv(io.StringIO(csv1))
    csv2 = read_csv(file2)
    df2 = pd.read_csv(io.StringIO(csv2))

    matches = pd.merge(df1, df2, on=['transaction_id', 'bank', 'date', 'amount', 'description'])

    file1_only = df1[~df1.transaction_id.isin(matches.transaction_id)]

    file2_only = df2[~df2.transaction_id.isin(matches.transaction_id)]

    if file1_only.empty and file2_only.empty:
        status = "files match"
    else:
        status = "files do not match"

    result = {
        'status': status,
        'matches': json.loads(matches.to_json(orient='records')),
        'file1_only': json.loads(file1_only.to_json(orient='records')),
        'file2_only': json.loads(file2_only.to_json(orient='records'))
    }

    return result


def reconcile_export(file1, file2):
    result = reconcile_transactions(file1, file2)

    matches = pd.DataFrame(result['matches'])
    matches['match'] = 'match'

    file1_only = pd.DataFrame(result['file1_only'])
    file1_only['match'] = file1

    file2_only = pd.DataFrame(result['file2_only'])
    file2_only['match'] = file2

    combined_df = pd.concat([matches, file1_only, file2_only])

    csv_result = combined_df.to_csv(index=False)

    return csv_result
