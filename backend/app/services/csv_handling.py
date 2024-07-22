import pandas as pd
from datetime import datetime
from fastapi import HTTPException


def get_all_transactions(filepath):
    df = pd.read_csv(filepath)
    transactions = df.to_dict(orient="records")
    return {"transactions": transactions}


def get_transactions_by_description(filepath, description):
    df = pd.read_csv(filepath)

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

    df = pd.read_csv(filepath)

    df['date'] = pd.to_datetime(df['date'])

    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    filtered_df = df.loc[mask]

    if filtered_df.empty:
        raise HTTPException(status_code=404, detail=f"No transactions found between {start_date} and {end_date}")

    filtered_df['date'] = filtered_df['date'].dt.strftime("%Y-%m-%d")

    transactions = filtered_df.to_dict(orient='records')
    return {"transactions": transactions}


def get_by_id(filepath, transaction_id):
    df = pd.read_csv(filepath)

    result = df[df["transaction_id"] == transaction_id]
    if not result.empty:
        transaction = result.iloc[0].to_dict()
        return {"transaction": transaction}
    else:
        raise HTTPException(status_code=404, detail=f"No transaction found with ID {transaction_id}")


def sort_transactions(filepath, sort_by, order):

    if sort_by not in ["amount", "description", "date"]:
        raise HTTPException(status_code=400, detail="sort_by must be one of 'amount', 'description', or 'date'")

    if order not in ["ascending", "descending"]:
        raise HTTPException(status_code=400, detail="order must be either 'ascending' or 'descending'")

    df = pd.read_csv(filepath)

    ascending = True if order == "ascending" else False

    sorted_df = df.sort_values(by=sort_by, ascending=ascending)

    transactions = sorted_df.to_dict(orient="records")
    return {"transactions": transactions}


def add_transaction(filepath, transaction_id, date, amount, description):
    if not all([transaction_id, date, amount, description]):
        raise HTTPException(status_code=400, detail="All fields must be provided")

    try:
        transaction_id = int(transaction_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Transaction ID must be an integer")

    try:
        date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Date must be in the format yyyy-mm-dd")

    try:
        amount = float(amount)
    except ValueError:
        raise HTTPException(status_code=400, detail="Amount must be a float")

    if len(description) >= 50:
        raise HTTPException(status_code=400, detail="Description must be less than 50 characters")

    df = pd.read_csv(filepath)

    if transaction_id in df["transaction_id"].values:
        raise HTTPException(status_code=409, detail=f"Transaction ID {transaction_id} already exists")

    new_transaction = {
        "transaction_id": transaction_id,
        "date": date,
        "amount": amount,
        "description": description
    }

    new_transaction_df = pd.DataFrame([new_transaction])
    updated_df = pd.concat([df, new_transaction_df], ignore_index=True)

    updated_df.to_csv(filepath, index=False)

    transactions = updated_df.to_dict(orient="records")
    return {"transactions": transactions}


def update_transaction(filepath, transaction_id, date=None, amount=None, description=None):
    if date is None and amount is None and description is None:
        return {"error": "At least one field (date, amount, description) must be provided for update"}

    df = pd.read_csv(filepath)

    if transaction_id not in df['transaction_id'].values:
        return {"error": f"Transaction ID {transaction_id} not found"}

    if date is not None:
        try:
            pd.to_datetime(date, format="%Y-%m-%d", errors='raise')
        except ValueError:
            return {"error": "Date must be in the format yyyy-mm-dd"}

    if amount is not None:
        try:
            float(amount)
        except ValueError:
            return {"error": "Amount must be a float"}

    if description is not None:
        if len(description) >= 50:
            return {"error": "Description must be less than 50 characters"}

    if date is not None:
        date = pd.to_datetime(date).strftime("%Y-%m-%d")
        df.loc[df['transaction_id'] == transaction_id, 'date'] = date

    if amount is not None:
        amount = float(amount)
        df.loc[df['transaction_id'] == transaction_id, 'amount'] = amount

    if description is not None:
        df.loc[df['transaction_id'] == transaction_id, 'description'] = description

    df.to_csv(filepath, index=False)

    updated_transaction = df[df['transaction_id'] == transaction_id].to_dict(orient='records')[0]
    return {"transaction": updated_transaction}


def delete_transaction(filepath, transaction_id):
    df = pd.read_csv(filepath)

    index_to_delete = df.index[df["transaction_id"] == transaction_id].tolist()

    if not index_to_delete:
        raise HTTPException(status_code=404, detail=f"Transaction ID {transaction_id} not found")

    df.drop(index_to_delete, inplace=True)

    df.to_csv(filepath, index=False)

    return {"detail": f"Transaction ID {transaction_id} has been deleted"}
