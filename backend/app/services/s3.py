import io
import json
import os
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from fastapi import HTTPException
import boto3

load_dotenv(find_dotenv())

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_DEFAULT_REGION')

if not aws_access_key_id or not aws_secret_access_key or not aws_region:
    raise EnvironmentError("AWS environment variables are not set")

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

bucket_name = "flat-file-state-bucket"

def list_bucket_contents():
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]
        else:
            files = []
        return {"files": files}
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def read_csv(file_key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read()
        return content.decode("utf-8")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def update_csv(file_key, new_content):
    try:
        content = io.StringIO(new_content)
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=content.getvalue())
        return True
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def delete_csv(file_key):
    try:
        s3.delete_object(Bucket=bucket_name, Key=file_key)
        return {"success": f"file {file_key} successfully deleted"}
    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail="INTERNAL SERVER ERROR")


def upload_csv(file_name, file_content):
    if not file_name.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a csv")

    try:
        df = pd.read_csv(io.StringIO(file_content.decode("utf-8")))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read CSV file: {e}")

    expected_headers = ["transaction_id", "bank", "date", "amount", "description"]
    if list(df.columns) != expected_headers:
        raise HTTPException(status_code=400, detail=f"CSV headers must be: {', '.join(expected_headers)}")

    unique_ids = set()
    for index, row in df.iterrows():
        try:
            transaction_id = int(row["transaction_id"])
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Transaction ID must be an integer at row {index + 1}")

        if transaction_id in unique_ids:
            raise HTTPException(status_code=400, detail=f"Duplicate Transaction ID {transaction_id} at row {index + 1}")

        unique_ids.add(transaction_id)

        bank = row["bank"]
        if not isinstance(bank, str) or len(bank) >= 50:
            raise HTTPException(status_code=400,
                                detail=f"Bank name must be a string less than 50 characters at row {index + 1}")

        date = row["date"]
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Date must be in the format yyyy-mm-dd at row {index + 1}")

        try:
            amount = round(float(row["amount"]), 2)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Amount must be a float at row {index + 1}")

        description = row["description"]
        if not isinstance(description, str) or len(description) >= 50:
            raise HTTPException(status_code=400,
                                detail=f"Description must be a string less than 50 characters at row {index + 1}")

    try:
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=file_content)
        return {"success": f"file {file_name} successfully uploaded"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {e}")
