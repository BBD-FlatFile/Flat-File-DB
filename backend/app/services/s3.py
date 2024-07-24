import io
import json
import os
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


def list_bucket_contents():
    try:
        response = s3.list_objects_v2(Bucket="flat-file-state-bucket")
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
        response = s3.get_object(Bucket="flat-file-state-bucket", Key=file_key)
        content = response['Body'].read()
        return content.decode("utf-8")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def update_csv(file_key, new_content):
    try:
        content = io.StringIO(new_content)
        s3.put_object(Bucket="flat-file-state-bucket", Key=file_key, Body=content.getvalue())
        return True
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def delete_csv(file_key):
    try:
        s3.delete_object(Bucket="flat-file-state-bucket", Key=file_key)
        return {"success": f"file {file_key} successfully deleted"}
    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail="INTERNAL SERVER ERROR")
