import boto3
import os
from fastapi import HTTPException
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

s3_client = boto3.client('s3')

def upload_file_to_s3(file, bucket_name, object_name=None):
    bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
    print("AWS_ACCESS_KEY_ID:", os.getenv("AWS_ACCESS_KEY_ID"))
    print("AWS_SECRET_ACCESS_KEY:", os.getenv("AWS_SECRET_ACCESS_KEY"))
    print("AWS_REGION:", os.getenv("AWS_REGION"))
    print("AWS_S3_BUCKET_NAME:", bucket_name)

    try:
        if object_name is None:
            object_name = file.filename
        s3_client.upload_fileobj(file.file, bucket_name, object_name)
        return {"message": "File uploaded successfully", "file_url": f"s3://{bucket_name}/{object_name}"}
    except NoCredentialsError:
        raise HTTPException(status_code=403, detail="AWS credentials not available")
    except PartialCredentialsError:
        raise HTTPException(status_code=403, detail="Incomplete AWS credentials")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")
