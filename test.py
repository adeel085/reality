# import logging
# import boto3
# from botocore.exceptions import ClientError
# import os
#
# def upload_file(file_name, bucket, object_name=None):
#     if object_name is None:
#         object_name = os.path.basename(file_name)
#
#     # Upload the file
#     s3_client = boto3.client('s3',
#              aws_access_key_id="AKIA3INQ36IHM2ORC4A4",
#              aws_secret_access_key="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")
#     try:
#         with open(file_name, "rb") as f:
#             s3_client.upload_fileobj(f, bucket, object_name)
#
#     except ClientError as e:
#         logging.error(e)
#         return False
#     return True
# upload_file("reality.csv", "reality-data", "reality.csv")
#
import logging
import boto3
from botocore.exceptions import ClientError
import os


def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3',
                             aws_access_key_id="AKIA3INQ36IHM2ORC4A4",
                             aws_secret_access_key="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")

    # Check if the object already exists
    try:
        s3_client.head_object(Bucket=bucket, Key=object_name)
        # If the object exists, delete it
        s3_client.delete_object(Bucket=bucket, Key=object_name)
        print(f"Deleted existing object: {object_name}")
    except ClientError as e:
        # Ignore error if the object does not exist
        if e.response['Error']['Code'] != '404':
            print(e)
            return False

    # Upload the new file
    try:
        with open(file_name, "rb") as f:
            s3_client.upload_fileobj(f, bucket, object_name)
        print(f"Uploaded new file: {object_name}")
    except ClientError as e:
        print(e)
        return False

    return True


upload_file("reality.csv", "reality-data", "reality.csv")
