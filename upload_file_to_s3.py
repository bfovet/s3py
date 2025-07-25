import logging
from pathlib import Path
import pprint

# from minio import Minio
# from minio.error import S3Error
import boto3
from botocore.exceptions import ClientError


def bucket_exists(s3_client, bucket_name) -> bool:
    """
    Check if a bucket exists and you have permission to access it.

    Args:
        s3_client: boto3 S3 client
        bucket_name: Name of the bucket to check

    Returns:
        True if bucket exists and accessible, False otherwise
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            return False
        else:
            # Bucket exists but we don"t have permission or other error
            logging.error(f"Error checking bucket {bucket_name}: {e}")
            return False


def create_bucket(s3_client, bucket_name) -> bool:
    """
    Create an S3 bucket in a specified region.

    Args:
        s3_client: boto3 S3 client
        bucket_name: Name of the bucket to create
        region: AWS region to create bucket in

    Returns:
        True if bucket was created or already exists, False otherwise

    """
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} created successfully")
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "BucketAlreadyExists":
            logging.error(
                f"Bucket {bucket_name} already exists and is owned by another account"
            )
            return False
        elif error_code == "BucketAlreadyOwnedByYou":
            logging.info(f"Bucket {bucket_name} already exists and is owned by you")
            return True
        else:
            logging.error(f"Error creating bucket {bucket_name}: {e}")
            return False


def ensure_bucket_exists(s3_client, bucket_name):
    """
    Check if bucket exists, create if it doesn't.

    Args:
        bucket_name: Name of the bucket
        region: AWS region

    Returns:
        True if bucket exists or was created successfully, False otherwise
    """
    # Check if bucket exists
    if bucket_exists(s3_client, bucket_name):
        logging.info(f"Bucket {bucket_name} already exists")
        return True

    # Bucket doesn't exist, try to create it
    logging.info(f"Bucket {bucket_name} doesn't exist, creating...")
    return create_bucket(s3_client, bucket_name)


def main():
    bucket_name = "python-test-bucket"
    Path("/tmp/file.txt").write_text("hello")

    # s3 = Minio("localhost:9000",
    #     access_key="minio_user",
    #     secret_key="minioadmin123",
    #     secure=False
    # )

    # found = s3.bucket_exists(bucket_name)
    # if not found:
    #     s3.make_bucket(bucket_name)
    #     print(f"Created bucket {bucket_name}")
    # else:
    #     print(f"Bucket {bucket_name} already exists")

    # s3.fput_object(bucket_name, "file.txt", "/tmp/file.txt")
    # objects = s3.list_objects(bucket_name, recursive=True)
    # for obj in objects:
    #     print(obj.object_name)

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minio_user",
        aws_secret_access_key="minioadmin123",
    )

    if ensure_bucket_exists(s3, bucket_name):
        print(f"Success! Bucket {bucket_name} is ready to use.")
    else:
        print(f"Failed to ensure bucket {bucket_name} exists.")

    s3.upload_file("/tmp/file.txt", bucket_name, "file.txt")
    response = s3.list_objects(Bucket=bucket_name)
    pp = pprint.PrettyPrinter(depth=2)
    pp.pprint(response)


if __name__ == "__main__":
    main()
