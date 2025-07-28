import boto3


# TODO: use aioboto3
s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minio_user",
    aws_secret_access_key="minioadmin123",
)
S3_BUCKET_NAME = "python-test-bucket"
