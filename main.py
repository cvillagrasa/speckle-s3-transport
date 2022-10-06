import boto3
from botocore.config import Config
from aws_credentials import ACCESS_KEY, SECRET_KEY


aws_config = Config(
    region_name='eu-west-3'  # Paris, closest to Barcelona
)

client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=aws_config
)
