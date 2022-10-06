from dataclasses import dataclass
from typing import Optional
import boto3
from botocore.config import Config
from aws_credentials import ACCESS_KEY, SECRET_KEY


@dataclass
class S3Connection:
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str = 'eu-west-3'  # Paris, closest to Barcelona
    config_args: Optional[dict] = None
    bucket_name: str = 'speckle-s3-transport'

    def __post_init__(self):
        self.config_args = {} if self.config_args is None else self.config_args
        self.client = self.get_client()

    @property
    def config(self):
        return Config(region_name=self.region_name, **self.config_args)

    def get_client(self):
        return boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            config=self.config
        )

    def put_object(self, key):
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=b'foobar'
        )

    def get_object(self, key):
        response = self.client.get_object(Bucket=self.bucket_name, Key=key)
        return response['Body'].read()


s3 = S3Connection(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

print('done!')
