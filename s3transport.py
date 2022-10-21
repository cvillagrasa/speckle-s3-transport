import os
from typing import Optional
import boto3
from botocore.config import Config
from dotenv import load_dotenv
from specklepy.transports.abstract_transport import AbstractTransport


class S3Transport(AbstractTransport):
    _name: str = 'S3'
    aws_access_key_id: str = ''
    aws_secret_access_key: str = ''
    region_name: str = 'eu-west-3'  # Paris, closest to Barcelona
    config_args: Optional[dict] = None
    bucket_name: str = 'speckle-s3-transport'
    objects: dict = {}
    sent_obj_count: int = 0

    def __init__(self, name=None, **data) -> None:
        super().__init__(**data)
        if name:
            self._name = name
        self.aws_access_key_id = os.environ['ACCESS_KEY']
        self.aws_secret_access_key = os.environ['SECRET_KEY']
        self.config_args = {} if self.config_args is None else self.config_args
        self.client = self.get_client()

    def __repr__(self) -> str:
        return f'S3Transport(objects: {len(self.objects)})'

    @property
    def connection_config(self):
        return Config(region_name=self.region_name, **self.config_args)

    def get_client(self):
        return boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            config=self.connection_config
        )

    def get_buckets(self):
        response = self.client.list_buckets()
        return [bucket['Name'] for bucket in response['Buckets']]

    def create_bucket(self, bucket_name):
        self.client.create_bucket(Bucket=bucket_name)

    def set_bucket(self, bucket_name, missing_ok=False):
        if missing_ok and bucket_name not in self.get_buckets():
            self.create_bucket(bucket_name)
        self.bucket_name = bucket_name

    def get_object(self, id: str) -> Optional[str]:
        response = self.client.get_object(Bucket=self.bucket_name, Key=id)
        return response['Body'].read()

    def save_object(self, id: str, serialized_object: str) -> None:
        self.client.put_object(Bucket=self.bucket_name, Key=id, Body=serialized_object)
        self.objects[id] = serialized_object
        self.sent_obj_count += 1

    def save_object_from_transport(self, id: str, source_transport: AbstractTransport) -> None:
        serialized_object = source_transport.get_object(id)
        self.save_object(id, serialized_object)

    def begin_write(self):
        self.sent_obj_count = 0

    def end_write(self):
        self.sent_obj_count = 0

    def has_objects(self, id_list: list[str]) -> dict[str, bool]:
        """ TODO: intercept concrete exceptions from an offline service, non-existant id, etc. """
        ret = {}
        for id in id_list:
            try:
                obj = self.get_object(key=id)
                ret[id] = True if obj else False
            except:
                ret[id] = False
        return ret

    def copy_object_and_children(self, id: str, target_transport: AbstractTransport) -> str:
        raise NotImplementedError


if __name__ == '__main__':
    load_dotenv("aws_credentials.env")
    s3 = S3Transport()
    print('done!')
