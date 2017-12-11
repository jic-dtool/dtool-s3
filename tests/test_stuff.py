import os
import json
import time

import boto3

from dtoolcore import ProtoDataSet, generate_admin_metadata

from dtoolcore.utils import (
    generate_identifier,
    base64_to_hex,
    get_config_value,
    mkdir_parents,
)

class S3StorageBroker(object):

    def __init__(self, uri):
        self.metadata_filename = uri + '/dtool'
        self.s3resource = boto3.resource('s3')
        self.s3client = boto3.client('s3')
        self.bucket = 'test-dtool-s3-bucket'
        self.readme_fpath = uri + '/README.yml'
        self.data_prefix = uri + '/data/'

    def create_structure(self):
        pass

    def put_admin_metadata(self, admin_metadata):

        self.s3resource.Object(self.bucket, self.metadata_filename).put(
            Body='dtoolfile',
            Metadata=admin_metadata
        )

    def get_admin_metadata(self):

        response = self.s3resource.Object(
            self.bucket,
            self.metadata_filename
        ).get()

        return response['Metadata']

    def get_readme_content(self):

        response = self.s3resource.Object(
            self.bucket,
            self.readme_fpath
        ).get()

        return response['Body'].read().decode('utf-8')

    def put_readme(self, content):

        self.s3resource.Object(self.bucket, self.readme_fpath).put(
            Body=content
        )

    def put_item(self, fpath, relpath):

        fname = generate_identifier(relpath)
        dest_path = self.data_prefix + fname
        self.s3client.upload_file(
            fpath,
            self.bucket,
            dest_path,
            ExtraArgs={'Metadata': {'handle': relpath}}
        )

    def iter_item_handles(self):
        bucket = self.s3resource.Bucket(self.bucket)

        for obj in bucket.objects.filter(Prefix=self.data_prefix).all():
            print(obj.key)
            yield self.s3resource.Object(self.bucket, obj.key).metadata['handle']

    def item_properties(self, handle):

        bucket_fname = self.data_prefix + handle

        obj = self.s3resource.Object(self.bucket, bucket_fname)


        size = int(obj.content_length)
        checksum = obj.e_tag[1:-1]
        relpath = obj.get()['Metadata']['handle']
        timestamp = time.mktime(obj.last_modified.timetuple())

        properties = {
            'hash': checksum,
            'size_in_bytes': size,
            'relpath': relpath,
            'utc_timestamp': timestamp
        }

        return properties


def create_bucket():

    s3 = boto3.client('s3')

    s3.create_bucket(
        Bucket='test-dtool-s3-bucket',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'}
    )


def list_buckets():
    s3 = boto3.client('s3')

    # Call S3 to list current buckets
    response = s3.list_buckets()

    for bucket in response['Buckets']:
        print(bucket['Name'])


def main():

    # list_buckets()

    uuid = '123445'

    s3 = boto3.resource('s3')
    bucket_name = 'test-dtool-s3-bucket'
    s3broker = S3StorageBroker(uuid)
    s3broker.put_item('data/reads_1.fq.gz', 'reads1.fq.gz')
    s3broker.put_item('data/reads_2.fq.gz', 'reads2.fq.gz')
    # admin_metadata = generate_admin_metadata(name)
    # s3broker.put_admin_metadata(admin_metadata)

    # s3broker.put_readme('---\nHello: world')
    # print(s3broker.get_admin_metadata())

    # print(s3broker.get_readme_content())

    # print(list(s3broker.iter_item_handles()))

    for handle in s3broker.iter_item_handles():
        print(handle)

    print(s3broker.item_properties('a1c32a4654050fc16626766ffa74ca48797e2dc2'))

if __name__ == '__main__':
    main()
