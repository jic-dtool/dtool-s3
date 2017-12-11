import json
import time

import boto3
from botocore.errorfactory import ClientError

from dtoolcore.utils import (
    generate_identifier,
)

from dtoolcore.filehasher import FileHasher, md5sum_hexdigest


class S3StorageBroker(object):

    #: Attribute used to define the type of storage broker.
    key = "s3"

    #: Attribute used by :class:`dtoolcore.ProtoDataSet` to write the hash
    #: function name to the manifest.
    hasher = FileHasher(md5sum_hexdigest)

    def __init__(self, uri, config_path=None):

        _, self.bucket, uuid = uri.split('/')
        self.metadata_filename = uuid + '/dtool'
        self.s3resource = boto3.resource('s3')
        self.s3client = boto3.client('s3')
        self.readme_fpath = uuid + '/README.yml'
        self.data_prefix = uuid + '/data/'
        self.manifest_fpath = uuid + '/manfest.json'

    @classmethod
    def generate_uri(cls, name, uuid, prefix):
        dataset_reference = "{}/{}".format(prefix, uuid)
        return "{}:{}".format(cls.key, dataset_reference)

    def create_structure(self):
        pass

    def put_admin_metadata(self, admin_metadata):

        for k, v in admin_metadata.items():
            admin_metadata[k] = str(v)

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

    def has_admin_metadata(self):
        """Return True if the administrative metadata exists.

        This is the definition of being a "dataset".
        """

        try:
            self.get_admin_metadata()
            return True
        except ClientError:
            return False

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

    def put_manifest(self, manifest):
        """Store the manifest by writing it to S3.

        It is the client's responsibility to ensure that the manifest provided
        is a dictionary with valid contents.

        :param manifest: dictionary with manifest structural metadata
        """

        self.s3resource.Object(self.bucket, self.manifest_fpath).put(
            Body=json.dumps(manifest)
        )

    def iter_item_handles(self):
        """Return iterator over item handles."""

        bucket = self.s3resource.Bucket(self.bucket)

        for obj in bucket.objects.filter(Prefix=self.data_prefix).all():
            yield obj.key

    def item_properties(self, handle):
        """Return properties of the item with the given handle."""

        obj = self.s3resource.Object(self.bucket, handle)

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

    def pre_freeze_hook(self):
        pass

    def post_freeze_hook(self):
        pass

    def get_item_metadata(self, handle):
        """Return dictionary containing all metadata associated with handle.

        In other words all the metadata added using the ``add_item_metadata``
        method.

        :param handle: handle for accessing an item before the dataset is
                       frozen
        :returns: dictionary containing item metadata
        """

        return {}
