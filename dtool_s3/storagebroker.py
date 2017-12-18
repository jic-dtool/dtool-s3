import os
import json
import time

try:
    from urlparse import urlunparse
except ImportError:
    from urllib.parse import urlunparse

import boto3
from botocore.errorfactory import ClientError

from dtoolcore.utils import (
    generate_identifier,
    get_config_value,
    mkdir_parents,
    generous_parse_uri,
)

from dtoolcore.filehasher import FileHasher, md5sum_hexdigest


class S3StorageBroker(object):

    #: Attribute used to define the type of storage broker.
    key = "s3"

    #: Attribute used by :class:`dtoolcore.ProtoDataSet` to write the hash
    #: function name to the manifest.
    hasher = FileHasher(md5sum_hexdigest)

    def __init__(self, uri, config_path=None):

        parse_result = generous_parse_uri(uri)

        self.bucket = parse_result.netloc
        uuid = parse_result.path[1:]

        self.uuid = uuid
        self.metadata_filename = uuid + '/dtool'
        self.s3resource = boto3.resource('s3')
        self.s3client = boto3.client('s3')
        self.readme_fpath = uuid + '/README.yml'
        self.data_prefix = uuid + '/data/'
        self.manifest_fpath = uuid + '/manfest.json'
        self.fragment_prefix = uuid + '/fragments/'
        self.overlays_prefix = uuid + '/overlays/'

        self._s3_cache_abspath = get_config_value(
            "DTOOL_S3_CACHE_DIRECTORY",
            config_path=config_path,
            default=os.path.expanduser("~/.cache/dtool/s3")
        )

    @classmethod
    def list_dataset_uris(cls, base_uri, config_path):
        """Return list containing URIs with base URI."""
        uri_list = []

        parse_result = generous_parse_uri(base_uri)
        bucket_name = parse_result.netloc
        bucket = boto3.resource('s3').Bucket(bucket_name)

        for obj in bucket.objects.filter(Prefix='dtool').all():
            uuid = obj.key.split('-', 1)[1]
            uri = cls.generate_uri(None, uuid, base_uri)

            storage_broker = cls(uri, config_path)
            if storage_broker.has_admin_metadata():
                uri_list.append(uri)

        return uri_list

    @classmethod
    def generate_uri(cls, name, uuid, base_uri):

        scheme, netloc, path, _, _, _ = generous_parse_uri(base_uri)
        assert scheme == 's3'

        # Force path (third component of tuple) to be the dataset UUID
        uri = urlunparse((scheme, netloc, uuid, _, _, _))

        return uri

    def create_structure(self):

        registration_key_name = 'dtool-{}'.format(self.uuid)
        self.s3resource.Object(self.bucket, registration_key_name).put(
            Body=''
        )


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

    def put_overlay(self, overlay_name, overlay):
        """Store the overlay by writing it to S3.

        It is the client's responsibility to ensure that the overlay provided
        is a dictionary with valid contents.

        :param overlay_name: name of the overlay
        :overlay: overlay dictionary
        """
        bucket_fpath = os.path.join(self.overlays_prefix, overlay_name + '.json')
        self.s3resource.Object(self.bucket, bucket_fpath).put(
            Body=json.dumps(overlay)
        )

#############################################################################
# Methods only used by DataSet.
#############################################################################

    def get_manifest(self):
        """Return the manifest contents from S3.

        :returns: manifest as a dictionary
        """
        response = self.s3resource.Object(
            self.bucket,
            self.manifest_fpath
        ).get()

        manifest_as_string = response['Body'].read().decode('utf-8')
        return json.loads(manifest_as_string)

    def get_item_abspath(self, identifier):
        """Return absolute path at which item content can be accessed.

        :param identifier: item identifier
        :returns: absolute path from which the item content can be accessed
        """
        admin_metadata = self.get_admin_metadata()
        uuid = admin_metadata["uuid"]
        # Create directory for the specific dataset.
        dataset_cache_abspath = os.path.join(self._s3_cache_abspath, uuid)
        mkdir_parents(dataset_cache_abspath)

        bucket_fpath = self.data_prefix + identifier
        obj = self.s3resource.Object(self.bucket, bucket_fpath)
        relpath = obj.get()['Metadata']['handle']
        _, ext = os.path.splitext(relpath)

        local_item_abspath = os.path.join(
            dataset_cache_abspath,
            identifier + ext
        )
        if not os.path.isfile(local_item_abspath):

            self.s3resource.Bucket(self.bucket).download_file(
                bucket_fpath,
                local_item_abspath
            )

        return local_item_abspath

    def list_overlay_names(self):
        """Return list of overlay names."""

        bucket = self.s3resource.Bucket(self.bucket)

        overlay_names = []
        for obj in bucket.objects.filter(Prefix=self.overlays_prefix).all():

            overlay_file = obj.key.rsplit('/', 1)[-1]
            overlay_name, ext = overlay_file.split('.')
            overlay_names.append(overlay_name)

        return overlay_names

    def get_overlay(self, overlay_name):
        """Return overlay as a dictionary.

        :param overlay_name: name of the overlay
        :returns: overlay as a dictionary
        """

        overlay_fpath = self.overlays_prefix + overlay_name + '.json'

        response = self.s3resource.Object(
            self.bucket,
            overlay_fpath
        ).get()

        overlay_as_string = response['Body'].read().decode('utf-8')
        return json.loads(overlay_as_string)


#############################################################################
# Methods only used by ProtoDataSet.
#############################################################################

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

        return relpath

    def add_item_metadata(self, handle, key, value):
        """Store the given key:value pair for the item associated with handle.

        :param handle: handle for accessing an item before the dataset is
                       frozen
        :param key: metadata key
        :param value: metadata value
        """

        bucket_fpath = self.fragment_prefix + '.{}.json'.format(key)

        self.s3resource.Object(self.bucket, bucket_fpath).put(
            Body=json.dumps(value)
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
            relpath = obj.get()['Metadata']['handle']

            yield relpath

    def item_properties(self, handle):
        """Return properties of the item with the given handle."""

        identifier = generate_identifier(handle)
        bucket_fpath = self.data_prefix + identifier
        obj = self.s3resource.Object(self.bucket, bucket_fpath)

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

        bucket = self.s3resource.Bucket(self.bucket)

        metadata = {}
        for obj in bucket.objects.filter(Prefix=self.fragment_prefix).all():
            metadata_key = obj.key.split('.')[-2]
            response = obj.get()
            value_as_string = response['Body'].read().decode('utf-8')
            value = json.loads(value_as_string)

            metadata[metadata_key] = value

        return metadata
