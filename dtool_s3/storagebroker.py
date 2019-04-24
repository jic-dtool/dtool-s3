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
    DEFAULT_CACHE_PATH,
)

from dtoolcore.filehasher import FileHasher, md5sum_hexdigest

from dtoolcore.storagebroker import BaseStorageBroker

from dtool_s3 import __version__


# We update the dataset_registration_key when we have the UUID for the dataset
_STRUCTURE_PARAMETERS = {
    "dataset_registration_key": None,
    "data_key_infix": "data",
    "fragment_key_infix": "fragments",
    "overlays_key_infix": "overlays",
    "structure_key_suffix": "structure.json",
    "dtool_readme_key_suffix": "README.txt",
    "dataset_readme_key_suffix": "README.yml",
    "manifest_key_suffix": "manifest.json",
    "admin_metadata_key_suffix": "dtool",
    "http_manifest_key": "http_manifest.json",
    "storage_broker_version": __version__,
}

_DTOOL_README_TXT = """README
======
This is a Dtool dataset stored in S3 accessible storage.

Content provided during the dataset creation process
----------------------------------------------------

Dataset registration key (at top level of bucket): dtool-$UUID

Where UUID is the unique identifier for the dataset. The file is empty and
used to aid fast enumeration of unique datasets in the bucket.

The UUID is used as a prefix for all other keys in the dataset.

Dataset descriptive metadata: $UUID/README.yml
Dataset items prefixed by: $UUID/data/

The item identifiers are used to name the files using the data
prefix.

An item identifier is the sha1sum hexdigest of the relative path
used to represent the file on traditional file system disk.

Automatically generated files and directories
---------------------------------------------

This file: $UUID/README.txt
Administrative metadata describing the dataset: $UUID/dtool
Structural metadata describing the dataset: $UUID/structure.json
Structural metadata describing the data items: $UUID/manifest.json
Per item descriptive metadata prefixed by: $UUID/overlays/
"""


class S3StorageBroker(BaseStorageBroker):

    #: Attribute used to define the type of storage broker.
    key = "s3"

    #: Attribute used by :class:`dtoolcore.ProtoDataSet` to write the hash
    #: function name to the manifest.
    hasher = FileHasher(md5sum_hexdigest)

    _dtool_readme_txt = _DTOOL_README_TXT

    def __init__(self, uri, config_path=None):

        parse_result = generous_parse_uri(uri)

        self.bucket = parse_result.netloc
        uuid = parse_result.path[1:]

        self.uuid = uuid
        self.s3resource = boto3.resource('s3')
        self.s3client = boto3.client('s3')

        self._structure_parameters = _STRUCTURE_PARAMETERS
        self.dataset_registration_key = 'dtool-{}'.format(self.uuid)
        self._structure_parameters["dataset_registration_key"] = self.dataset_registration_key  # NOQA

        self.data_key_prefix = self._generate_key_prefix("data_key_infix")
        self.fragments_key_prefix = self._generate_key_prefix(
            "fragment_key_infix"
        )
        self.overlays_key_prefix = self._generate_key_prefix(
            "overlays_key_infix"
        )

        self.http_manifest_key = self._generate_key("http_manifest_key")

        self._s3_cache_abspath = get_config_value(
            "DTOOL_CACHE_DIRECTORY",
            config_path=config_path,
            default=DEFAULT_CACHE_PATH
        )

    # Generic helper functions.

    def _generate_key(self, structure_dict_key):
        return self.uuid + '/' + self._structure_parameters[structure_dict_key]

    def _generate_key_prefix(self, structure_dict_key):
        return self._generate_key(structure_dict_key) + '/'

    def _get_item_object(self, handle):
        identifier = generate_identifier(handle)
        item_key = self.data_key_prefix + identifier
        obj = self.s3resource.Object(self.bucket, item_key)
        return obj

    def _make_key_public(self, key):
        acl = self.s3resource.ObjectAcl(self.bucket, key)
        acl.put(ACL='public-read')

    def _generate_http_manifest(self):

        readme_url = self.generate_key_url(self.get_readme_key())
        manifest_url = self.generate_key_url(self.get_manifest_key())

        overlays = {}
        for overlay_name in self.list_overlay_names():
            overlay_fpath = self.overlays_key_prefix + overlay_name + '.json'
            overlays[overlay_name] = self.generate_key_url(overlay_fpath)

        manifest = self.get_manifest()
        item_urls = {}
        for identifier in manifest["items"]:
            item_urls[identifier] = self.generate_key_url(
                self.data_key_prefix + identifier
            )

        http_manifest = {
            "admin_metadata": self.get_admin_metadata(),
            "item_urls": item_urls,
            "overlays": overlays,
            "readme_url": readme_url,
            "manifest_url": manifest_url
        }

        return http_manifest

    def _write_http_manifest(self, http_manifest):

        self.s3resource.Object(self.bucket, self.http_manifest_key).put(
            Body=json.dumps(http_manifest)
        )

        self._make_key_public(self.http_manifest_key)

    # Class methods to override.

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

    # Methods to override.

    def _create_structure(self):
        self.s3resource.Object(self.bucket, self.dataset_registration_key).put(
            Body=''
        )

    def put_text(self, key, content):
        self.s3resource.Object(self.bucket, key).put(
            Body=content
        )

    def get_text(self, key):

        response = self.s3resource.Object(
            self.bucket,
            key
        ).get()

        return response['Body'].read().decode('utf-8')

    def get_structure_key(self):
        return self._generate_key("structure_key_suffix")

    def get_dtool_readme_key(self):
        return self._generate_key("dtool_readme_key_suffix")

    def get_readme_key(self):
        return self._generate_key("dataset_readme_key_suffix")

    def get_overlay_key(self, overlay_name):
        return os.path.join(self.overlays_key_prefix, overlay_name + '.json')

    def get_manifest_key(self):
        return self._generate_key("manifest_key_suffix")

    def get_admin_metadata_key(self):
        return self._generate_key("admin_metadata_key_suffix")

    def put_admin_metadata(self, admin_metadata):

        str_admin_metadata = {}
        for k, v in admin_metadata.items():
            str_admin_metadata[k] = str(v)

        self.s3resource.Object(self.bucket, self.get_admin_metadata_key()).put(
            Body=json.dumps(admin_metadata),
            Metadata=str_admin_metadata
        )

    def get_admin_metadata(self):

        response = self.s3resource.Object(
            self.bucket,
            self.get_admin_metadata_key()
        ).get()

        return response['Metadata']

    def get_size_in_bytes(self, handle):
        obj = self._get_item_object(handle)
        return int(obj.content_length)

    def get_utc_timestamp(self, handle):
        obj = self._get_item_object(handle)
        return time.mktime(obj.last_modified.timetuple())

    def get_hash(self, handle):

        # Here the calculated MD5 checksum is retrieved from the
        # metadata. This is needed as the AWS etag is
        # not the md5 sum of the uploaded object for items that are uploaded
        # using multipart uploads (large files).
        # See: https://stackoverflow.com/a/43067788
        obj = self._get_item_object(handle)
        return obj.get()['Metadata']['checksum']

# According to the tests the below is not needed.
#   def get_relpath(self, handle):
#       obj = self._get_item_object(handle)
#       return obj.get()['Metadata']['handle']

    def has_admin_metadata(self):
        """Return True if the administrative metadata exists.

        This is the definition of being a "dataset".
        """

        try:
            self.get_admin_metadata()
            return True
        except ClientError:
            return False

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

        bucket_fpath = self.data_key_prefix + identifier
        obj = self.s3resource.Object(self.bucket, bucket_fpath)
        relpath = obj.get()['Metadata']['handle']
        _, ext = os.path.splitext(relpath)

        local_item_abspath = os.path.join(
            dataset_cache_abspath,
            identifier + ext
        )
        if not os.path.isfile(local_item_abspath):

            tmp_local_item_abspath = local_item_abspath + ".tmp"
            self.s3resource.Bucket(self.bucket).download_file(
                bucket_fpath,
                tmp_local_item_abspath
            )
            os.rename(tmp_local_item_abspath, local_item_abspath)

        return local_item_abspath

    def list_overlay_names(self):
        """Return list of overlay names."""

        bucket = self.s3resource.Bucket(self.bucket)

        overlay_names = []
        for obj in bucket.objects.filter(
            Prefix=self.overlays_key_prefix
        ).all():

            overlay_file = obj.key.rsplit('/', 1)[-1]
            overlay_name, ext = overlay_file.split('.')
            overlay_names.append(overlay_name)

        return overlay_names

    def put_item(self, fpath, relpath):

        # Here the MD5 checksum is calculated so that it can be uploaded with
        # the item as a piece of metadata. This is needed as the AWS etag is
        # not the md5 sum of the uploaded object for items that are uploaded
        # using multipart uploads (large files).
        # See: https://stackoverflow.com/a/43067788
        checksum = S3StorageBroker.hasher(fpath)

        fname = generate_identifier(relpath)
        dest_path = self.data_key_prefix + fname
        self.s3client.upload_file(
            fpath,
            self.bucket,
            dest_path,
            ExtraArgs={'Metadata': {
                'handle': relpath,
                'checksum': checksum,
                }
            }
        )

        return relpath

    def add_item_metadata(self, handle, key, value):
        """Store the given key:value pair for the item associated with handle.

        :param handle: handle for accessing an item before the dataset is
                       frozen
        :param key: metadata key
        :param value: metadata value
        """

        identifier = generate_identifier(handle)
        suffix = '{}.{}.json'.format(identifier, key)
        bucket_fpath = self.fragments_key_prefix + suffix

        self.s3resource.Object(self.bucket, bucket_fpath).put(
            Body=json.dumps(value)
        )

    def iter_item_handles(self):
        """Return iterator over item handles."""

        bucket = self.s3resource.Bucket(self.bucket)

        for obj in bucket.objects.filter(Prefix=self.data_key_prefix).all():
            relpath = obj.get()['Metadata']['handle']

            yield relpath

    def pre_freeze_hook(self):
        pass

    def post_freeze_hook(self):

        # Delete the temporary fragment metadata objects from the bucket.

        # Get the keys of the fragment metadata objects.
        bucket = self.s3resource.Bucket(self.bucket)
        prefix_object_keys = [
            obj.key for obj in
            bucket.objects.filter(Prefix=self.fragments_key_prefix).all()
        ]

        def _chunks(l, n):
            """Yield successive n-sized chunks from l."""

            for i in range(0, len(l), n):
                yield l[i:i + n]

        # Delete the chunks of 500 fragment metadata objects.
        for keys in _chunks(prefix_object_keys, 500):
            keys_as_list_of_dicts = [{'Key': k} for k in keys]
            bucket.objects.delete(
                Delete={'Objects': keys_as_list_of_dicts}
            )

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

        identifier = generate_identifier(handle)
        prefix = self.fragments_key_prefix + '{}'.format(identifier)
        for obj in bucket.objects.filter(Prefix=prefix).all():
            metadata_key = obj.key.split('.')[-2]
            response = obj.get()
            value_as_string = response['Body'].read().decode('utf-8')
            value = json.loads(value_as_string)

            metadata[metadata_key] = value

        return metadata

    # HTTP enabling functions

    def generate_key_url(self, key):

        url = "https://{}.s3.amazonaws.com/{}".format(
            self.bucket,
            key
        )

        return url

    def http_enable(self):

        self._make_key_public(self.get_readme_key())
        self._make_key_public(self.get_manifest_key())

        for overlay_name in self.list_overlay_names():
            overlay_fpath = self.overlays_key_prefix + overlay_name + '.json'
            self._make_key_public(overlay_fpath)

        manifest = self.get_manifest()
        for identifier in manifest["items"]:
            self._make_key_public(self.data_key_prefix + identifier)

        http_manifest = self._generate_http_manifest()
        self._write_http_manifest(http_manifest)

        access_url = "https://{}.s3.amazonaws.com/{}".format(
            self.bucket,
            self.uuid
        )

        return access_url

    def _list_historical_readme_keys(self):
        # This method is used to test the
        # BaseStorageBroker.readme_update method.
        prefix = self.get_readme_key() + "-"
        historical_readme_keys = []

        bucket = boto3.resource('s3').Bucket(self.bucket)
        for obj in bucket.objects.filter(Prefix=prefix).all():
            historical_readme_keys.append(obj.key)

        return historical_readme_keys
