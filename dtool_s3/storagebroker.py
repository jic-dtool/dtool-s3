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
        self.s3resource = boto3.resource('s3')
        self.s3client = boto3.client('s3')
        self._set_prefixes()

        self._s3_cache_abspath = get_config_value(
            "DTOOL_S3_CACHE_DIRECTORY",
            config_path=config_path,
            default=os.path.expanduser("~/.cache/dtool/s3")
        )

    def _set_prefixes(self):

        def generate_key(structure_dict_key):
            return self.uuid + '/' + _STRUCTURE_PARAMETERS[structure_dict_key]

        def generate_key_prefix(structure_dict_key):
            return generate_key(structure_dict_key) + '/'

        self.data_key_prefix = generate_key_prefix("data_key_infix")
        self.fragments_key_prefix = generate_key_prefix("fragment_key_infix")
        self.overlays_key_prefix = generate_key_prefix("overlays_key_infix")

        self.admin_metadata_key = generate_key("admin_metadata_key_suffix")
        self.dtool_readme_key = generate_key("dtool_readme_key_suffix")
        self.dataset_readme_key = generate_key("dataset_readme_key_suffix")
        self.manifest_key = generate_key("manifest_key_suffix")
        self.structure_key = generate_key("structure_key_suffix")

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

        dataset_registration_key = 'dtool-{}'.format(self.uuid)
        self.s3resource.Object(self.bucket, dataset_registration_key).put(
            Body=''
        )

        # Write out self descriptive metadata.
        _STRUCTURE_PARAMETERS["dataset_registration_key"] = dataset_registration_key  # NOQA
        self.s3resource.Object(self.bucket, self.structure_key).put(
            Body=json.dumps(_STRUCTURE_PARAMETERS)
        )
        self.s3resource.Object(self.bucket, self.dtool_readme_key).put(
            Body=_DTOOL_README_TXT
        )

    def put_admin_metadata(self, admin_metadata):

        for k, v in admin_metadata.items():
            admin_metadata[k] = str(v)

        self.s3resource.Object(self.bucket, self.admin_metadata_key).put(
            Body='dtoolfile',
            Metadata=admin_metadata
        )

    def get_admin_metadata(self):

        response = self.s3resource.Object(
            self.bucket,
            self.admin_metadata_key
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
            self.dataset_readme_key
        ).get()

        return response['Body'].read().decode('utf-8')

    def put_overlay(self, overlay_name, overlay):
        """Store the overlay by writing it to S3.

        It is the client's responsibility to ensure that the overlay provided
        is a dictionary with valid contents.

        :param overlay_name: name of the overlay
        :overlay: overlay dictionary
        """
        bucket_fpath = os.path.join(
            self.overlays_key_prefix,
            overlay_name + '.json'
        )
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
            self.manifest_key
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

        bucket_fpath = self.data_key_prefix + identifier
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
        for obj in bucket.objects.filter(
            Prefix=self.overlays_key_prefix
        ).all():

            overlay_file = obj.key.rsplit('/', 1)[-1]
            overlay_name, ext = overlay_file.split('.')
            overlay_names.append(overlay_name)

        return overlay_names

    def get_overlay(self, overlay_name):
        """Return overlay as a dictionary.

        :param overlay_name: name of the overlay
        :returns: overlay as a dictionary
        """

        overlay_fpath = self.overlays_key_prefix + overlay_name + '.json'

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

        self.s3resource.Object(self.bucket, self.dataset_readme_key).put(
            Body=content
        )

    def put_item(self, fpath, relpath):

        fname = generate_identifier(relpath)
        dest_path = self.data_key_prefix + fname
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

        identifier = generate_identifier(handle)
        suffix = '{}.{}.json'.format(identifier, key)
        bucket_fpath = self.fragments_key_prefix + suffix

        self.s3resource.Object(self.bucket, bucket_fpath).put(
            Body=json.dumps(value)
        )

    def put_manifest(self, manifest):
        """Store the manifest by writing it to S3.

        It is the client's responsibility to ensure that the manifest provided
        is a dictionary with valid contents.

        :param manifest: dictionary with manifest structural metadata
        """

        self.s3resource.Object(self.bucket, self.manifest_key).put(
            Body=json.dumps(manifest)
        )

    def iter_item_handles(self):
        """Return iterator over item handles."""

        bucket = self.s3resource.Bucket(self.bucket)

        for obj in bucket.objects.filter(Prefix=self.data_key_prefix).all():
            relpath = obj.get()['Metadata']['handle']

            yield relpath

    def item_properties(self, handle):
        """Return properties of the item with the given handle."""

        identifier = generate_identifier(handle)
        bucket_fpath = self.data_key_prefix + identifier
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
