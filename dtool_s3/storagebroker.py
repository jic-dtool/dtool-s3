import json
import logging
import os
import time
import packaging.version
import random

import base64

try:
    from urlparse import urlunparse
except ImportError:
    from urllib.parse import urlunparse

import boto3
import boto3.exceptions
import botocore.waiter
import botocore.exceptions
from boto3.session import Session
from botocore.errorfactory import ClientError
from botocore.exceptions import EndpointConnectionError

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


logger = logging.getLogger(__name__)

# We update the dataset_registration_key when we have the UUID for the dataset
_STRUCTURE_PARAMETERS = {
    "dataset_registration_key": None,
    "data_key_infix": "data",
    "fragment_key_infix": "fragments",
    "overlays_key_infix": "overlays",
    "annotations_key_infix": "annotations",
    "tags_key_infix": "tags",
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

Where UUID is the unique identifier for the dataset. The contains the local
path to the dataset and is empty if the dataset is stored in the top level
of the bucket. It is used to aid fast enumeration of unique datasets in the
bucket.

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
Dataset key/value pairs metadata prefixed by: $UUID/annotations/
Dataset tags metadata: $UUID/tags/
"""


# Helper functions

def _unicode_to_base64(unicode_str):
    """Convert input string to bytes represented as a string.

    Helper function to deal with the fact that AWS S3 metadata
    cannot deal with non-ascii characters.

    See: https://github.com/jic-dtool/dtool-s3/issues/14
    """
    us2bs = unicode_str.encode("utf-8")
    us2bs = base64.b64encode(us2bs)
    us2bs = str(us2bs)
    us2bs = us2bs[2:-1]
    return us2bs


def _base64_to_unicode(base64_str):
    """Bytes represented as a string to unicode string.

    Helper function to deal with the fact that AWS S3 metadata
    cannot deal with non-ascii characters.

    See: https://github.com/jic-dtool/dtool-s3/issues/14
    """
    bs2us = base64.b64decode(base64_str)
    bs2us = bs2us.decode("utf-8")
    return bs2us


def _object_exists(s3resource, bucket, dest_path):
    """Return object from bucket."""

    try:
        obj = s3resource.Object(bucket, dest_path)
        obj.wait_until_exists(WaiterConfig={'Delay': 5, 'MaxAttempts': 20})
    except botocore.waiter.WaiterError:
        return False
    return True


def _upload_file(s3client, fpath, bucket, dest_path, extra_args):
    """Upload file to S3 bucket."""

    try:
        s3client.upload_file(
            fpath,
            bucket,
            dest_path,
            ExtraArgs=extra_args
        )

    except (s3client.exceptions.NoSuchUpload,
            boto3.exceptions.S3UploadFailedError,
            EndpointConnectionError) as e:
        logger.debug("Upload failed with: " + str(e))
        return False

    return True


def _put_item_with_retry(
    s3client,
    s3resource,
    fpath,
    bucket,
    dest_path,
    extra_args,
    max_retry_time=90,  # this is the maximum value that one iteration can run
                        # for therefore the maximum amount the total function
                        # can execute for is double this value
    retry_seed=random.randint(1, 10),
    retry_time_spent=0,
    retry_attempts=0,
):
    """Robust putting of item into s3 bucket."""
    success = _upload_file(s3client, fpath, bucket, dest_path, extra_args)

    # If file upload did not succeed
    if not success:
        obj = _object_exists(s3resource, bucket, dest_path)  # NOQA

        # If the object does not exist on the remote storage
        if obj is None:

            # If the time spent retrying doesn't exceed the maximum retry time
            if retry_time_spent < max_retry_time:

                # Calculate sleep time as the smaller of:
                #  * the maximum retry value, or
                #  * the larger of the following, to the power of 2:
                #   * the random retry seed, or
                #   * the retry time spent (initially zero)
                sleep_time = min(
                    max_retry_time,
                    (max(retry_time_spent, retry_seed) ** 2)
                )

                time.sleep(sleep_time)

                retry_time_spent += sleep_time
                retry_attempts += 1

                _put_item_with_retry(
                    s3client=s3client,
                    s3resource=s3resource,
                    fpath=fpath,
                    bucket=bucket,
                    dest_path=dest_path,
                    extra_args=extra_args,
                    max_retry_time=max_retry_time,
                    retry_seed=retry_seed,
                    retry_time_spent=retry_time_spent,
                    retry_attempts=retry_attempts
                )

            else:

                error = "Put with retry failed after {} attempts.".format(
                    retry_attempts
                )
                logger.warning(error)
                raise(S3StorageBrokerPutItemError(error))


class S3StorageBrokerPutItemError(RuntimeError):
    pass


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

        self.dataset_prefix = get_config_value(
            "DTOOL_S3_DATASET_PREFIX_{}".format(self.bucket))

        self.uuid = uuid

        self.s3resource, self.s3client, self.unsigned_s3client = \
            self._get_resource_and_client(self.bucket)

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
        self.annotations_key_prefix = self._generate_key_prefix(
            "annotations_key_infix"
        )
        self.tags_key_prefix = self._generate_key_prefix(
            "tags_key_infix"
        )

        self.http_manifest_key = self._generate_key("http_manifest_key")

        self._s3_cache_abspath = get_config_value(
            "DTOOL_CACHE_DIRECTORY",
            config_path=config_path,
            default=DEFAULT_CACHE_PATH
        )

    # Generic helper functions.

    @classmethod
    def _get_resource_and_client(cls, bucket_name):
        # Get S3 endpoint, access key and secret key. Can be left
        # unconfigured, in which case the AWS configuration is used.
        s3_endpoint = get_config_value(
            "DTOOL_S3_ENDPOINT_{}".format(bucket_name)
        )
        s3_access_key_id = get_config_value(
            "DTOOL_S3_ACCESS_KEY_ID_{}".format(bucket_name)
        )
        s3_secret_access_key = get_config_value(
            "DTOOL_S3_SECRET_ACCESS_KEY_{}".format(bucket_name)
        )

        unsigned_config = botocore.client.Config(
            signature_version=botocore.UNSIGNED)

        if (
            s3_endpoint is not None
            or s3_access_key_id is not None
            or s3_secret_access_key is not None
        ):
            # We can only proceed if all three of endpoint, access key id and
            # secret access key are configure
            if (
                not s3_endpoint
                or not s3_access_key_id
                or not s3_secret_access_key
            ):
                raise RuntimeError(
                    "If you want to configure your S3 endpoint for bucket "
                    "'{bucket}' via the dtool config file, please set "
                    "DTOOL_S3_ENDPOINT_{bucket}, "
                    "DTOOL_S3_ACCESS_KEY_ID_{bucket} and "
                    "DTOOL_S3_SECRET_ACCESS_KEY_{bucket}."
                    .format(bucket=bucket_name))

            session = Session(
                aws_access_key_id=s3_access_key_id,
                aws_secret_access_key=s3_secret_access_key
            )

            s3resource = session.resource(
                's3',
                endpoint_url=s3_endpoint
            )
            s3client = session.client(
                's3',
                endpoint_url=s3_endpoint
            )
            unsigned_s3client = boto3.client(
                's3',
                endpoint_url=s3_endpoint,
                config=unsigned_config
            )
        else:
            s3resource = boto3.resource('s3')
            s3client = boto3.client('s3')
            unsigned_s3client = boto3.client('s3', config=unsigned_config)

        return s3resource, s3client, unsigned_s3client

    def _get_upload_storage_broker_version(self):
        """Return version of dtool-s3 storage broker used to upload the dataset.

        In cases of old datasets (prior to version 0.4.0) this information is
        not available on the dataset. In these cases this method returns None.
        """
        sb_version = None
        structure_key = self.get_structure_key()
        if _object_exists(self.s3resource, self.bucket, structure_key):
            structure_parameters_txt = self.get_text(structure_key)
            structure_parameters = json.loads(structure_parameters_txt)
            if "storage_broker_version" in structure_parameters:
                sb_version = structure_parameters["storage_broker_version"]
        return sb_version

    def _get_prefix(self):
        if not hasattr(self, '_prefix'):
            # Load prefix only if it does not exist
            try:
                rkey = self.s3resource.Object(
                    self.bucket, self.dataset_registration_key).get()
                # If the registration key exists, we use the prefix stored
                # there
                self._prefix = rkey['Body'].read().decode()
                logger.debug(
                    'Prefix from registration key: {}'
                    .format(self._prefix)
                )
            except botocore.exceptions.ClientError:
                # If the registration key does not exist, we use the
                # configured prefix
                self._prefix = '' if self.dataset_prefix is None \
                    else self.dataset_prefix
                logger.debug(
                    'Prefix from configuration: {}'
                    .format(self._prefix)
                )
        return self._prefix

    def _generate_key(self, structure_dict_key):
        prefix = self._get_prefix()
        return prefix + self.uuid + '/' + self._structure_parameters[structure_dict_key]  # NOQA

    def _generate_key_prefix(self, structure_dict_key):
        return self._generate_key(structure_dict_key) + '/'

    def _get_item_object(self, handle):
        identifier = generate_identifier(handle)
        item_key = self.data_key_prefix + identifier
        obj = self.s3resource.Object(self.bucket, item_key)
        return obj

    # Class methods to override.

    @classmethod
    def list_dataset_uris(cls, base_uri, config_path):
        """Return list containing URIs with base URI."""
        uri_list = []

        parse_result = generous_parse_uri(base_uri)
        bucket_name = parse_result.netloc
        resource, _, _ = cls._get_resource_and_client(bucket_name)
        bucket = resource.Bucket(bucket_name)

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
            Body='' if self.dataset_prefix is None else self.dataset_prefix
        )

    def put_text(self, key, content):
        logger.debug("Put text {}".format(self))
        self.s3resource.Object(self.bucket, key).put(
            Body=content
        )

    def get_text(self, key):
        logger.debug("Get text {}".format(self))

        response = self.s3resource.Object(
            self.bucket,
            key
        ).get()

        return response['Body'].read().decode('utf-8')

    def delete_key(self, key):
        logger.debug("Delete key {} {}".format(key, self))

        self.s3resource.Object(self.bucket, key).delete()

    def get_structure_key(self):
        return self._generate_key("structure_key_suffix")

    def get_dtool_readme_key(self):
        return self._generate_key("dtool_readme_key_suffix")

    def get_readme_key(self):
        return self._generate_key("dataset_readme_key_suffix")

    def get_overlay_key(self, overlay_name):
        return os.path.join(self.overlays_key_prefix, overlay_name + '.json')

    def get_annotation_key(self, annotation_name):
        return os.path.join(
            self.annotations_key_prefix,
            annotation_name + '.json'
        )

    def get_tag_key(self, tag):
        return os.path.join(
            self.tags_key_prefix,
            tag
        )

    def get_manifest_key(self):
        return self._generate_key("manifest_key_suffix")

    def get_admin_metadata_key(self):
        return self._generate_key("admin_metadata_key_suffix")

    def put_admin_metadata(self, admin_metadata):
        logger.debug("Put admin metdata {}".format(self))

        str_admin_metadata = {}
        for k, v in admin_metadata.items():
            str_admin_metadata[k] = str(v)

        self.s3resource.Object(self.bucket, self.get_admin_metadata_key()).put(
            Body=json.dumps(admin_metadata),
            Metadata=str_admin_metadata
        )

    def get_admin_metadata(self):
        logger.debug("Get admin metdata {}".format(self))

        response = self.s3resource.Object(
            self.bucket,
            self.get_admin_metadata_key()
        ).get()

        admin_metadata = response['Metadata']
        # s3-native metadata comes as str only, convert timestamps
        # back to float:
        if "frozen_at" in admin_metadata:
            admin_metadata["frozen_at"] = float(admin_metadata["frozen_at"])
        if "created_at" in admin_metadata:
            admin_metadata["created_at"] = float(admin_metadata["created_at"])
        return admin_metadata

    def get_size_in_bytes(self, handle):
        logger.debug("Get size in bytes {}".format(self))
        obj = self._get_item_object(handle)
        return int(obj.content_length)

    def get_utc_timestamp(self, handle):
        logger.debug("Get utc timestamp {}".format(self))
        obj = self._get_item_object(handle)
        return time.mktime(obj.last_modified.timetuple())

    def get_hash(self, handle):
        logger.debug("Get hash {}".format(self))

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
        logger.debug("Has admin metadata {}".format(self))

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
        logger.debug("Get item abspath {} {}".format(identifier, self))

        if not hasattr(self, "_admin_metadata_cache"):
            self._admin_metadata_cache = self.get_admin_metadata()
        admin_metadata = self._admin_metadata_cache

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
        logger.debug("List overlay names {}".format(self))

        bucket = self.s3resource.Bucket(self.bucket)

        overlay_names = []
        for obj in bucket.objects.filter(
            Prefix=self.overlays_key_prefix
        ).all():

            overlay_file = obj.key.rsplit('/', 1)[-1]
            overlay_name, ext = overlay_file.split('.')
            overlay_names.append(overlay_name)

        return overlay_names

    def list_annotation_names(self):
        """Return list of annotation names."""
        logger.debug("List annotation names {}".format(self))

        bucket = self.s3resource.Bucket(self.bucket)

        annotation_names = []
        for obj in bucket.objects.filter(
            Prefix=self.annotations_key_prefix
        ).all():

            annotation_file = obj.key.rsplit('/', 1)[-1]
            annotation_name, ext = annotation_file.split('.')
            annotation_names.append(annotation_name)

        return annotation_names

    def list_tags(self):
        """Return list of tags."""
        logger.debug("List tags {}".format(self))

        bucket = self.s3resource.Bucket(self.bucket)

        tags = []
        for obj in bucket.objects.filter(
            Prefix=self.tags_key_prefix
        ).all():

            tag = obj.key.rsplit('/', 1)[-1]
            tags.append(tag)

        return tags

    def put_item(self, fpath, relpath):
        logger.debug("Put item {}".format(self))

        # Here the MD5 checksum is calculated so that it can be uploaded with
        # the item as a piece of metadata. This is needed as the AWS etag is
        # not the md5 sum of the uploaded object for items that are uploaded
        # using multipart uploads (large files).
        # See: https://stackoverflow.com/a/43067788
        checksum = S3StorageBroker.hasher(fpath)

        fname = generate_identifier(relpath)
        dest_path = self.data_key_prefix + fname

        # _unicode_to_base64 used to deal with relpaths that include non-ascii chars.  # NOQA
        extra_args = {
            'Metadata': {
                'handle': _unicode_to_base64(relpath),
                'checksum': checksum,
            }
        }
        _put_item_with_retry(
            s3client=self.s3client,
            s3resource=self.s3resource,
            fpath=fpath,
            bucket=self.bucket,
            dest_path=dest_path,
            extra_args=extra_args
        )

        return relpath

    def add_item_metadata(self, handle, key, value):
        """Store the given key:value pair for the item associated with handle.

        :param handle: handle for accessing an item before the dataset is
                       frozen
        :param key: metadata key
        :param value: metadata value
        """
        logger.debug("Add item metadata {}".format(self))

        identifier = generate_identifier(handle)
        suffix = '{}.{}.json'.format(identifier, key)
        bucket_fpath = self.fragments_key_prefix + suffix

        self.s3resource.Object(self.bucket, bucket_fpath).put(
            Body=json.dumps(value)
        )

    def iter_item_handles(self):
        """Return iterator over item handles."""
        logger.debug("Iter item handles {}".format(self))

        bucket = self.s3resource.Bucket(self.bucket)

        # Older dtool-s3 datasets, prior to version 0.14.0, use handles that
        # represent the relative path of the file. These did not support
        # non-ascii charcters. Since 0.14.0 dtool-s3 datasets therefore use
        # base64 encoded versions of the relative paths as handles. This means
        # that we need to work out if the handle is base64 encoded or not to
        # handle each case appropriately.
        base64_encoded = True
        sb_version = self._get_upload_storage_broker_version()
        if sb_version is not None:
            if packaging.version.parse(sb_version) < packaging.version.parse("0.14.0"):  # NOQA
                base64_encoded = False
        else:
            base64_encoded = False

        for obj in bucket.objects.filter(Prefix=self.data_key_prefix).all():
            handle = obj.get()['Metadata']['handle']

            # The handle is a base64 encoded version of the relpath in order
            # to deal with non-ascii chars in the relpath. We therefore need
            # to decode it to get the actual relpath.
            if base64_encoded:
                yield _base64_to_unicode(handle)
            else:
                yield handle

    def pre_freeze_hook(self):
        pass

    def post_freeze_hook(self):
        logger.debug("Post freeze hook {}".format(self))

        # Delete the temporary fragment metadata objects from the bucket.

        # Get the keys of the fragment metadata objects.
        bucket = self.s3resource.Bucket(self.bucket)
        prefix_object_keys = [
            obj.key for obj in
            bucket.objects.filter(Prefix=self.fragments_key_prefix).all()
        ]

        def _chunks(l, n):  # NOQA
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
        logger.debug("Get item metadata {}".format(self))

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
    def _create_presigned_url(self, object_name, expiration):
        """Generate a presigned URL to share an S3 object

        :param object_name: string
        :param expiration: Time in seconds for the presigned URL to remain valid
        :return: Presigned URL as string. If error, returns None.
        """  # NOQA

        # Generate a presigned URL for the S3 object
        params = {
            "Bucket": self.bucket,
            "Key": object_name
        }
        try:
            url = self.s3client.generate_presigned_url(
                "get_object",
                Params=params,
                ExpiresIn=expiration
            )
        except botocore.exceptions.ClientError as e:
            logger.error(e)
            return None

        # The response contains the presigned URL
        return url

    def _url(self, key):
        return self.unsigned_s3client.generate_presigned_url(
            'get_object',
            ExpiresIn=0,
            Params={'Bucket': self.bucket,
                    'Key': key})

    def _make_key_public_noexpiry(self, key):
        acl = self.s3resource.ObjectAcl(self.bucket, key)
        acl.put(ACL='public-read')

        return self._url(key)

    def _generate_key_url(self, key, expiry):

        if expiry is None or expiry == "":
            url = self._make_key_public_noexpiry(key)
        else:
            url = self._create_presigned_url(key, expiry)

        return url

    def _generate_http_manifest(self, expiry):

        readme_url = self._generate_key_url(self.get_readme_key(), expiry)
        manifest_url = self._generate_key_url(self.get_manifest_key(), expiry)

        overlays = {}
        for overlay_name in self.list_overlay_names():
            overlay_fpath = self.overlays_key_prefix + overlay_name + '.json'
            overlays[overlay_name] = self._generate_key_url(overlay_fpath, expiry)  # NOQA

        annotations = {}
        for ann_name in self.list_annotation_names():
            ann_fpath = self.annotations_key_prefix + ann_name + '.json'
            annotations[ann_name] = self._generate_key_url(ann_fpath, expiry)

        tags = self.list_tags()

        manifest = self.get_manifest()
        item_urls = {}
        for identifier in manifest["items"]:
            item_urls[identifier] = self._generate_key_url(
                self.data_key_prefix + identifier,
                expiry
            )

        http_manifest = {
            "admin_metadata": self.get_admin_metadata(),
            "item_urls": item_urls,
            "overlays": overlays,
            "annotations": annotations,
            "tags": tags,
            "readme_url": readme_url,
            "manifest_url": manifest_url
        }

        return http_manifest

    def _write_http_manifest(self, http_manifest, expiry):

        self.s3resource.Object(self.bucket, self.http_manifest_key).put(
            Body=json.dumps(http_manifest, indent=2)
        )

        return self._generate_key_url(self.http_manifest_key, expiry)

    def http_enable(self):
        logger.debug("HTTP enable {}".format(self))

        expiry = get_config_value("DTOOL_S3_PUBLISH_EXPIRY")
        if expiry is not None:
            if expiry != "":
                try:
                    expiry = int(expiry)
                except ValueError:
                    logger.error("DTOOL_S3_PUBLISH_EXPIRY must be set to a value that can be converted to an integer")  # NOQA
                    raise(RuntimeError())
        http_manifest = self._generate_http_manifest(expiry)
        manifest_url = self._write_http_manifest(http_manifest, expiry)  # NOQA

        manifest_presignature = None

        if "?" in manifest_url:
            _, manifest_presignature = manifest_url.split("?")

        access_url = self._url(self._get_prefix() + self.uuid)
        if manifest_presignature is not None:
            access_url = access_url + "?" + manifest_presignature

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
