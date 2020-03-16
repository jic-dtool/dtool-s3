
from contextlib import contextmanager
import os
import json
import shutil
import tempfile

import pytest

from dtoolcore import generate_admin_metadata
from dtool_s3.storagebroker import (
    S3StorageBroker,
)

_HERE = os.path.dirname(__file__)
TEST_SAMPLE_DATA = os.path.join(_HERE, "data")

S3_TEST_BASE_URI = os.getenv("S3_TEST_BASE_URI", "s3://test-dtool-s3-bucket")


@contextmanager
def tmp_env_var(key, value):
    os.environ[key] = value
    yield
    del os.environ[key]


@contextmanager
def tmp_directory():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)


@pytest.fixture
def tmp_dir_fixture(request):
    d = tempfile.mkdtemp()

    @request.addfinalizer
    def teardown():
        shutil.rmtree(d)

    return d


def _key_exists_in_storage_broker(storage_broker, key):

    bucket = storage_broker.s3resource.Bucket(storage_broker.bucket)

    objs = list(bucket.objects.filter(Prefix=key))
    if len(objs) > 0 and objs[0].key == key:
        return True
    else:
        return False


def _all_objects_in_storage_broker(storage_broker):

    bucket = storage_broker.s3resource.Bucket(storage_broker.bucket)

    for obj in bucket.objects.filter(Prefix=storage_broker.uuid).all():
        yield obj.key

    registration_key = "dtool-{}".format(storage_broker.uuid)
    yield registration_key


def _get_data_structure_from_key(storage_broker, key):

    response = storage_broker.s3resource.Object(
        storage_broker.bucket,
        key
    ).get()

    content_as_string = response['Body'].read().decode('utf-8')
    return json.loads(content_as_string)


def _get_unicode_from_key(storage_broker, key):

    response = storage_broker.s3resource.Object(
        storage_broker.bucket,
        key
    ).get()

    content_as_string = response['Body'].read().decode('utf-8')

    return content_as_string


def _chunks(l, n):
    """Yield successive n-sized chunks from l."""

    for i in range(0, len(l), n):
        yield l[i:i + n]


def _remove_dataset(uri):

    storage_broker = S3StorageBroker(uri)

    object_key_iterator = _all_objects_in_storage_broker(storage_broker)

    object_key_list = list(object_key_iterator)

    bucket = storage_broker.s3resource.Bucket(storage_broker.bucket)
    # Max objects to delete in one API call is 1000, we'll do 500 for safety
    for keys in _chunks(object_key_list, 500):
        keys_as_list_of_dicts = [{'Key': k} for k in keys]
        bucket.objects.delete(
            Delete={'Objects': keys_as_list_of_dicts}
        )


@pytest.fixture
def tmp_uuid_and_uri(request):
    admin_metadata = generate_admin_metadata("test_dataset")
    uuid = admin_metadata["uuid"]

    uri = S3StorageBroker.generate_uri(
        "test_dataset",
        uuid,
        S3_TEST_BASE_URI,
    )

    @request.addfinalizer
    def teardown():
        _remove_dataset(uri)

    return (uuid, uri)
