
import pytest

from dtoolcore import generate_admin_metadata
from dtool_s3.storagebroker import (
    S3StorageBroker,
)


def _all_objects_in_storage_broker(storage_broker):

    bucket = storage_broker.s3resource.Bucket(storage_broker.bucket)

    for obj in bucket.objects.filter(Prefix=storage_broker.uuid).all():
        yield obj.key


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
        response = bucket.objects.delete(
            Delete={'Objects': keys_as_list_of_dicts}
        )


@pytest.fixture
def tmp_uuid_and_uri(request):
    admin_metadata = generate_admin_metadata("test_dataset")
    uuid = admin_metadata["uuid"]

    uri = S3StorageBroker.generate_uri("test_dataset", uuid, "s3://test-dtool-s3-bucket")

    @request.addfinalizer
    def teardown():
        _remove_dataset(uri)

    return (uuid, uri)
