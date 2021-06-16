"""Test the storage prefix code."""

import pytest

from . import tmp_uuid_and_uri  # NOQA
from . import tmp_env_var


def _prefix_contains_something(storage_broker, prefix):
    bucket = storage_broker.s3resource.Bucket(storage_broker.bucket)
    prefix_objects = list(
        bucket.objects.filter(Prefix=prefix).all()
    )
    return len(prefix_objects) > 0


def test_prefix_functional(tmp_uuid_and_uri):  # NOQA

    uuid, dest_uri = tmp_uuid_and_uri

    from dtoolcore import ProtoDataSet, generate_admin_metadata
    from dtoolcore import DataSet

    name = "my_dataset"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid

    # Create a minimal dataset
    prefix = "u/olssont/"
    with tmp_env_var("DTOOL_S3_DATASET_PREFIX", prefix):
        proto_dataset = ProtoDataSet(
            uri=dest_uri,
            admin_metadata=admin_metadata,
            config_path=None)
        assert proto_dataset._storage_broker.get_structure_key().startswith(prefix)
