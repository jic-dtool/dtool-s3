import pytest

from . import tmp_uuid_and_uri  # NOQA
from . import (
    S3_TEST_BASE_URI,
    S3_TEST_ACCESS_KEY_ID,
    S3_TEST_SECRET_ACCESS_KEY,
)
from . import tmp_env_var

def test_fails_if_only_endpoint_is_set(tmp_uuid_and_uri):  # NOQA

    from dtoolcore import ProtoDataSet, generate_admin_metadata

    bucket_name = S3_TEST_BASE_URI[5:]
    endpoint_key = "DTOOL_S3_ENDPOINT_{}".format(bucket_name)

    uuid, dest_uri = tmp_uuid_and_uri
    name = "my_dataset"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid

    with tmp_env_var(endpoint_key, "https://s3.amazonaws.com"):
        with pytest.raises(RuntimeError):
            ProtoDataSet(dest_uri, admin_metadata)

def test_fails_if_any_endpoint_is_missing(tmp_uuid_and_uri):  # NOQA

    from dtoolcore import ProtoDataSet, generate_admin_metadata

    bucket_name = S3_TEST_BASE_URI[5:]
    endpoint_key = "DTOOL_S3_ENDPOINT_{}".format(bucket_name)
    access_key = "DTOOL_S3_ACCESS_KEY_ID_{}".format(bucket_name)
    secret_access_key = "DTOOL_S3_SECRET_ACCESS_KEY_{}".format(bucket_name)

    uuid, dest_uri = tmp_uuid_and_uri
    name = "my_dataset"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid

    env_vars = {
        endpoint_key: "https://s3.amazonaws.com",
        access_key: S3_TEST_ACCESS_KEY_ID,
        secret_access_key: S3_TEST_SECRET_ACCESS_KEY,
    }

    from itertools import combinations
    for a, b in combinations(env_vars.keys(), 2):
        with tmp_env_var(a, env_vars[a]):
            with tmp_env_var(b, env_vars[b]):
                with pytest.raises(RuntimeError):
                    ProtoDataSet(dest_uri, admin_metadata)


def test_works_if_all_set(tmp_uuid_and_uri):  # NOQA

    from dtoolcore import ProtoDataSet, generate_admin_metadata

    bucket_name = S3_TEST_BASE_URI[5:]
    endpoint_key = "DTOOL_S3_ENDPOINT_{}".format(bucket_name)
    access_key = "DTOOL_S3_ACCESS_KEY_ID_{}".format(bucket_name)
    secret_access_key = "DTOOL_S3_SECRET_ACCESS_KEY_{}".format(bucket_name)

    uuid, dest_uri = tmp_uuid_and_uri
    name = "my_dataset"
    admin_metadata = generate_admin_metadata(name)
    admin_metadata["uuid"] = uuid

    env_vars = {
        endpoint_key: "https://s3.amazonaws.com",
        access_key: S3_TEST_ACCESS_KEY_ID,
        secret_access_key: S3_TEST_SECRET_ACCESS_KEY,
    }

    a, b, c = list(env_vars.keys())
    with tmp_env_var(a, env_vars[a]):
        with tmp_env_var(b, env_vars[b]):
            with tmp_env_var(c, env_vars[c]):
                proto_dataset = ProtoDataSet(dest_uri, admin_metadata)
                proto_dataset.create()
