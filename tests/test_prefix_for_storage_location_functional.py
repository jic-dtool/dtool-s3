"""Test the storage prefix code."""

from . import tmp_env_var, S3_TEST_BASE_URI, _remove_dataset


def _prefix_contains_something(storage_broker, prefix):
    bucket = storage_broker.s3resource.Bucket(storage_broker.bucket)
    prefix_objects = list(
        bucket.objects.filter(Prefix=prefix).all()
    )
    return len(prefix_objects) > 0


def test_prefix_functional():  # NOQA

    from dtoolcore import DataSetCreator
    from dtoolcore import DataSet, iter_datasets_in_base_uri
    from dtoolcore.utils import generous_parse_uri

    parse_result = generous_parse_uri(S3_TEST_BASE_URI)
    bucket = parse_result.netloc

    # Create a minimal dataset without a prefix
    with tmp_env_var("DTOOL_S3_DATASET_PREFIX_{}".format(bucket), ""):
        with DataSetCreator("no-prefix", S3_TEST_BASE_URI) as ds_creator:
            ds_creator.put_annotation("prefix", "no")
            no_prefix_uri = ds_creator.uri

    dataset_no_prefix = DataSet.from_uri(no_prefix_uri)

    # Basic test that retrieval works.
    assert dataset_no_prefix.get_annotation("prefix") == "no"

    # Basic test that prefix is correct.
    structure_key = dataset_no_prefix._storage_broker.get_structure_key()
    assert structure_key.startswith(dataset_no_prefix.uuid)

    # Create a minimal dataset
    prefix = "u/olssont/"
    with tmp_env_var(
            "DTOOL_S3_DATASET_PREFIX_{}".format(bucket), prefix):
        with DataSetCreator("no-prefix", S3_TEST_BASE_URI) as ds_creator:
            ds_creator.put_annotation("prefix", "yes")
            prefix_uri = ds_creator.uri

    dataset_with_prefix = DataSet.from_uri(prefix_uri)

    # Basic test that retrieval works.
    assert dataset_with_prefix.get_annotation("prefix") == "yes"

    # Basic test that prefix is correct.
    structure_key = dataset_with_prefix._storage_broker.get_structure_key()
    assert structure_key.startswith(prefix)

    # Basic tests that everything can be picked up.
    dataset_uris = list(
        ds.uri for ds in
        iter_datasets_in_base_uri(S3_TEST_BASE_URI)
    )
    assert dataset_no_prefix.uri in dataset_uris
    assert dataset_with_prefix.uri in dataset_uris

    _remove_dataset(dataset_no_prefix.uri)
    _remove_dataset(dataset_with_prefix.uri)
