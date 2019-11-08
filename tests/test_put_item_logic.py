"""Test the more robust put_item logic."""

import pytest

from . import tmp_dir_fixture  # NOQA

try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock


def test_get_object_failure():
    """
    Mock scenario where the get fails.
    """

    from botocore.exceptions import WaiterError
    from dtool_s3.storagebroker import _object_exists

    mock_s3resource = MagicMock()
    obj = MagicMock()
    obj.wait_until_exists = MagicMock(side_effect=WaiterError(
        'ObjectExists', 'Max attempts exceeded', {}))
    mock_s3resource.Object = MagicMock(return_value=obj)

    value = _object_exists(
        mock_s3resource,
        "dummy_bucket",
        "dummy_dest_path"
    )

    assert value is False


def test_get_object_success():
    """
    Mock scenario where the get succeeds.
    """

    from dtool_s3.storagebroker import _object_exists

    mock_s3resource = MagicMock()
    obj = MagicMock()
    obj.wait_until_exists = MagicMock()
    mock_s3resource.Object = MagicMock(return_value=obj)

    value = _object_exists(
        mock_s3resource,
        "dummy_bucket",
        "dummy_dest_path"
    )

    obj.wait_until_exists.assert_called_once()
    assert value is True


def test_upload_file_simulating_successful_upload():
    """
    Mock scenario where upload simply succeeds.
    """

    from dtool_s3.storagebroker import _upload_file  # NOQA

    s3client = MagicMock()
    s3client.upload_file = MagicMock(return_value=True)

    value = _upload_file(
        s3client,
        "dummy_fpath",
        "dummy_bucket",
        "dummy_dest_path",
        "dummy_extra_args"
    )

    assert value is True


def test_upload_file_simulating_nosuchupload_failure(tmp_dir_fixture):  # NOQA
    """
    Mock scenario where upload fails with a NoSuchUpload exception.
    """

    from dtool_s3.storagebroker import _upload_file  # NOQA
    import boto3

    error_response = {'Error': {'Code': 'NoSuchUpload',
                                'Message': 'The specified multipart upload ' +
                                'does not exist. The upload ID might be ' +
                                'invalid, or the multipart upload might ' +
                                'have been aborted or completed.'}}

    s3client = boto3.client("s3")
    s3client.upload_file = MagicMock(
        side_effect=s3client.exceptions.NoSuchUpload(
            error_response,
            "AbortMultipartUpload")
    )

    value = _upload_file(
        s3client,
        "dummy_fpath",
        "dummy_bucket",
        "dummy_dest_path",
        "dummy_extra_args",
    )

    assert value is False


def test_upload_file_simulating_endpointconnectionerror(tmp_dir_fixture):  # NOQA
    """
    Mock scenario where upload fails with a EndpointConnectionError exception.
    """

    from dtool_s3.storagebroker import _upload_file  # NOQA
    import boto3
    from botocore.exceptions import EndpointConnectionError

    s3client = boto3.client("s3")
    s3client.upload_file = MagicMock(
        side_effect=EndpointConnectionError(
            endpoint_url="dummy_bucket/dest_path")
    )

    value = _upload_file(
        s3client,
        "dummy_fpath",
        "dummy_bucket",
        "dummy_dest_path",
        "dummy_extra_args",
    )

    assert value is False


def test_upload_file_simulating_S3UploadFailedError(tmp_dir_fixture):  # NOQA
    """
    Mock scenario where upload fails with a S3UploadFailedError exception.
    """

    from dtool_s3.storagebroker import _upload_file  # NOQA
    import boto3
    from boto3.exceptions import S3UploadFailedError

    s3client = boto3.client("s3")
    s3client.upload_file = MagicMock(
        side_effect=S3UploadFailedError()
    )

    value = _upload_file(
        s3client,
        "dummy_fpath",
        "dummy_bucket",
        "dummy_dest_path",
        "dummy_extra_args",
    )

    assert value is False


def test_put_item_with_retry():
    from dtool_s3.storagebroker import _put_item_with_retry  # NOQA


def test_put_item_with_retry_immediate_success():
    """
    Mock scenario where while doing a put, the upload succeeds without needing
    to retry.
    """

    import dtool_s3.storagebroker

    dtool_s3.storagebroker._upload_file = MagicMock(return_value=True)
    dtool_s3.storagebroker._object_exists = MagicMock()

    dtool_s3.storagebroker._put_item_with_retry(
        "dummy_s3client",
        "dummy_s3resource",
        "dummy_fpath",
        "dummy_bucket",
        "dummy_dest_path",
        {}
    )
    dtool_s3.storagebroker._upload_file.assert_called()
    dtool_s3.storagebroker._object_exists.assert_not_called()


def test_put_item_with_retry_simulating_upload_error_item_uploaded():
    """
    Mock scenario where while doing a put, the upload fails with an ambiguous
    failure, however item has been successfully created in the bucket.
    """

    import dtool_s3.storagebroker

    dtool_s3.storagebroker._upload_file = MagicMock(return_value=False)
    dtool_s3.storagebroker._object_exists = MagicMock(return_value=True)

    dtool_s3.storagebroker._put_item_with_retry(
        "dummy_s3client",
        "dummy_s3resource",
        "dummy_fpath",
        "dummy_bucket",
        "dummy_dest_path",
        {}
    )

    dtool_s3.storagebroker._upload_file.assert_called_once()
    dtool_s3.storagebroker._object_exists.assert_called_once()


def test_put_item_with_retry_simulating_upload_error_item_doesnt_exist():
    """
    Mock scenario where while doing a put, the upload fails, the object hasn't
    been created on the target, so the retry routine is engaged.
    """

    import dtool_s3.storagebroker

    max_retry_time = 10

    dtool_s3.storagebroker._upload_file = MagicMock(return_value=False)
    dtool_s3.storagebroker._object_exists = MagicMock(return_value=None)
    dtool_s3.storagebroker._put_item_with_retry = MagicMock(
        side_effect=dtool_s3.storagebroker._put_item_with_retry)

    with pytest.raises(dtool_s3.storagebroker.S3StorageBrokerPutItemError):
        dtool_s3.storagebroker._put_item_with_retry(
            s3client="dummy_s3client",
            s3resource="dummy_s3resource",
            fpath="dummy_fpath",
            bucket="dummy_bucket",
            dest_path="dummy_dest_path",
            extra_args={},
            max_retry_time=max_retry_time
        )

    assert dtool_s3.storagebroker._put_item_with_retry.call_count > 1
    my_args = dtool_s3.storagebroker._put_item_with_retry.call_args
    args, kwargs = my_args
    assert kwargs['retry_time_spent'] >= max_retry_time
