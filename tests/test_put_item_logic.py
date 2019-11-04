"""Test the more robust put_item logic."""

from . import tmp_dir_fixture  # NOQA

try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock


def test_get_object():
    from dtool_s3.storagebroker import _get_object  # NOQA


def test_upload_file_simulating_successful_upload():
    from dtool_s3.storagebroker import _upload_file  # NOQA

    # Mock scenario where upload succeeds without need for retry.
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


def test_put_item_with_retry():
    from dtool_s3.storagebroker import _put_item_with_retry  # NOQA


def test_put_item_with_retry_immediate_success():
    import dtool_s3.storagebroker

    # Mock scenario where upload succeeds without need for retry.
    dtool_s3.storagebroker._upload_file = MagicMock(return_value=True)
    dtool_s3.storagebroker._get_object = MagicMock()

    dtool_s3.storagebroker._put_item_with_retry(
        "dummy_s3client",
        "dummy_fpath",
        "dummy_bucket",
        "dummy_dest_path",
        {},
        90
    )
    dtool_s3.storagebroker._upload_file.assert_called()
    dtool_s3.storagebroker._get_object.assert_not_called()


def test_put_item_with_retry_simulating_upload_error_item_uploaded():
    import dtool_s3.storagebroker

    # Mock scenario where upload fails, and retruns ambigious failure
    # MultipartUploadError, but item has been created in the bucket.
    dtool_s3.storagebroker._upload_file = MagicMock(return_value=False)
    dtool_s3.storagebroker._get_object = MagicMock(return_value=True)

    dtool_s3.storagebroker._put_item_with_retry(
        "dummy_s3client",
        "dummy_fpath",
        "dummy_bucket",
        "dummy_dest_path",
        {},
        90
    )

    dtool_s3.storagebroker._upload_file.assert_called_once()
    dtool_s3.storagebroker._get_object.assert_called_once()


def test_put_item_with_retry_simulating_upload_error_item_doesnt_exist():
    import dtool_s3.storagebroker

    max_retry = 90

    # Mock scenario where upload fails and retry occurs
    dtool_s3.storagebroker._upload_file = MagicMock(return_value=False)
    dtool_s3.storagebroker._get_object = MagicMock(return_value=None)
    dtool_s3.storagebroker._put_item_with_retry = MagicMock(
        side_effect=dtool_s3.storagebroker._put_item_with_retry)

    dtool_s3.storagebroker._put_item_with_retry(
        s3client="dummy_s3client",
        fpath="dummy_fpath",
        bucket="dummy_bucket",
        dest_path="dummy_dest_path",
        extra_args={},
        max_retry=max_retry
    )

    assert dtool_s3.storagebroker._put_item_with_retry.call_count > 1
    my_args = dtool_s3.storagebroker._put_item_with_retry.call_args
    args, kwargs = my_args
    assert kwargs['retry_time_spent'] >= max_retry
