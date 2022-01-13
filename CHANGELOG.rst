CHANGELOG
=========

This project uses `semantic versioning <http://semver.org/>`_.
This change log uses principles from `keep a changelog <http://keepachangelog.com/>`_.

[Unreleased]
------------

Added
^^^^^


Changed
^^^^^^^


Deprecated
^^^^^^^^^^


Removed
^^^^^^^


Fixed
^^^^^


Security
^^^^^^^^


[0.13.0] - 2022-01-13
---------------------

Added
^^^^^

- Support for presigned URLs when using ``dtool publish``. To enable this feature one needs
  to set the ``DTOOL_S3_PUBLISH_EXPIRY`` setting to the number of seconds one wants the
  dataset to be accessible for. 

Fixed
^^^^^

- Fixed long standing issue with ``created_at`` and ``frozen_at``  admin
  metadata being returned as string rather than float.  Many thanks to
  `Johannes L. Hörmann <https://github.com/jotelha>`_ for reporting and fixing.
  See https://github.com/jic-dtool/dtool-s3/pull/13.


[0.12.0] - 2021-08-29
---------------------

Added
^^^^^

- Added ability to specify custom endpoints thanks to `Lars Pastewka
  <https://github.com/pastewka>`_


[0.11.0] - 2021-06-20
---------------------

Added
^^^^^

- Added ability to specify user prefix for object path thanks to `Lars Pastewka
  <https://github.com/pastewka>`_


[0.10.0] - 2020-03-19
---------------------

Added support for tags.

Added
^^^^^

- Added ``dtool_s3.storagebroker.delete_key()`` method
- Added ``dtool_s3.storagebroker.get_tag_key()`` method
- Added ``dtool_s3.storagebroker.list_tags()`` method


[0.9.0] - 2019-11-06
--------------------

Added
^^^^^

- Added ``boto3.exceptions.S3UploadFailedError`` to list of exceptions to
  retry a file upload


[0.8.0] - 2019-11-06
--------------------

Added
^^^^^

- Added more robust logic for retrying interrupted put_item calls in the
  S3StorageBroker thanks to Adam Carrgilson. Code now retries when
  encountering ``botocore.errorfactory.NoSuchUpload`` and
  ``botocore.exceptions.EndpointConnectionError.


[0.7.0] - 2019-10-31
--------------------

Added
^^^^^

- Added support for dataset annotations


[0.6.0] - 2019-07-12
--------------------

Added
^^^^^

- Added debug logging
- Added optimisation to improve speed when copying data from S3 object storage


[0.5.0] - 2019-04-25
--------------------

Changed
^^^^^^^

- Cache environment variable changed from DTOOL_S3_CACHE_DIRECTORY to DTOOL_CACHE_DIRECTORY
- Default cache directory changed from ``~/.cache/dtool/s3`` to ``~/.cache/dtool``


[0.4.1] - 2018-09-11
--------------------

Fixed
^^^^^

- Fixed defect where multipart upload files did not have the md5sum in the
  manifest



[0.4.0] - 2018-07-24
--------------------

Added
^^^^^

- Add writing of admin_metadata as content of admin_metadata_key
- Added ``storage_broker_version`` to structure parameters
- Added inheritance from ``dtoolcore.storagebroker.BaseStorageClass``
- Overrode ``get_text`` method on ``BaseStorageBroker`` class
- Overrode ``put_text`` method on ``BaseStorageBroker`` class
- Overrode ``get_admin_metadata_key`` method on ``BaseStorageBroker`` class
- Overrode ``get_readme_key`` method on ``BaseStorageBroker`` class
- Overrode ``get_manifest_key`` method on ``BaseStorageBroker`` class
- Overrode ``get_overlay_key`` method on ``BaseStorageBroker`` class
- Overrode ``get_structure_key`` method on ``BaseStorageBroker`` class
- Overrode ``get_dtool_readme_key`` method on ``BaseStorageBroker`` class
- Overrode ``get_size_in_bytes`` method on ``BaseStorageBroker`` class
- Overrode ``get_utc_timestamp`` method on ``BaseStorageBroker`` class
- Overrode ``get_hash`` method on ``BaseStorageBroker`` class


[0.3.0] - 2018-07-09
--------------------

Fixed
^^^^^

- Made download to DTOOL_S3_CACHE_DIRECTORY more robust


[0.2.0] - 2018-07-05
--------------------

Added
^^^^^

- Added ``http_enable`` method to the ``S3StorageBroker`` class,  to allow
  publishing of datasets


[0.1.1] - 2018-01-18
--------------------

Fixed
^^^^^

- README.rst
- dtoolcore dependency in ``setup.py``


[0.1.0] - 2018-01-18
--------------------

Initial release.
