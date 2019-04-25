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
