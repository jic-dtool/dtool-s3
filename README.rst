Add S3 support to dtool
=======================

.. image:: https://badge.fury.io/py/dtool-s3.svg
   :target: http://badge.fury.io/py/dtool-s3
   :alt: PyPi package

- GitHub: https://github.com/jic-dtool/dtool-S3
- PyPI: https://pypi.python.org/pypi/dtool-S3
- Free software: MIT License

Features
--------

- Copy datasets to and from S3 object storage
- List all the datasets in a S3 bucket
- Create datasets directly in S3

Installation
------------

To install the dtool-S3 package::

    pip install dtool-s3


Configuration
-------------

Install the ``aws`` client, for details see
`https://docs.aws.amazon.com/cli/latest/userguide/installing.html <https://docs.aws.amazon.com/cli/latest/userguide/installing.html>`_. In short::

    pip install awscli --upgrade --user

Configure the credentials using::

    aws configure

These are needed for the ``boto3`` library, for more details see
`https://boto3.readthedocs.io/en/latest/guide/quickstart.html <https://boto3.readthedocs.io/en/latest/guide/quickstart.html>`_.


Configuring custom endpoints
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is possible to configure buckets to make use of custom endpoints. This is useful if one wants to make use of S3 storage not hosted in AWS.

Create the file ``.config/dtool/dtool.json`` and add the s3 storage account details
using the format below::

    {
       "DTOOL_S3_ENDPOINT_<BUCKET NAME>": "<ENDPOINT URL HERE>",
       "DTOOL_S3_ACCESS_KEY_<BUCKET NAME>": "<USER NAME HERE>",
       "DTOOL_S3_SECRET_ACCESS_KEY_<BUCKET NAME>": "<KEY HERE>"
    }

For example::

    {
       "DTOOL_S3_ENDPOINT_my-bucket": "http://blueberry.famous.uni.ac.uk",
       "DTOOL_S3_ACCESS_KEY_ID_my-bucket": "olssont",
       "DTOOL_S3_SECRET_ACCESS_KEY_my-bucket": "some-secret-token"
    }

The configuration can also be done using your environment variables. For example on Linux/Mac::

    env 'DTOOL_S3_ENDPOINT_my-bucket=http://blueberry.famous.uni.ac.uk' \
        'DTOOL_S3_ACCESS_KEY_ID_my-bucket=olssont' \
        'DTOOL_S3_SECRET_ACCESS_KEY_my_bucket=some-secret-token' bash

Note that hyphens in environment variable names do not adhere to the
`POSIX standard <https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap08.html>`_
``export`` will not allow such names, hence the above *workaround* via ``env`` may
be necessary to modify the environment.


Usage
-----

To copy a dataset from local disk (``my-dataset``) to a S3 bucket
(``/data_raw``) one can use the command below::

    dtool copy ./my-dataset s3://data_raw

To list all the datasets in a S3 bucket one can use the command below::

    dtool ls s3://data_raw

See the `dtool documentation <http://dtool.readthedocs.io>`_ for more detail.


Publishing datasets
-------------------

It is possible to make datasets stored in S3 publicly accessible using the
`dtool publish` command. The S3 storage broker supports making datasets
accessible to the world by setting the ACL to `public-read` (the default) as
well as giving limited access to datasets using presigned URLS.

To publish a dataset using a presigned URL one needs to set the
`DTOOL_S3_PUBLISH_EXPIRY` to the number of seconds one wants to make the
dataset accessible for. For example by adding this setting to the
`~/.config/dtool/dtool.json` file or by exporting it as an environment
variable.

::

    export DTOOL_S3_PUBLISH_EXPIRY=3600


Path prefix and access control
------------------------------

The S3 plugin supports an endpoint-specific configurable prefix to the path.
This can be used for access control to the dataset. For example::

    env 'DTOOL_S3_DATASET_PREFIX_my-bucket=u/olssont' bash

Alternatively one can edit the ``~/.config/dtool/dtool.json`` file::

    {
       ...,
       "DTOOL_S3_DATASET_PREFIX_my-bucket": "u/olssont"
    }


Use the following S3 access to policy to that allows reading all data
in the bucket but only writing to the prefix `u/<username>` and `dtool-`::

    {
      "Statement": [
        {
          "Sid": "AllowReadonlyAccess",
          "Effect": "Allow",
          "Action": [
            "s3:ListBucket",
            "s3:ListBucketVersions",
            "s3:GetObject",
            "s3:GetObjectTagging",
            "s3:GetObjectVersion",
            "s3:GetObjectVersionTagging"
          ],
          "Resource": [
            "arn:aws:s3:::my-bucket",
            "arn:aws:s3:::my-bucket/*"
          ]
        },
        {
          "Sid": "AllowPartialWriteAccess",
          "Effect": "Allow",
          "Action": [
            "s3:DeleteObject",
            "s3:PutObject",
            "s3:PutObjectAcl"
          ],
          "Resource": [
            "arn:aws:s3:::my-bucket/dtool-*",
            "arn:aws:s3:::my-bucket/u/${aws:username}/*"
          ]
        },
        {
          "Sid": "AllowListAllBuckets",
          "Effect": "Allow",
          "Action": [
            "s3:ListAllMyBuckets",
            "s3:GetBucketLocation"
          ],
          "Resource": "arn:aws:s3:::*"
        }
      ]
    }

The user also needs write access to toplevel objects that start with `dtool-`.
Those are the registration keys that are not stored under the configured
prefix. The registration keys contain the prefix where the respective dataset
is found. They are empty if no prefix is configured.

Testing
-------

Linux/Mac
~~~~~~~~~

All tests need the S3_TEST_BASE_URI environment variable set.

::

    export S3_TEST_BASE_URI="s3://your-dtool-s3-test-bucket"

For the ``tests/test_custom_endpoint_config.py`` test one also needs to specify the S3_TEST_ACCESS_KEY_ID and S3_TEST_SECRET_ACCESS_KEY environment variables.

::

    export S3_TEST_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY
    export S3_TEST_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY

To run the tests.

::

    python setup.py develop
    pytest


Windows PowerShell
~~~~~~~~~~~~~~~~~~

All tests need the S3_TEST_BASE_URI environment variable set.

::

    $env:S3_TEST_BASE_URI = "s3://your-dtool-s3-test-bucket"

For the ``tests/test_custom_endpoint_config.py`` test one also needs to specify the S3_TEST_ACCESS_KEY_ID and S3_TEST_SECRET_ACCESS_KEY environment variables.

::

    $env:S3_TEST_ACCESS_KEY_ID = YOUR_AWS_ACCESS_KEY
    $env:S3_TEST_SECRET_ACCESS_KEY = YOUR_AWS_SECRET_ACCESS_KEY

To run the tests.

::

    python setup.py develop
    pytest

Windows DOS
~~~~~~~~~~~

All tests need the S3_TEST_BASE_URI environment variable set.

::

    setx S3_TEST_BASE_URI "s3://test-dtool-s3-bucket-to"
    python setup.py develop
    pytest

For the ``tests/test_custom_endpoint_config.py`` test one also needs to specify the S3_TEST_ACCESS_KEY_ID and S3_TEST_SECRET_ACCESS_KEY environment variables.

::

    setx S3_TEST_ACCESS_KEY_ID YOUR_AWS_ACCESS_KEY
    setx S3_TEST_SECRET_ACCESS_KEY YOUR_AWS_SECRET_ACCESS_KEY

To run the tests.

::

    python setup.py develop
    pytest


Related packages
----------------

- `dtoolcore <https://github.com/jic-dtool/dtoolcore>`_
- `dtool-cli <https://github.com/jic-dtool/dtool-cli>`_
- `dtool-ecs <https://github.com/jic-dtool/dtool-ecs>`_
- `dtool-http <https://github.com/jic-dtool/dtool-http>`_
- `dtool-azure <https://github.com/jic-dtool/dtool-azure>`_
- `dtool-irods <https://github.com/jic-dtool/dtool-irods>`_
- `dtool-smb <https://github.com/IMTEK-Simulation/dtool-smb>`_
