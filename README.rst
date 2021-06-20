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


Usage
-----

To copy a dataset from local disk (``my-dataset``) to a S3 bucket
(``/data_raw``) one can use the command below::

    dtool copy ./my-dataset s3://data_raw

To list all the datasets in a S3 bucket one can use the command below::

    dtool ls s3://data_raw

See the `dtool documentation <http://dtool.readthedocs.io>`_ for more detail.

Path prefix and access control
------------------------------

The S3 plugin supports a configurable prefix to the path. This can be used for
access control to the dataset. For example::

    export DTOOL_S3_DATASET_PREFIX="u/olssont"

Alternatively one can edit the ``~/.config/dtool/dtool.json`` file::

    {
       ...,
       "DTOOL_S3_DATASET_PREFIX": "u/olssont"
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

::

    export S3_TEST_BASE_URI="s3://your-dtool-s3-test-bucket"
    python setup.py develop
    pytest


Windows PowerShell
~~~~~~~~~~~~~~~~~~

::

    $env:S3_TEST_BASE_URI = "s3://your-dtool-s3-test-bucket"
    python setup.py develop
    pytest

Windows DOS
~~~~~~~~~~~

::

    setx S3_TEST_BASE_URI "s3://test-dtool-s3-bucket-to"
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
