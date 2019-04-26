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


Related packages
----------------

- `dtoolcore <https://github.com/jic-dtool/dtoolcore>`_
- `dtool-cli <https://github.com/jic-dtool/dtool-cli>`_
- `dtool-http <https://github.com/jic-dtool/dtool-http>`_
- `dtool-azure <https://github.com/jic-dtool/dtool-azure>`_
- `dtool-irods <https://github.com/jic-dtool/dtool-irods>`_
