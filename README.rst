Add S3 support to dtool
=======================

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

To install the dtool-S3 package.

.. code-block:: bash

    pip install dtool-s3


Usage
-----

To copy a dataset from local disk (``my-dataset``) to a S3 bucket
(``/data_raw``) one can use the command below.

.. code-block::

    dtool copy ./my-dataset s3://data_raw

To list all the datasets in a S3 bucket one can use the command below.

.. code-block::

    dtool ls s3://data_raw

See the `dtool documentation <http://dtool.readthedocs.io>`_ for more detail.


Configuring the local dtool S3 cache
------------------------------------

When fetching items from a dataset, for example using the ``dtool item fetch``
command, the content of the item is cached in a file on local disk. The default
cache directory is ``~/.cache/dtool/s3``.

One may want to change this directory. For example, if working on a HPC cluster
to set it to a directory that lives on fast solid state disk. This can be achieved
by setting the ``DTOOL_S3_CACHE_DIRECTORY`` environment variable. For example

.. code-block::

    mkdir -p /tmp/dtool/s3
    export DTOOL_S3_CACHE_DIRECTORY=/tmp/dtool/s3

Alternatively, when using the ``dtool`` command line interface one can add the
``DTOOL_S3_CACHE_DIRECTORY`` key to the ``~/.config/dtool/dtool.json`` file.
For example,

.. code-block:: json

    {
       "DTOOL_S3_CACHE_DIRECTORY": "/tmp/dtool/s3"
    }

If the file does not exist one may need to create it.


Related packages
----------------

- `dtoolcore <https://github.com/jic-dtool/dtoolcore>`_
- `dtool-cli <https://github.com/jic-dtool/dtool-cli>`_
- `dtool-symlink <https://github.com/jic-dtool/dtool-symlink>`_
- `dtool-symlink <https://github.com/jic-dtool/dtool-irods>`_
