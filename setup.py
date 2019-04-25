from setuptools import setup

url = "https://github.com/jic-dtool/dtool-s3"
version = "0.5.0"
readme = open('README.rst').read()

setup(
    name="dtool-s3",
    packages=["dtool_s3"],
    version=version,
    description="Add S3 support to dtool",
    long_description=readme,
    include_package_data=True,
    # Package will be released using Tjelvar's PyPi credentials.
    author="Tjelvar Olsson",
    author_email="tjelvar.olsson@jic.ac.uk",
#   author="Matthew Hartley",  # NOQA
#   author_email="matthew.hartley@jic.ac.uk",  # NOQA
    url=url,
    download_url="{}/tarball/{}".format(url, version),
    install_requires=[
        "click",
        "dtoolcore>=3.10",
        "dtool_cli",
        "boto3",
    ],
    entry_points={
        "dtool.storage_brokers": [
            "S3StorageBroker=dtool_s3.storagebroker:S3StorageBroker",
        ],
    },
    license="MIT"
)
