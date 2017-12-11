from setuptools import setup

url = "https://github.com/jic-dtool/dtool-s3"
version = "0.1.0"
readme = open('README.rst').read()

setup(
    name="dtool-s3",
    packages=["dtool_s3"],
    version=version,
    description="Add iRODS support to dtool",
    long_description=readme,
    include_package_data=True,
    author="Matthew Hartley",
    author_email="matthew.hartley@jic.ac.uk",
    url=url,
    download_url="{}/tarball/{}".format(url, version),
    install_requires=[
        "click",
        "dtoolcore>=2.2.0",
        "dtool_cli",
    ],
    entry_points={
        "dtool.storage_brokers": [
            "S3StorageBroker=dtool_s3.storagebroker:S3StorageBroker",
        ],
    },
    license="MIT"
)
