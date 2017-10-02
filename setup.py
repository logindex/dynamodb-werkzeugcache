from setuptools import setup, find_packages

version = '0.0.0.1'

setup(
    name='dynamodb-werkzeugcache',
    description=(
        "DynamoDB implementation of the werkzeug cache"),
    version=version,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'boto3',
        'flask-cache',
        'werkzeug',
    ],
    author='Ricardo Pereira',
    author_email='ricardo.pereira@gmail.com',
    download_url=(
        'https://github.com/logindex/dynamodb-werkzeugcacge/tarball/' + version),
    classifiers=[
        'Programming Language :: Python :: 3.5',
    ],
)