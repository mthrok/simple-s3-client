"""Install NCBI tools"""
import setuptools


def _setup():
    setuptools.setup(
        name='simple-s3-client',
        version='v0.0.1',
        packages=setuptools.find_packages(
            exclude=[
                'tests',
            ]
        ),
        install_requires=[
            'six',
            'boto3',
        ],
        test_suite='tests',
    )


if __name__ == '__main__':
    _setup()
