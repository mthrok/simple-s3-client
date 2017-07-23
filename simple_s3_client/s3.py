"""Simple interface for S3"""
from __future__ import division
from __future__ import absolute_import

import os.path
import logging

import boto3

from simple_s3_client import utils

_LG = logging.getLogger(__name__)


class S3(object):
    """Simple interface for S3

    Parameters
    ----------
    bucket : str
        Bucket name

    prefix : str or None
        Base prefix to store object. When valid string is given,
        all the keys given to methods are prefixed with this.
    """
    def __init__(self, bucket, region='us-east-1', prefix=None):
        self.bucket = bucket
        self.prefix = prefix
        self._client = boto3.client('s3', region_name=region)

    def _prefix(self, key):
        return '{}/{}'.format(self.prefix, key) if self.prefix else key

    def file_exists(self, key):
        """Check if a file with the given key exists"""
        return self.files_exist([key])[0]

    def files_exist(self, keys):
        """Check if files with common prefix exist

        Parameters
        ----------
        keys : list of str
            File names to check.

        Examples
        --------
        Suppose you have the following objects

        ``s3://bucket/tmp/file1``
        ``s3://bucket/tmp/file2``

        >>> s3 = S3(bucket='bucket')
        >>> print s3.files_exist(
        >>>     keys=['tmp/file1', 'tmp/file2', 'tmp/file3'],
        >>> )
        [True, True, False]
        """
        prefix = os.path.commonprefix(keys)
        prefix = self._prefix(prefix) if prefix else self.prefix
        keys = [self._prefix(key) for key in keys]
        ret = [False] * len(keys)

        paginator = self._client.get_paginator('list_objects')
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys_ = set(content['Key'] for content in page.get('Contents', []))
            for i, key in enumerate(keys):
                ret[i] = ret[i] or key in keys_
        return ret

    def get_url(self, key):
        """Get the URL for public access"""
        return 'https://s3.amazonaws.com/{}/{}'.format(
            self.bucket, self._prefix(key))

    def put_obj(self, key, obj, compress=True, acl='private'):
        """Put a byte string object on S3

        Parameters
        ----------
        key : str
            Key to store the object. Note that when `prefix` parameter is
            given at the time of instantiation, this `key` value is prefixed
            with the base prefix.

        obj : byte string
            String or unicode object to store

        compress : bool
            If True, the input string is compressed with gzipped.

        acl : str
            The object ACL. Value must be one of
            'private', 'public-read', 'public-read-write',
            'authenticated-read', 'aws-exec-read', 'bucket-owner-read' or
            'bucket-owner-full-control'.
        """
        key = self._prefix(key)
        obj = utils.compress(obj) if compress else obj
        _LG.info('Storing data on s3://%s/%s', self.bucket, key)
        self._client.put_object(Bucket=self.bucket, Key=key, Body=obj, ACL=acl)

    def get_obj(self, key, decompress=True):
        """Get an object from S3 in byte string

        Parameters
        ----------
        key : str
            Key to the object to get. Note that when `prefix` parameter is
            given at the time of instantiation, this `key` value is prefixed
            with the base prefix.

        compress : bool
            If True, the object is decompressed with gzip.

        Returns
        -------
        byte strings
            Byte string represents the object
        """
        key = self._prefix(key)
        _LG.info('Fetching s3://%s/%s', self.bucket, key)
        resp = self._client.get_object(Bucket=self.bucket, Key=key)
        data = resp['Body'].read()
        return utils.decompress(data) if decompress else data

    def put_jsonl(self, key, obj, compress=True, acl='private'):
        """Put JSONL object on S3

        Parameters
        ----------
        key : str
            Key of the object

         obj : list
            JSONL objects to store
        """
        self.put_obj(key, utils.dump_jsonl(obj), compress=compress, acl=acl)

    def put_json(self, key, obj, compress=True, acl='private'):
        """Put JSON objects to S3

        Parameters
        ----------
        key : str
            Key of the object

         obj
            JSON objects to store
        """
        self.put_jsonl(key, [obj], compress=compress, acl=acl)

    def get_jsonl(self, key, decompress=True):
        """Get JSONL object from S3

        Parameters
        ----------
        key : str
            Key to the object to get. Note that when `prefix` parameter is
            given at the time of instantiation, this `key` value is prefixed
            with the base prefix.

        decompress : bool
            Give True to fetch compressed JSONL object.

        Returns
        -------
        list
            List of objects
        """
        obj = self.get_obj(key, decompress=decompress)
        return utils.load_jsonl(obj)

    def get_json(self, key, decompress=True):
        """Get JSON objects to S3

        Parameters
        ----------
        key : str
            Key to the object to get. Note that when `prefix` parameter is
            given at the time of instantiation, this `key` value is prefixed
            with the base prefix.

        decompress : bool
            Give True to fetch compressed JSONL object.

        Returns
        -------
        obj
        """
        return self.get_jsonl(key, decompress=decompress)[0]
