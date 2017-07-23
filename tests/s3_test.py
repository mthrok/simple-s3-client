"""Test s3_ module"""
from __future__ import absolute_import
import json
import unittest

import boto3
from moto import mock_s3

from simple_s3_client import utils, s3 as s3_

_BUCKET = 'test-bucket'
_REGION = 'us-east-1'
_PREFIX = 'test'


def _init_bucket_and_clients(bucket=_BUCKET, region=_REGION, prefix=_PREFIX):
    client = boto3.client('s3', region_name=region)
    client.create_bucket(Bucket=bucket)
    s3 = s3_.S3(bucket=bucket, region=region, prefix=prefix)
    return client, s3


def _prefix(key, prefix=_PREFIX):
    return '{}/{}'.format(prefix, key) if prefix else key


class TestS3(unittest.TestCase):
    """Unit test for S3 class"""
    @mock_s3
    def test_file_exists(self):
        """file_exists returns True when file exists otherwise False"""
        client, s3 = _init_bucket_and_clients()

        key = 'random_file'
        self.assertFalse(s3.file_exists(key))
        client.put_object(Bucket=_BUCKET, Key=_prefix(key), Body='foo')
        self.assertTrue(s3.file_exists(key))

    @mock_s3
    def test_files_exist(self):
        """files_exist returns True for existing files and False for not"""
        client, s3 = _init_bucket_and_clients()

        keys = ['{}.json.gz'.format(i) for i in range(30)]
        for key in keys[::3]:
            client.put_object(Bucket=_BUCKET, Key=_prefix(key), Body='foo')

        flags = s3.files_exist(keys=keys)
        for i, flag in enumerate(flags):
            self.assertEqual(flag, i % 3 == 0)

    @mock_s3
    def test_put_obj(self):
        """put_obj stores string object on S3"""
        client, s3 = _init_bucket_and_clients()

        key, obj = 'random_file', 'foo'
        s3.put_obj(key, obj, compress=False)
        resp = client.get_object(Bucket=_BUCKET, Key=_prefix(key))
        found = resp['Body'].read().decode('utf8')
        self.assertEqual(obj, found)

    def _test_put_jsonl(self, obj):
        """Put the given object on S3 and check if it's same"""
        client, s3 = _init_bucket_and_clients()

        keys = ['file.jsonl', 'file.jsonl.gz']
        dumped = utils.dump_jsonl(obj).encode('utf-8')

        s3.put_jsonl(keys[0], obj, compress=False)
        expected = dumped
        resp = client.get_object(Bucket=_BUCKET, Key=_prefix(keys[0]))
        found = resp['Body'].read()
        self.assertEqual(expected, found)

        s3.put_jsonl(keys[1], obj, compress=True)
        expected = utils.compress_bytes(dumped)
        resp = client.get_object(Bucket=_BUCKET, Key=_prefix(keys[1]))
        found = resp['Body'].read()
        self.assertEqual(expected, found)

    def _test_put_json(self, obj):
        """Put the given object on S3 and check if it's same"""
        client, s3 = _init_bucket_and_clients()

        keys = ['file.json', 'file.json.gz']
        dumped = json.dumps(obj).encode('utf-8')

        s3.put_json(keys[0], obj, compress=False)
        expected = dumped
        resp = client.get_object(Bucket=_BUCKET, Key=_prefix(keys[0]))
        found = resp['Body'].read()
        self.assertEqual(expected, found)

        s3.put_json(keys[1], obj, compress=True)
        expected = utils.compress_bytes(dumped)
        resp = client.get_object(Bucket=_BUCKET, Key=_prefix(keys[1]))
        found = resp['Body'].read()
        self.assertEqual(expected, found)

    @mock_s3
    def test_put_jsonl(self):
        """put_jsonl stores JSONL objects on S3"""
        self._test_put_jsonl([])
        self._test_put_jsonl([[]])
        self._test_put_jsonl([{}])
        self._test_put_jsonl([None])
        self._test_put_jsonl([0])
        self._test_put_jsonl([''])
        self._test_put_jsonl([u'\u03b2'])
        self._test_put_jsonl([
            None,
            0,
            1,
            '',
            u'\u03b2',
            [],
            {},
            [1, 2, {None: 45}],
            {'foo': None},
            {'bar': 30},
            {'baz': [1, 2, 3]},
        ])

    @mock_s3
    def test_put_json(self):
        """put_json stores JSON objects on S3"""
        self._test_put_json(None)
        self._test_put_json([])
        self._test_put_json({})
        self._test_put_json(0)
        self._test_put_json('')
        self._test_put_json(u'\u03b2')
        self._test_put_json([
            None,
            0,
            1,
            '',
            u'\u03b2',
            [],
            {},
            [1, 2, {None: 45}],
            {'foo': None},
            {'bar': 30},
            {'baz': [1, 2, 3]},
        ])
        self._test_put_json({
            None: 1,
            0: 2,
            1: 3,
            '': 'foo',
            u'\u03b2': 'aa',
            'foo': [4, 5],
            'bar': {20: 4},
            'baz': None,
        })
