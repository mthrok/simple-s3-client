"""Test s3_ module"""
from __future__ import absolute_import
import json
import unittest

import boto3
from moto import mock_s3

from simple_s3_client import utils, s3 as s3_

_TEST_BUCKET = 'test-bucket'


def _init_bucket_and_clients(bucket, region='us-east-1'):
    client = boto3.client('s3', region_name=region)
    client.create_bucket(Bucket=bucket)
    s3 = s3_.S3(bucket=bucket, region=region)
    return client, s3


class TestS3(unittest.TestCase):
    """Unit test for S3 class"""
    @mock_s3
    def test_file_exists(self):
        """file_exists returns True when file exists otherwise False"""
        key = 'test/random_file'
        client, s3 = _init_bucket_and_clients(bucket=_TEST_BUCKET)

        self.assertFalse(s3.file_exists(key))
        client.put_object(Bucket=_TEST_BUCKET, Key=key, Body='foo')
        self.assertTrue(s3.file_exists(key))

    @mock_s3
    def test_files_exist(self):
        """files_exist returns True for existing files and False for not"""
        keys = ['test/{}.json.gz'.format(i) for i in range(30)]
        client, s3 = _init_bucket_and_clients(bucket=_TEST_BUCKET)

        for key in keys[::3]:
            client.put_object(Bucket=_TEST_BUCKET, Key=key, Body='foo')

        flags = s3.files_exist(keys=keys)
        for i, flag in enumerate(flags):
            self.assertEqual(flag, i % 3 == 0)

    @mock_s3
    def test_put_obj(self):
        """put_obj stores string object on S3"""
        key, obj = 'random_file', 'foo'
        client, s3 = _init_bucket_and_clients(bucket=_TEST_BUCKET)

        s3.put_obj(key, obj, compress=False)
        found = client.get_object(
            Bucket=_TEST_BUCKET, Key=key)['Body'].read().decode('utf8')
        self.assertEqual(obj, found)

    def _test_put_jsonl(self, obj):
        """Put the given object on S3 and check if it's same"""
        keys = ['file.jsonl', 'file.jsonl.gz']
        client, s3 = _init_bucket_and_clients(bucket=_TEST_BUCKET)

        obj = [obj] if not isinstance(obj, list) else obj
        dumped = utils.dump_jsonl(obj).encode('utf-8')

        s3.put_jsonl(keys[0], obj, compress=False)
        expected = dumped
        found = client.get_object(
            Bucket=_TEST_BUCKET, Key=keys[0])['Body'].read()
        self.assertEqual(expected, found)

        s3.put_jsonl(keys[1], obj, compress=True)
        expected = utils.compress_bytes(dumped)
        found = client.get_object(
            Bucket=_TEST_BUCKET, Key=keys[1])['Body'].read()
        self.assertEqual(expected, found)

    def _test_put_json(self, obj):
        """Put the given object on S3 and check if it's same"""
        keys = ['file.json', 'file.json.gz']

        client, s3 = _init_bucket_and_clients(bucket=_TEST_BUCKET)

        dumped = json.dumps(obj).encode('utf-8')

        s3.put_json(keys[0], obj, compress=False)
        expected = dumped
        found = client.get_object(
            Bucket=_TEST_BUCKET, Key=keys[0])['Body'].read()
        self.assertEqual(expected, found)

        s3.put_json(keys[1], obj, compress=True)
        expected = utils.compress_bytes(dumped)
        found = client.get_object(
            Bucket=_TEST_BUCKET, Key=keys[1])['Body'].read()
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
