"""Test s3 module"""
from __future__ import absolute_import
import unittest

import six

from simple_s3_client import utils

_TEST_BUCKET = 'test-bucket'
# pylint: disable=protected-access


class TestCompression(unittest.TestCase):
    """Test (de)compression functions"""
    longMessage = True

    def test_bytes_round_compression(self):
        """Byte strings are correctly compressed/decompressed"""
        objs = [b'', b'\n', b'foo', b'\\u03b2']
        for obj in objs:
            compressed = utils.compress_bytes(obj)
            recovered = utils.decompress_bytes(compressed)
            self.assertEqual(obj, recovered)

    def test_text_round_compression(self):
        """Unicode strings are correctly compressed/decompressed"""
        objs = [u'', u'\n', u'foo', u'\u03b2']
        for obj in objs:
            compressed = utils.compress_text(obj)
            recovered = utils.decompress_text(compressed)
            self.assertEqual(obj, recovered)


_SINGLE_JSONL_OBJECTS = [
    [obj] for obj in [
        None,
        u'', u'foo', u'\n', u'\u03b2',
        0, 0.0, 1.0, 1, -1, 10, -10, 1e10, 1e-10,
        [], [None], [0], [u''], [[]], [{}],
        {u'foo': u'bar'}, {u'foo': 0, u'boo': []}, {u'foo': {u'bar': 0}}
    ]
]


_MULTI_JSONL_OBJECTS = [
    [six.unichr(i) for i in range(0, 1024, 256)],
    [six.unichr(i) for i in range(0, 1024, 256)],
    [{six.unichr(i): i} for i in range(0, 1024, 256)],
]


class TestSerialization(unittest.TestCase):
    """Test (de)serialization of JSONL objects"""
    longMessage = True

    def test_dump_jsonl(self):
        """dump_jsonl produces JSON Lines format string"""
        values = [
            (
                [101],
                '101'
            ),
            (
                ['foo'],
                '"foo"'
            ),
            (
                [u'\u03b2'],
                u'"\\u03b2"'
            ),
            (
                [u'\u03b2'],
                u'"\\u03b2"'
            ),
            (
                ['foo', 'bar'],
                '"foo"\n"bar"',
            ),
            (
                [u'foo', 'bar'],
                u'"foo"\n"bar"',
            ),
            (
                [{'foo': 'bar\n'}],
                '{"foo": "bar\\n"}'
            ),
            (
                [{u'foo': 'bar'}],
                u'{"foo": "bar"}'
            ),
            (
                [{'foo1': 'bar1'}, {'foo2': 'bar2'}],
                '{"foo1": "bar1"}\n{"foo2": "bar2"}'
            ),
            (
                [{'foo1': 'bar1'}, {'foo2': 'bar2'}, {'foo3': 'bar3'}],
                '{"foo1": "bar1"}\n{"foo2": "bar2"}\n{"foo3": "bar3"}'
            ),
        ]
        for obj, expected in values:
            found = utils.dump_jsonl(obj)
            self.assertEqual(expected, found)

    def test_round_serialization_single(self):
        """Single line JSONL objects are correctly dumped/loaded"""
        for expected in _SINGLE_JSONL_OBJECTS:
            recovered = utils.load_jsonl(utils.dump_jsonl(expected))
            self.assertEqual(expected, recovered)

    def test_round_serialization_multi(self):
        """Multi-line JSONL objects are correctly dumped/loaded"""
        for expected in _MULTI_JSONL_OBJECTS:
            recovered = utils.load_jsonl(utils.dump_jsonl(expected))
            self.assertEqual(expected, recovered)


class TestCompressedSerialization(unittest.TestCase):
    """Test (de)serialization of JSONL objects with compression"""
    longMessage = True

    def test_round_compressed_serialization(self):
        """JSON object is correctly dumped with compression and recovered"""
        for obj in _SINGLE_JSONL_OBJECTS + _MULTI_JSONL_OBJECTS:
            dumped = utils.dump_jsonl(obj)
            compressed = utils.compress_text(dumped)
            decompressed = utils.decompress_text(compressed)
            recovered = utils.load_jsonl(decompressed)
            self.assertEqual(obj, recovered)
