"""Helper functions for string manipulations"""
from __future__ import absolute_import

import gzip
import json

import six


def compress_bytes(string):
    """Compress byte string with GZip

    Parameters
    ----------
    string : byte string
        String to be compressed.

    Returns
    -------
    byte string
        Compressed string.
    """
    file_ = six.BytesIO()
    gzip.GzipFile(fileobj=file_, mode='w').write(string)
    return file_.getvalue()


def compress_text(string, codec='utf-8'):
    """Compress unicode string object with GZip

    Parameters
    ----------
    string : unicode string
        String to be compressed.

    codec : str
        Codec type to use when encoding unicode string to bytes string.

    Returns
    -------
    byte string
        Compressed string.
    """
    return compress_bytes(string.encode(codec))


def compress(string, codec='utf-8'):
    """Compress string object with GZip

    Parameters
    ----------
    string : byte string for unicode string
        String to be compressed.

    codec : str
        Codec type to use when encoding unicode string to bytes string.

    Returns
    -------
    byte string
        Compressed string.
    """
    if isinstance(string, six.binary_type):
        return compress_bytes(string)
    return compress_text(string, codec)


def decompress_bytes(string):
    """Decompress byte string with GZip

    Parameters
    ----------
    string : byte string
        String to be decompressed.

    Returns
    -------
    byte string
        Decompressed string.
    """
    file_ = six.BytesIO(string)
    return gzip.GzipFile(fileobj=file_, mode='r').read()


def decompress_text(string, codec='utf-8'):
    """Decompress byte string with GZip and decode with the codec.

    The name of this function is a bit confusing as it decompress byte string
    into text string.

    Parameters
    ----------
    string : byte str
        String to be decompressed.

    encode: str
        Codec type to use when decoding the decompressed bytes string.

    Returns
    -------
    string
        Decompressed unicode string.
    """
    return decompress_bytes(string).decode(codec)


def decompress(string, codec='utf-8'):
    """Decompress string object with GZip

    Parameters
    ----------
    string : byte string for unicode string
        String to be decompressed.

    codec : str
        Codec type to use when decoding the decompressed bytes string.

    Returns
    -------
    byte string or unicode string
        Decompressed string.
    """
    if isinstance(string, six.binary_type):
        return decompress_bytes(string)
    return decompress_text(string, codec)


def dump_jsonl(obj):
    """Convert JSON object into 'JSON Lines' format string.
    See http://jsonlines.org/ for the detail of the format.

    Parameters
    ----------
    obj : list of JSON compatible objects
        Objects to be dumped.

    Returns
    -------
    str
        Resulting string.
    """
    if not isinstance(obj, list):
        raise TypeError('The most outer structure must be list.')
    return '\n'.join(json.dumps(obj_, ensure_ascii=True) for obj_ in obj)


def load_jsonl(string):
    """JSON Lines format string into JSON object.
    See http://jsonlines.org/ for the detail of the format.

    Parameters
    ----------
    str : string
        string to load

    Returns
    -------
    list of objects
        Resulting object.
    """
    if isinstance(string, bytes):
        string = string.decode('utf-8')
    return [json.loads(part) for part in string.split('\n')]
