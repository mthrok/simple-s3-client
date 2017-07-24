[![Codacy Badge](https://api.codacy.com/project/badge/Grade/ac5841b9e7b242bb977b019a74a7f4f7)](https://www.codacy.com/app/mthrok/simple-s3-client?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=mthrok/simple-s3-client&amp;utm_campaign=Badge_Grade)
[![Coverage Status](https://coveralls.io/repos/github/mthrok/simple-s3-client/badge.svg?branch=master)](https://coveralls.io/github/mthrok/simple-s3-client?branch=master)

# Simple S3 Client

Simple S3 Client is wrapper around `boto3` and provides a simple method to fetch/store small text/json/byte string files, and abstract away (de)compression process.

It is useful if your application

  - Only uses one bucket.
  - Handles JSON files.


## Usage

```python
from simple_s3_client.s3 import S3

s3 = S3(bucket='a-bucket')
assert s3.file_exists('foo') == False
s3.put_obj(key='foo', obj='bar', compress=True, acl='public-read')
assert s3.file_exists('foo') == True
print(s3.get_url('foo'))
# https://s3.amazonaws.com/a-bucket/foo


obj = {
    'some_json_object': [ ... ],
}

s3.put_json(key='bar.json.gz', obj=obj, compress=True)

obj2 = s3.gett_json(key='bar.json.gz', decompress=True)

assert obj == obj2
```