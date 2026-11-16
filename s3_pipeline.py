"""Part 2: local-only S3 upload/processing retry experiments (not production)."""
import configparser
import hashlib
import json
import os
import pathlib
import re
import ssl
import urllib.parse
import urllib.request
import urllib.error
import uuid

import boto3
import botocore.session
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = pathlib.Path(__file__).resolve().parent
RUNTIME = ROOT / '.runtime/emulator/9298'
ENDPOINT = 'https://127.0.0.1:9298'

def output_key_for(key):
    return 'outputs/' + digest(key.encode()) + '-v1.json'


def client():
    cfg = configparser.ConfigParser()
    if not cfg.read(RUNTIME / 'emulator_credentials'):
        raise RuntimeError('Start the dedicated lab emulator first')
    core = botocore.session.get_session()
    store = core.get_component('config_store')
    for key, value in [('profile', None), ('config_file', os.devnull), ('credentials_file', os.devnull)]:
        store.set_config_variable(key, value)
    credentials = cfg['default']
    return boto3.session.Session(botocore_session=core).client(
        's3', endpoint_url=ENDPOINT, region_name='us-east-1',
        aws_access_key_id=credentials['aws_access_key_id'],
        aws_secret_access_key=credentials['aws_secret_access_key'],
        verify=str(RUNTIME / 'ca.pem'),
        config=Config(signature_version='s3v4', proxies={}, s3={'addressing_style': 'path'},
                      connect_timeout=3, read_timeout=5, retries={'max_attempts': 0}))


def digest(body):
    return hashlib.sha256(body).hexdigest()


def read(s3, bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    try:
        return response['Body'].read()
    finally:
        response['Body'].close()


class Conflict(ValueError):
    pass


class InjectedFailure(RuntimeError):
    pass


def upload(s3, bucket, request_id, body):
    """Sequential demo only: this GET/PUT check is NOT an atomic claim."""
    if not re.fullmatch(r'[a-zA-Z0-9-]{1,64}', request_id):
        raise ValueError('Invalid request id')
    key = f'inputs/{request_id}.txt'
    try:
        previous = read(s3, bucket, key)
    except ClientError as exc:
        if exc.response['Error']['Code'] not in ('NoSuchKey', '404'):
            raise
    else:
        if previous != body:
            raise Conflict('Same request id with a different body')
        return key
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType='text/plain',
                  Metadata={'sha256': digest(body)})
    return key


def process(s3, bucket, key, expected_hash, fail_at=None):
    if fail_at == 'before_read':
        raise InjectedFailure(fail_at)
    body = read(s3, bucket, key)
    if digest(body) != expected_hash:
        raise Conflict('Input mutated after upload validation')
    text = body.decode('utf-8')
    result = {'input_key': key, 'input_sha256': expected_hash,
              'byte_count': len(body), 'character_count': len(text), 'processor_version': 'v1'}
    encoded = json.dumps(result, sort_keys=True, ensure_ascii=False).encode('utf-8')
    output_key = output_key_for(key)
    if fail_at == 'after_read_before_write':
        raise InjectedFailure(fail_at)
    s3.put_object(Bucket=bucket, Key=output_key, Body=encoded, ContentType='application/json')
    if fail_at == 'after_write_before_response':
        raise InjectedFailure(fail_at)
    return output_key, encoded


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RuntimeError('Redirect refused: local-only experiment')


def validate_url(url):
    parsed = urllib.parse.urlsplit(url)
    if (parsed.scheme, parsed.hostname, parsed.port) != ('https', '127.0.0.1', 9298) or parsed.username or parsed.password or parsed.fragment:
        raise ValueError('Presigned request is not the isolated local endpoint')


def presigned_put(url, body):
    validate_url(url)
    context = ssl.create_default_context(cafile=str(RUNTIME / 'ca.pem'))
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(),
                                        urllib.request.HTTPSHandler(context=context))
    request = urllib.request.Request(url, data=body, method='PUT', headers={'Content-Type': 'text/plain'})
    try:
        with opener.open(request, timeout=5) as response:
            return response.status
    except urllib.error.HTTPError as exc:
        status = exc.code
        exc.close()
        raise RuntimeError(f'Local presigned PUT failed with HTTP {status}') from None
    except urllib.error.URLError:
        raise RuntimeError('Local presigned PUT transport failed') from None
    except TimeoutError:
        raise RuntimeError('Local presigned PUT timed out') from None


def run():
    s3 = None
    bucket = 'part2-' + uuid.uuid4().hex[:12]
    report = {'scope': 'local S3 functional test; sequential application retries', 'bucket': bucket,
              'endpoint': ENDPOINT, 'cases': [], 'cleanup': False}
    def check(name, passed, **details):
        report['cases'].append({'case': name, 'pass': bool(passed), **details})
        if not passed:
            raise AssertionError(name)
    created = False
    try:
        s3 = client()
        s3.create_bucket(Bucket=bucket); created = True
        body = 'Hello S3\n안녕하세요\n'.encode()
        s3.put_object(Bucket=bucket, Key='crud.txt', Body=body, ContentType='text/plain')
        head = s3.head_object(Bucket=bucket, Key='crud.txt')
        check('crud_readback', head['ContentLength'] == len(body) and read(s3, bucket, 'crud.txt') == body,
              sha256=digest(body), byte_count=len(body))
        s3.delete_object(Bucket=bucket, Key='crud.txt')
        try:
            s3.head_object(Bucket=bucket, Key='crud.txt')
        except ClientError as exc:
            check('delete_then_head', exc.response['ResponseMetadata']['HTTPStatusCode'] == 404, status=exc.response['ResponseMetadata']['HTTPStatusCode'])
        else:
            check('delete_then_head', False)
        url = s3.generate_presigned_url('put_object', Params={'Bucket': bucket, 'Key': 'signed.txt', 'ContentType': 'text/plain'}, ExpiresIn=60)
        status = presigned_put(url, body)
        check('presigned_put_readback', status == 200 and read(s3, bucket, 'signed.txt') == body, status=status,
              note='Transfer compatibility only; not IAM, signature enforcement, expiry or CORS validation')
        replacement = b'replacement'
        s3.put_object(Bucket=bucket, Key='signed.txt', Body=replacement)
        check('same_key_overwrite', read(s3, bucket, 'signed.txt') == replacement)
        key = upload(s3, bucket, 'job-1', body)
        check('same_request_same_body', upload(s3, bucket, 'job-1', body) == key)
        try:
            upload(s3, bucket, 'job-1', b'different')
        except Conflict:
            check('same_request_different_body', read(s3, bucket, key) == body)
        else:
            check('same_request_different_body', False)
        for index, point in enumerate(('before_read', 'after_read_before_write', 'after_write_before_response')):
            input_key = upload(s3, bucket, f'failure-{index}', body)
            try:
                process(s3, bucket, input_key, digest(body), point)
            except InjectedFailure:
                pass
            else:
                raise AssertionError('Failure was not injected')
            interrupted = s3.list_objects_v2(Bucket=bucket, Prefix=output_key_for(input_key)).get('Contents', [])
            expected_count = int(point == 'after_write_before_response')
            if len(interrupted) != expected_count:
                raise AssertionError('Unexpected object state at failure boundary')
            interrupted_body = read(s3, bucket, output_key_for(input_key)) if interrupted else None
            out, expected = process(s3, bucket, input_key, digest(body))
            second, repeated = process(s3, bucket, input_key, digest(body))
            objects = s3.list_objects_v2(Bucket=bucket, Prefix=out).get('Contents', [])
            check(point, out == second and expected == repeated and read(s3, bucket, out) == expected and len(objects) == 1 and (interrupted_body is None or interrupted_body == expected),
                  attempt=3, objects_before_retry=len(interrupted), input_key=input_key, output_key=out, input_sha256=digest(body), output_sha256=digest(expected),
                  note='Injected Python exception; not SIGKILL or network-disconnect test')
        s3.put_object(Bucket=bucket, Key=key, Body=b'mutated')
        try:
            process(s3, bucket, key, digest(body))
        except Conflict:
            check('input_mutation_rejected', True)
        else:
            check('input_mutation_rejected', False)
        return report
    except Exception as exc:
        report['error_type'] = type(exc).__name__  # Never persist presigned URLs or credentials.
        raise
    finally:
        try:
            if created:
                for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket):
                    for item in page.get('Contents', []):
                        s3.delete_object(Bucket=bucket, Key=item['Key'])
                s3.delete_bucket(Bucket=bucket)
                report['cleanup'] = True
        except Exception as exc:
            report['cleanup_error_type'] = type(exc).__name__
            # Keep the original processing failure, but fail a successful run if cleanup fails.
            if 'error_type' not in report:
                raise
        finally:
            (ROOT / 'results').mkdir(exist_ok=True)
            (ROOT / 'results/s3-pipeline.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    run()
    print('S3 cases passed; synthetic report: results/s3-pipeline.json')
