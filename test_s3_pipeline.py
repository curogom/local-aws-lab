import io
import unittest
from unittest.mock import patch, Mock
import tempfile
import pathlib
import os
from botocore.exceptions import ClientError, ReadTimeoutError
import s3_pipeline as p
import s3_crash_probe as crash

class FakeS3:
    def __init__(self): self.objects = {}
    def get_object(self, Bucket, Key):
        if Key not in self.objects: raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
        return {'Body': io.BytesIO(self.objects[Key])}
    def put_object(self, Bucket, Key, Body, **kw): self.objects[Key] = Body

class RunS3(FakeS3):
    """Small in-memory service for exercising the report and cleanup lifecycle."""
    def create_bucket(self, **kw): pass
    def delete_bucket(self, **kw): pass
    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({'Error': {'Code': '404'}, 'ResponseMetadata': {'HTTPStatusCode': 404}}, 'HeadObject')
        return {'ContentLength': len(self.objects[Key])}
    def delete_object(self, Bucket, Key): self.objects.pop(Key, None)
    def list_objects_v2(self, Bucket, Prefix=''):
        return {'Contents': [{'Key': key} for key in self.objects if key.startswith(Prefix)]}
    def generate_presigned_url(self, *args, **kw): return p.ENDPOINT + '/synthetic'
    def get_paginator(self, name): return self
    def paginate(self, **kw): return [self.list_objects_v2(**kw)]

class PipelineTests(unittest.TestCase):
    def test_client_ignores_cloud_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime = pathlib.Path(directory)
            (runtime / 'emulator_credentials').write_text('[default]\naws_access_key_id=synthetic\naws_secret_access_key=synthetic\n')
            with patch.object(p, 'RUNTIME', runtime), patch.dict(os.environ, {'AWS_PROFILE': 'invalid-profile', 'AWS_ENDPOINT_URL': 'https://example.invalid'}):
                client = p.client()
                self.assertEqual(client.meta.endpoint_url, p.ENDPOINT)
                self.assertEqual(client._request_signer._credentials.access_key, 'synthetic')
                self.assertEqual(client.meta.config.proxies, {})
    def test_presigned_errors_redact_url(self):
        url = p.ENDPOINT + '/bucket/key?secret=do-not-log'
        opener = Mock()
        response = io.BytesIO(b'denied')
        opener.open.side_effect = p.urllib.error.HTTPError(url, 403, 'denied', {}, response)
        with patch.object(p.ssl, 'create_default_context'), patch.object(p.urllib.request, 'build_opener', return_value=opener):
            with self.assertRaises(RuntimeError) as exc: p.presigned_put(url, b'x')
            self.assertNotIn('secret', str(exc.exception))
            self.assertTrue(exc.exception.__suppress_context__)
            self.assertTrue(response.closed)
    def test_presigned_transport_and_server_failures(self):
        url = p.ENDPOINT + '/bucket/key?secret=do-not-log'
        errors = (
            (p.urllib.error.HTTPError(url, 503, 'unavailable', {}, None), 'HTTP 503'),
            (p.urllib.error.URLError(url), 'transport failed'),
            (TimeoutError(url), 'timed out'),
        )
        for error, message in errors:
            with self.subTest(error=type(error).__name__):
                opener = Mock(); opener.open.side_effect = error
                with patch.object(p.ssl, 'create_default_context'), patch.object(p.urllib.request, 'build_opener', return_value=opener):
                    with self.assertRaisesRegex(RuntimeError, message) as caught:
                        p.presigned_put(url, b'x')
                self.assertNotIn('secret', str(caught.exception))
                self.assertTrue(caught.exception.__suppress_context__)

    def test_failed_reads_never_write_output(self):
        for error in (ReadTimeoutError(endpoint_url=p.ENDPOINT),
                      ClientError({'Error': {'Code': 'InternalError'}, 'ResponseMetadata': {'HTTPStatusCode': 500}}, 'GetObject')):
            with self.subTest(error=type(error).__name__):
                s3 = Mock(); s3.get_object.side_effect = error
                with self.assertRaises(type(error)):
                    p.process(s3, 'b', 'inputs/j.txt', p.digest(b'hello'))
                s3.put_object.assert_not_called()

    def test_read_body_closed_when_stream_fails(self):
        stream = Mock(); stream.read.side_effect = ReadTimeoutError(endpoint_url=p.ENDPOINT)
        s3 = Mock(); s3.get_object.return_value = {'Body': stream}
        with self.assertRaises(ReadTimeoutError): p.read(s3, 'b', 'key')
        stream.close.assert_called_once_with()

    def test_failed_write_can_be_retried_without_duplicate_output(self):
        s3 = FakeS3(); key = p.upload(s3, 'b', 'j', b'hello')
        with patch.object(s3, 'put_object', side_effect=ClientError({'Error': {'Code': 'ServiceUnavailable'}}, 'PutObject')):
            with self.assertRaises(ClientError): p.process(s3, 'b', key, p.digest(b'hello'))
        self.assertFalse(any(k.startswith('outputs/') for k in s3.objects))
        out, result = p.process(s3, 'b', key, p.digest(b'hello'))
        self.assertEqual(p.process(s3, 'b', key, p.digest(b'hello')), (out, result))
        self.assertEqual(len(s3.objects), 2)

    def test_lost_write_response_retry_preserves_already_written_bytes(self):
        s3 = FakeS3(); key = p.upload(s3, 'b', 'j', b'hello')
        original = s3.put_object
        def commit_then_timeout(**kwargs):
            original(**kwargs)
            raise ReadTimeoutError(endpoint_url=p.ENDPOINT)
        with patch.object(s3, 'put_object', side_effect=commit_then_timeout):
            with self.assertRaises(ReadTimeoutError): p.process(s3, 'b', key, p.digest(b'hello'))
        before = dict(s3.objects)
        p.process(s3, 'b', key, p.digest(b'hello'))
        self.assertEqual(s3.objects, before)

    def test_upload_does_not_treat_server_error_as_missing_input(self):
        s3 = Mock(); s3.get_object.side_effect = ClientError({'Error': {'Code': 'InternalError'}}, 'GetObject')
        with self.assertRaises(ClientError): p.upload(s3, 'b', 'j', b'hello')
        s3.put_object.assert_not_called()

    def test_reports_preserve_primary_error_when_cleanup_also_fails(self):
        for module, filename in ((p, 's3-pipeline.json'), (crash, 's3-crash-probe.json')):
            with self.subTest(module=module.__name__):
                s3 = RunS3()
                s3.put_object = Mock(side_effect=ReadTimeoutError(endpoint_url=p.ENDPOINT))
                s3.delete_bucket = Mock(side_effect=RuntimeError('cleanup secret'))
                with tempfile.TemporaryDirectory() as directory, patch.object(module, 'ROOT', pathlib.Path(directory)), patch.object(module, 'client', return_value=s3):
                    with self.assertRaises(ReadTimeoutError): module.run()
                    report = p.json.loads((pathlib.Path(directory) / 'results' / filename).read_text())
                self.assertEqual(report['error_type'], 'ReadTimeoutError')
                self.assertEqual(report['cleanup_error_type'], 'RuntimeError')
                self.assertFalse(report['cleanup'])
                self.assertEqual(report['cases'], [])
                self.assertNotIn('secret', p.json.dumps(report))

    def test_cleanup_failure_does_not_report_success(self):
        s3 = RunS3(); s3.delete_bucket = Mock(side_effect=RuntimeError('cleanup secret'))
        def signed_upload(url, body):
            s3.objects['signed.txt'] = body
            return 200
        with tempfile.TemporaryDirectory() as directory, patch.object(p, 'ROOT', pathlib.Path(directory)), patch.object(p, 'client', return_value=s3), patch.object(p, 'presigned_put', side_effect=signed_upload):
            with self.assertRaises(RuntimeError): p.run()
            report = p.json.loads((pathlib.Path(directory) / 'results/s3-pipeline.json').read_text())
        self.assertEqual(len(report['cases']), 10)
        self.assertTrue(all(case['pass'] for case in report['cases']))
        self.assertFalse(report['cleanup'])
        self.assertEqual(report['cleanup_error_type'], 'RuntimeError')
    def test_local_guard(self):
        p.validate_url(p.ENDPOINT + '/bucket/key?sig=test')
        for url in ('https://s3.amazonaws.com/b/k', 'http://127.0.0.1:9298/b/k', 'https://localhost:9298/b/k', 'https://127.0.0.1:9297/b/k', 'https://u@127.0.0.1:9298/b/k'):
            with self.assertRaises(ValueError): p.validate_url(url)
    def test_upload_conflict(self):
        s = FakeS3(); key = p.upload(s, 'b', 'j', b'hello')
        self.assertEqual(p.upload(s, 'b', 'j', b'hello'), key)
        with self.assertRaises(p.Conflict): p.upload(s, 'b', 'j', b'changed')
        self.assertEqual(s.objects[key], b'hello')
    def test_retry_boundaries(self):
        for point in ('before_read', 'after_read_before_write', 'after_write_before_response'):
            with self.subTest(point=point):
                s = FakeS3(); key = p.upload(s, 'b', 'j', b'hello')
                with self.assertRaises(p.InjectedFailure): p.process(s, 'b', key, p.digest(b'hello'), point)
                self.assertEqual(sum(k.startswith('outputs/') for k in s.objects), int(point == 'after_write_before_response'))
                out, body = p.process(s, 'b', key, p.digest(b'hello'))
                self.assertEqual(p.process(s, 'b', key, p.digest(b'hello')), (out, body))
                self.assertEqual(sum(k.startswith('outputs/') for k in s.objects), 1)
    def test_mutation_and_utf8(self):
        s = FakeS3(); key = p.upload(s, 'b', 'j', '한글'.encode())
        out, body = p.process(s, 'b', key, p.digest('한글'.encode()))
        self.assertEqual(p.json.loads(body)['character_count'], 2)
        s.objects[key] = b'changed'
        with self.assertRaises(p.Conflict): p.process(s, 'b', key, p.digest('한글'.encode()))
    def test_bad_input(self):
        with self.assertRaises(ValueError): p.upload(FakeS3(), 'b', '../bad', b'x')
        s = FakeS3(); key = p.upload(s, 'b', 'binary', b'\xff')
        with self.assertRaises(UnicodeDecodeError): p.process(s, 'b', key, p.digest(b'\xff'))
        self.assertFalse(any(k.startswith('outputs/') for k in s.objects))

if __name__ == '__main__': unittest.main()
