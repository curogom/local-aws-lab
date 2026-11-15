import io
import unittest
from unittest.mock import patch, Mock
import tempfile
import pathlib
import os
from botocore.exceptions import ClientError
import s3_pipeline as p

class FakeS3:
    def __init__(self): self.objects = {}
    def get_object(self, Bucket, Key):
        if Key not in self.objects: raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
        return {'Body': io.BytesIO(self.objects[Key])}
    def put_object(self, Bucket, Key, Body, **kw): self.objects[Key] = Body

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
        opener.open.side_effect = p.urllib.error.HTTPError(url, 403, 'denied', {}, None)
        with patch.object(p.ssl, 'create_default_context'), patch.object(p.urllib.request, 'build_opener', return_value=opener):
            with self.assertRaises(RuntimeError) as exc: p.presigned_put(url, b'x')
            self.assertNotIn('secret', str(exc.exception))
            self.assertTrue(exc.exception.__suppress_context__)
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
