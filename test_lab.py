import concurrent.futures, pathlib, tempfile, unittest
from lab import ledger, apply
class IdempotencyTest(unittest.TestCase):
 def test_duplicate_effect_and_parallel_unique_constraint(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=pathlib.Path(tmp)/'test.sqlite';ledger(p).close()
   def work(i):
    db=ledger(p);r=apply(db,{'id':str(i%10),'text':'test'});db.close();return r
   with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex: results=list(ex.map(work,range(100)))
   db=ledger(p);self.assertEqual(sum(results),10);self.assertEqual(db.execute('SELECT count(*) FROM results').fetchone()[0],10);db.close()
class SafetyTest(unittest.TestCase):
 def test_endpoint_is_fixed_to_local_lab(self):
  import inspect, lab
  source=inspect.getsource(lab.sqs_client)
  self.assertIn("endpoint_url='https://127.0.0.1:9298'",source)
  self.assertIn("aws_access_key_id=section['aws_access_key_id']",source)
  self.assertIn("verify=str(runtime/'ca.pem')",source)
  self.assertNotIn('assert ',source)
 def test_existing_aws_profile_is_not_used(self):
  import os, lab
  from unittest.mock import patch
  with tempfile.TemporaryDirectory() as tmp:
   root=pathlib.Path(tmp); runtime=root/'.runtime'/'emulator'/'9298';runtime.mkdir(parents=True)
   (runtime/'emulator_credentials').write_text('[default]\naws_access_key_id=local-test\naws_secret_access_key=local-test\n')
   with patch.object(lab,'ROOT',root), patch.dict(os.environ,{'AWS_PROFILE':'nonexistent-test-profile','AWS_ENDPOINT_URL':'https://example.invalid','AWS_EC2_METADATA_DISABLED':'true'}):
    client=lab.sqs_client()
    self.assertEqual(client.meta.endpoint_url,'https://127.0.0.1:9298')
    self.assertEqual(client._request_signer._credentials.access_key,'local-test')
    client.close()
if __name__=='__main__': unittest.main()


