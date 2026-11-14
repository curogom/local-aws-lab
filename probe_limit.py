"""Reproduce the observed emulator-only queue capacity limit with synthetic payloads."""
if not __debug__: raise RuntimeError('Run this test lab without -O/PYTHONOPTIMIZE')
import json, pathlib, uuid
from lab import sqs_client, RESULTS
RESULTS.mkdir(exist_ok=True)
s=sqs_client();u=s.create_queue(QueueName='qlab-limit-'+uuid.uuid4().hex[:10])['QueueUrl']; result={}
try:
 for i in range(100):
  r=s.send_message_batch(QueueUrl=u,Entries=[{'Id':str(j),'MessageBody':'synthetic-limit-probe'} for j in range(10)])
  assert not r.get('Failed')
 try:
  s.send_message(QueueUrl=u,MessageBody='synthetic-over-limit')
  result={'observed_limit':False,'note':'Version may have changed'}
 except s.exceptions.OverLimit as e:
  result={'observed_limit':True,'queued_before_error':1000,'code':e.response['Error']['Code'],'message':e.response['Error']['Message'],'scope':'Local AWS Emulator only, not actual SQS quota; actual version recorded in environment.json'}
finally:
 s.delete_queue(QueueUrl=u)
pathlib.Path(__file__).with_name('results').joinpath('emulator-limit-probe.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))
