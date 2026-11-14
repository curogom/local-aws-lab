"""Native retry/DLQ and explicit manual repair/requeue."""
import json
import time
from lab import RESULTS, apply, ledger, sqs_client
from scenario_support import queues, receive, percentile

def retry_and_dlq():
    """Transient failure succeeds on attempt 2; poison is repaired after native DLQ."""
    with queues() as (client, create, run):
        dead = create('dead')
        arn = client.get_queue_attributes(QueueUrl=dead, AttributeNames=['QueueArn'])['Attributes']['QueueArn']
        source = create('retry', VisibilityTimeout=1,
                        RedrivePolicy=json.dumps({'deadLetterTargetArn': arn, 'maxReceiveCount': '3'}))
        path = RESULTS / f'{run}-retry.sqlite'
        db = ledger(path)
        attempts = {'transient': 0, 'poison': 0}
        client.send_message(QueueUrl=source, MessageBody=json.dumps({'id': 'transient', 'text': 'synthetic'}))
        client.send_message(QueueUrl=source, MessageBody=json.dumps({'id': 'poison', 'text': 'synthetic'}))
        dlq_message = None
        deadline = time.monotonic() + 25
        try:
            while time.monotonic() < deadline:
                for message in receive(client, source):
                    event = json.loads(message['Body']); attempts[event['id']] += 1
                    # Failure means no acknowledgement. Native visibility expiry retries it.
                    if event['id'] == 'poison' or attempts['transient'] == 1:
                        continue
                    apply(db, event)
                    client.delete_message(QueueUrl=source, ReceiptHandle=message['ReceiptHandle'])
                dead_messages = receive(client, dead)
                if dead_messages:
                    dlq_message = dead_messages[0]
                if dlq_message and attempts['transient'] >= 2:
                    break
            if not dlq_message:
                raise AssertionError('Poison message did not reach native DLQ')
            failed = json.loads(dlq_message['Body'])
            # Explicit manual repair/requeue, NOT a claim about AWS redrive-task API support.
            client.send_message(QueueUrl=source, MessageBody=json.dumps(failed))
            client.delete_message(QueueUrl=dead, ReceiptHandle=dlq_message['ReceiptHandle'])
            repaired = receive(client, source)
            if not repaired:
                raise AssertionError('Repaired message did not become available')
            apply(db, json.loads(repaired[0]['Body']))
            client.delete_message(QueueUrl=source, ReceiptHandle=repaired[0]['ReceiptHandle'])
            rows = db.execute('SELECT COUNT(*) FROM results').fetchone()[0]
            passed = attempts == {'transient': 2, 'poison': 3} and failed['id'] == 'poison' and rows == 2
            return {'case': 'retry_native_dlq_and_manual_repair', 'source_attempts': attempts,
                    'native_max_receive_count': 3, 'repaired_rows': rows,
                    'redrive_method': 'application manual requeue after fixing synthetic failure', 'pass': passed}
        finally:
            db.close()

