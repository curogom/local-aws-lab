"""Actual spawned-worker SIGKILL and restart at commit boundaries."""
import multiprocessing as mp
import signal
import json
import time
from lab import RESULTS, apply, ledger, sqs_client
from scenario_support import queues, receive, percentile

def crash_worker(url, database, boundary, reached, recover=False):
    """A real spawned child. Parent SIGKILLs it at an explicitly signalled boundary."""
    client = sqs_client()
    db = ledger(database)
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            messages = receive(client, url)
            if not messages:
                continue
            message = messages[0]
            if not recover and boundary == 'before_commit':
                reached.set()
                time.sleep(60)
            apply(db, json.loads(message['Body']))
            if not recover and boundary == 'after_commit_before_ack':
                reached.set()
                time.sleep(60)
            client.delete_message(QueueUrl=url, ReceiptHandle=message['ReceiptHandle'])
            return
        raise TimeoutError('Worker did not receive a test message')
    finally:
        db.close()
        client.close()


def process_crashes():
    reports = []
    context = mp.get_context('spawn')
    with queues() as (client, create, run):
        for boundary in ['before_commit', 'after_commit_before_ack']:
            url = create(boundary, VisibilityTimeout=2)
            path = RESULTS / f'{run}-{boundary}.sqlite'
            ledger(path).close()
            client.send_message(QueueUrl=url, MessageBody=json.dumps({'id': boundary, 'text': 'synthetic'}))
            reached = context.Event()
            child = context.Process(target=crash_worker, args=(url, str(path), boundary, reached))
            recovery = None
            child.start()
            try:
                if not reached.wait(15):
                    raise TimeoutError(f'Crash boundary not reached: {boundary}')
                child.kill()  # Real SIGKILL, not an exception or close simulation.
                child.join(5)
                killed_code = child.exitcode
                db = ledger(path)
                rows_at_crash = db.execute('SELECT COUNT(*) FROM results').fetchone()[0]
                db.close()
                recovery = context.Process(target=crash_worker,
                                           args=(url, str(path), boundary, reached, True))
                recovery.start()
                recovery.join(20)
                if recovery.is_alive():
                    recovery.kill(); recovery.join()
                    raise TimeoutError('Recovery worker did not exit')
                db = ledger(path)
                rows_after = db.execute('SELECT COUNT(*) FROM results').fetchone()[0]
                db.close()
                empty = not receive(client, url)
                passed = (killed_code == -signal.SIGKILL and recovery.exitcode == 0
                          and rows_at_crash == int(boundary == 'after_commit_before_ack')
                          and rows_after == 1 and empty)
                reports.append({'boundary': boundary, 'killed_exitcode': killed_code,
                                'rows_at_crash': rows_at_crash, 'rows_after_restart': rows_after,
                                'queue_drained': empty, 'recovery_exitcode': recovery.exitcode,
                                'pass': passed})
            finally:
                for process in [child, recovery]:
                    if process is not None and process.is_alive():
                        process.kill(); process.join()
    return {'case': 'real_process_crashes', 'boundaries': reports, 'pass': all(r['pass'] for r in reports)}
