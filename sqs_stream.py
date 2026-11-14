"""Bounded admission, concurrent production and slow-consumer recovery."""
import concurrent.futures
import threading
import json
import time
from lab import RESULTS, apply, ledger, sqs_client
from scenario_support import queues, receive, percentile

def bounded_stream(total=1200, capacity=64):
    """Concurrent producer/consumers with admission credits, not a preloaded queue."""
    with queues() as (client, create, run):
        url = create('stream', VisibilityTimeout=15)
        path = RESULTS / f'{run}-stream.sqlite'
        ledger(path).close()
        credits = threading.BoundedSemaphore(capacity)
        lock = threading.Lock()
        finished = threading.Event()
        producer_done = threading.Event()
        state = {'accepted': 0, 'completed': 0, 'blocked_admissions': 0, 'max_outstanding': 0}
        e2e = []; timeline = []; errors = []
        started = time.monotonic()
        deadline = started + 90

        def producer():
            local = sqs_client()
            try:
                for index in range(total):
                    wait_started = time.monotonic()
                    if not credits.acquire(timeout=max(0, deadline - time.monotonic())):
                        raise TimeoutError('Admission deadline exceeded')
                    blocked = time.monotonic() - wait_started
                    event = {'id': f'stream-{index}', 'text': 'synthetic', 'sent_at': time.time()}
                    with lock:
                        state['accepted'] += 1
                        state['blocked_admissions'] += int(blocked > .005)
                        state['max_outstanding'] = max(state['max_outstanding'], state['accepted'] - state['completed'])
                    local.send_message(QueueUrl=url, MessageBody=json.dumps(event))
                producer_done.set()
            except Exception as exc:
                errors.append(repr(exc)); finished.set(); raise
            finally:
                local.close()

        def worker():
            local = sqs_client(); db = ledger(path)
            try:
                # Start late to deliberately fill the bounded admission window.
                time.sleep(.5)
                while not finished.is_set() and time.monotonic() < deadline:
                    messages = local.receive_message(QueueUrl=url, WaitTimeSeconds=1,
                                                     MaxNumberOfMessages=10).get('Messages', [])
                    for message in messages:
                        event = json.loads(message['Body'])
                        # Slow period followed by recovery, still synthetic work.
                        time.sleep(.02 if time.monotonic() - started < 2 else .001)
                        fresh = apply(db, event)
                        local.delete_message(QueueUrl=url, ReceiptHandle=message['ReceiptHandle'])
                        if fresh:
                            with lock:
                                state['completed'] += 1
                                e2e.append((time.time() - event['sent_at']) * 1000)
                            credits.release()
                    with lock:
                        if producer_done.is_set() and state['completed'] == total:
                            finished.set()
                if not finished.is_set():
                    raise TimeoutError('Stream did not drain')
            except Exception as exc:
                errors.append(repr(exc)); finished.set(); raise
            finally:
                db.close(); local.close()

        def monitor():
            while not finished.wait(.1):
                with lock:
                    timeline.append({'elapsed_s': round(time.monotonic() - started, 3),
                                     'outstanding': state['accepted'] - state['completed']})

        watcher = threading.Thread(target=monitor, daemon=True); watcher.start()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(producer), executor.submit(worker), executor.submit(worker)]
            for future in futures:
                future.result(timeout=95)
        finished.set(); watcher.join(2)
        db = ledger(path); rows = db.execute('SELECT COUNT(*) FROM results').fetchone()[0]; db.close()
        elapsed = time.monotonic() - started
        passed = (not errors and rows == total and state['accepted'] == state['completed'] == total
                  and state['max_outstanding'] <= capacity and state['blocked_admissions'] > 0)
        return {'case': 'bounded_concurrent_stream', 'messages': total, 'capacity': capacity,
                **state, 'stored_rows': rows, 'elapsed_s': round(elapsed, 3),
                'throughput_local_msg_s': round(total / elapsed, 2),
                'e2e_ms': {'p50': percentile(e2e, 50), 'p95': percentile(e2e, 95), 'p99': percentile(e2e, 99)},
                'metric_definition': 'send-admission timestamp to successful effect+ack, single host clock',
                'timeline': timeline, 'remaining_outstanding': state['accepted'] - state['completed'], 'pass': passed}
