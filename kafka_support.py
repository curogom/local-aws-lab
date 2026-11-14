"""Real-process Kafka crash/rebalance experiments on the dedicated local broker.

No broker failover, multi-AZ test or external-side-effect exactly-once claim.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from lab import ROOT, RESULTS, apply, ledger

ENDPOINT = '127.0.0.1:19092'
PARTITIONS = 3


def emit(path, event):
    """Persist a worker observation before the parent can act on it."""
    record = dict(event, monotonic_s=time.monotonic())
    with Path(path).open('a') as stream:
        stream.write(json.dumps(record) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def observations(path):
    if not Path(path).exists():
        return []
    records = []
    for line in Path(path).read_text().splitlines():
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            # A writer may be between write and flush; re-read on the next poll.
            pass
    return records


def assignment(path):
    current = set()
    for event in observations(path):
        if event['event'] == 'assigned':
            current = set(event['partitions'])
        elif event['event'] == 'revoked':
            current.clear()
    return current


def wait_for(check, message, processes=(), seconds=35):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        for process in processes:
            if process.poll() is not None:
                raise RuntimeError(f'{message}: worker exited {process.returncode}')
        time.sleep(0.1)
    raise TimeoutError(message)


def client(group=None):
    from kafka import KafkaConsumer
    return KafkaConsumer(
        bootstrap_servers=ENDPOINT,
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        request_timeout_ms=15000,
        session_timeout_ms=6000,
        heartbeat_interval_ms=1000,
        max_poll_interval_ms=30000,
        max_poll_records=1,
        value_deserializer=json.loads,
    )


def worker(args):
    from kafka import ConsumerRebalanceListener

    class Listener(ConsumerRebalanceListener):
        def on_partitions_revoked(self, partitions):
            emit(args.log, {'event': 'revoked', 'partitions': sorted(p.partition for p in partitions)})

        def on_partitions_assigned(self, partitions):
            emit(args.log, {'event': 'assigned', 'partitions': sorted(p.partition for p in partitions)})

    consumer = client(args.group)
    db = ledger(args.db)
    applied = duplicates = 0
    consumer.subscribe([args.topic], listener=Listener())
    deadline = time.monotonic() + 120
    try:
        while time.monotonic() < deadline and not Path(args.stop).exists():
            batch = consumer.poll(timeout_ms=200)
            for messages in batch.values():
                for message in messages:
                    fresh = apply(db, message.value)
                    applied += int(fresh)
                    duplicates += int(not fresh)
                    if args.pause_before_commit:
                        emit(args.log, {
                            'event': 'effect_committed_before_offset',
                            'id': message.value['id'],
                            'partition': message.partition,
                            'offset': message.offset,
                            'effect_applied': fresh,
                        })
                        # Parent sends SIGKILL here. No normal close/finally path.
                        while True:
                            time.sleep(1)
                    consumer.commit()
                    emit(args.log, {
                        'event': 'processed', 'id': message.value['id'],
                        'partition': message.partition, 'offset': message.offset,
                        'applied': fresh,
                    })
        if time.monotonic() >= deadline:
            raise TimeoutError('Worker lifetime exceeded 120 seconds')
    finally:
        consumer.close(autocommit=False)
        db.close()
        emit(args.log, {'event': 'summary', 'applied': applied, 'duplicates': duplicates})


def row_count(path):
    db = ledger(path)
    try:
        return db.execute('SELECT count(*) FROM results').fetchone()[0]
    finally:
        db.close()


def group_offsets(topic, group, partitions):
    from kafka import TopicPartition
    observer = client(group)
    try:
        tps = [TopicPartition(topic, i) for i in range(partitions)]
        ends = observer.end_offsets(tps)
        committed = {str(tp.partition): observer.committed(tp) for tp in tps}
        end = {str(tp.partition): ends[tp] for tp in tps}
        return {
            'committed': committed,
            'end': end,
            'lag': sum(end[key] - (committed[key] or 0) for key in end),
        }
    finally:
        observer.close(autocommit=False)


from contextlib import contextmanager

@contextmanager
def resources():
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic

    if os.name != 'posix':
        raise RuntimeError('The real SIGKILL experiment requires a POSIX platform')
    RESULTS.mkdir(exist_ok=True)
    run_id = 'qlab-process-' + uuid.uuid4().hex[:10]
    folder = RESULTS / run_id
    folder.mkdir()
    children = []
    topics = []
    handles = []
    admin = KafkaAdminClient(bootstrap_servers=ENDPOINT, request_timeout_ms=15000)
    producer = KafkaProducer(
        bootstrap_servers=ENDPOINT, acks='all',
        value_serializer=lambda value: json.dumps(value).encode(),
        request_timeout_ms=15000,
    )

    def create(suffix, partitions):
        topic = run_id + '-' + suffix
        admin.create_topics([NewTopic(topic, partitions, 1,
            topic_configs={'retention.ms': '3600000', 'retention.bytes': '67108864'})])
        topics.append(topic)
        return topic

    def start(name, topic, group, db, pause=False):
        log = folder / (name + '.jsonl')
        stop = folder / (name + '.stop')
        output = (folder / (name + '.stderr.log')).open('w')
        handles.append(output)
        command = [sys.executable, str(Path(__file__).resolve()), 'worker',
                   '--topic', topic, '--group', group, '--db', str(db),
                   '--log', str(log), '--stop', str(stop)]
        if pause:
            command.append('--pause-before-commit')
        process = subprocess.Popen(command, stdout=output, stderr=output)
        children.append(process)
        return process, log, stop

    def stop(process, marker):
        marker.touch()
        process.wait(timeout=15)
        if process.returncode != 0:
            raise RuntimeError(f'Worker stop failed: {process.returncode}')

    try:
        yield dict(folder=folder, run_id=run_id, create=create, start=start,
                   stop=stop, producer=producer)
    finally:
        for process in children:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
        for handle in handles:
            handle.close()
        producer.close(timeout=10)
        if topics:
            admin.delete_topics(topics)
        admin.close()


def report(ctx, cases):
    result = {
        'run': ctx['run_id'], 'kind': 'kafka-process-scenarios',
        'broker': 'apache/kafka:4.4.0', 'cases': cases,
        'limitations': ['Single broker / replication factor 1',
            'SQLite transaction only; no external side-effect guarantee',
            'No broker failure, multi-AZ or throughput benchmark'],
        'pass': all(case['pass'] for case in cases),
    }
    (ctx['folder'] / 'report.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
    return result


def worker_cli():
    import argparse
    parser = argparse.ArgumentParser(description='Internal local Kafka worker')
    parser.add_argument('mode', choices=['worker'])
    for field in ('topic', 'group', 'db', 'log', 'stop'):
        parser.add_argument('--' + field, required=True)
    parser.add_argument('--pause-before-commit', action='store_true')
    worker(parser.parse_args())


if __name__ == '__main__':
    worker_cli()
