"""Independent local Kafka crash experiment."""
from kafka_support import (resources, report, ledger, observations, wait_for,
                           group_offsets, row_count, assignment, PARTITIONS)
import signal


def run_case(ctx):
    folder = ctx['folder']
    create, start, stop = ctx['create'], ctx['start'], ctx['stop']
    producer = ctx['producer']
    # A real SIGKILL, with an observable committed-effect/pre-offset barrier.
    topic = create('crash', 1)
    group = topic + '-group'
    db = folder / 'crash.sqlite'
    ledger(db).close()
    producer.send(topic, partition=0, value={'id': 'crash-1', 'text': 'synthetic'}).get(timeout=15)
    first, first_log, _ = start('crash-first', topic, group, db, pause=True)
    boundary = wait_for(lambda: next((e for e in observations(first_log)
        if e['event'] == 'effect_committed_before_offset'), None),
        'No commit-before-offset boundary', [first])
    before = group_offsets(topic, group, 1)
    if row_count(db) != 1 or before['committed']['0'] not in (None, 0):
        raise AssertionError('Crash precondition failed')
    first.kill()
    first.wait(timeout=10)
    if first.returncode != -signal.SIGKILL:
        raise AssertionError('Worker was not killed by SIGKILL')
    recovery, recovery_log, recovery_stop = start('crash-recovery', topic, group, db)
    wait_for(lambda: any(e['event'] == 'processed' for e in observations(recovery_log)),
             'Recovery did not replay the record', [recovery], seconds=40)
    stop(recovery, recovery_stop)
    after = group_offsets(topic, group, 1)
    summary = next(e for e in observations(recovery_log) if e['event'] == 'summary')
    crash = {
        'case': 'real_sigkill_after_sqlite_commit_before_offset',
        'worker_returncode': first.returncode,
        'boundary': boundary,
        'offsets_before_kill': before,
        'offsets_after_recovery': after,
        'recovery_applied': summary['applied'],
        'recovery_duplicates': summary['duplicates'],
        'stored_rows': row_count(db),
        'pass': summary['applied'] == 0 and summary['duplicates'] == 1
                and row_count(db) == 1 and after['lag'] == 0,
    }

    return crash


def run():
    with resources() as ctx:
        return report(ctx, [run_case(ctx)])


if __name__ == '__main__':
    raise SystemExit(0 if run()['pass'] else 2)
