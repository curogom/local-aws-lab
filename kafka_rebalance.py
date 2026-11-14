"""Independent local Kafka rebalance experiment."""
from kafka_support import (resources, report, ledger, observations, wait_for,
                           group_offsets, row_count, assignment, PARTITIONS)


def run_case(ctx):
    folder = ctx['folder']
    create, start, stop = ctx['create'], ctx['start'], ctx['stop']
    producer = ctx['producer']
    # Join a second consumer, process together, then leave and reassign to A.
    topic = create('rebalance', PARTITIONS)
    group = topic + '-group'
    db = folder / 'rebalance.sqlite'
    ledger(db).close()
    a, log_a, stop_a = start('consumer-a', topic, group, db)
    all_parts = set(range(PARTITIONS))
    wait_for(lambda: assignment(log_a) == all_parts, 'A did not own every partition', [a])
    b, log_b, stop_b = start('consumer-b', topic, group, db)

    def joined():
        aa, bb = assignment(log_a), assignment(log_b)
        if aa and bb and not aa.intersection(bb) and aa.union(bb) == all_parts:
            return {'a': sorted(aa), 'b': sorted(bb)}
        return None

    join_assignment = wait_for(joined, 'Group did not split partitions after B joined', [a, b])
    for i in range(120):
        producer.send(topic, partition=i % PARTITIONS,
            value={'id': f'event-{i}', 'text': 'synthetic'}).get(timeout=15)
    wait_for(lambda: row_count(db) == 120, 'Joined consumers did not drain first batch', [a, b])
    # Commit can follow the row write; check broker progress before the leave.
    wait_for(lambda: group_offsets(topic, group, PARTITIONS)['lag'] == 0,
             'Joined consumers did not commit', [a, b])
    stop(b, stop_b)
    wait_for(lambda: assignment(log_a) == all_parts, 'A did not recover B partitions', [a])
    for i in range(120, 180):
        producer.send(topic, partition=i % PARTITIONS,
            value={'id': f'event-{i}', 'text': 'synthetic'}).get(timeout=15)
    wait_for(lambda: row_count(db) == 180, 'Remaining consumer did not drain second batch', [a])
    wait_for(lambda: group_offsets(topic, group, PARTITIONS)['lag'] == 0,
             'Final offsets not committed', [a])
    leave_assignment = sorted(assignment(log_a))
    stop(a, stop_a)
    offsets = group_offsets(topic, group, PARTITIONS)
    events_a, events_b = observations(log_a), observations(log_b)
    summary_a = next(e for e in events_a if e['event'] == 'summary')
    summary_b = next(e for e in events_b if e['event'] == 'summary')
    rebalance = {
        'case': 'concurrent_consumer_group_join_and_leave',
        'partitions': PARTITIONS,
        'assignment_after_join': join_assignment,
        'assignment_after_leave': leave_assignment,
        'a_processed': sum(e['event'] == 'processed' for e in events_a),
        'b_processed': sum(e['event'] == 'processed' for e in events_b),
        'a_revoke_events': sum(e['event'] == 'revoked' for e in events_a),
        'a_summary': summary_a,
        'b_summary': summary_b,
        'stored_rows': row_count(db),
        'input_unique_events': 180,
        'offsets': offsets,
        'pass': row_count(db) == 180 and offsets['lag'] == 0
                and summary_a['applied'] > 0 and summary_b['applied'] > 0
                and any(e['event'] == 'revoked' for e in events_a)
                and leave_assignment == list(range(PARTITIONS)),
    }
    return rebalance


def run():
    with resources() as ctx:
        return report(ctx, [run_case(ctx)])


if __name__ == '__main__':
    raise SystemExit(0 if run()['pass'] else 2)
