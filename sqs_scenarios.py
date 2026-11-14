"""Entry point for bounded local-only SQS reliability scenarios."""
import json
from lab import RESULTS
from sqs_crash import process_crashes
from sqs_retry import retry_and_dlq
from sqs_stream import bounded_stream

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('case', choices=['crash', 'retry', 'stream', 'all'], default='all', nargs='?')
    args = parser.parse_args()
    RESULTS.mkdir(exist_ok=True)
    cases = {'crash': process_crashes, 'retry': retry_and_dlq, 'stream': bounded_stream}
    results = [function() for name, function in cases.items() if args.case in [name, 'all']]
    report = {'kind': 'sqs-advanced', 'cases': results, 'pass': all(case['pass'] for case in results)}
    (RESULTS / f'sqs-advanced-{args.case}.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if report['pass'] else 2)
