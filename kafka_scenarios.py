"""Run independent local Kafka crash and/or consumer-rebalance experiments."""
import argparse
import sys
from kafka_support import resources, report, worker_cli


def run(case='all'):
    with resources() as ctx:
        cases = []
        if case in ('crash', 'all'):
            from kafka_crash import run_case
            cases.append(run_case(ctx))
        if case in ('rebalance', 'all'):
            from kafka_rebalance import run_case
            cases.append(run_case(ctx))
        return report(ctx, cases)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'worker':
        worker_cli()
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('mode', choices=['run'], nargs='?', default='run')
        parser.add_argument('--case', choices=['crash', 'rebalance', 'all'], default='all')
        options = parser.parse_args()
        raise SystemExit(0 if run(options.case)['pass'] else 2)
