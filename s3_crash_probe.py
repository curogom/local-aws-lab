"""Real child-process SIGKILL probes against the dedicated local S3 emulator."""
import json
import os
import signal
import subprocess
import sys
import uuid

from s3_pipeline import ROOT, client, digest, output_key_for, process, read, upload


def worker(bucket, key, expected_hash, point):
    s3 = client()
    if point == 'after_read_before_write':
        original = s3.get_object
        def get_then_die(**kwargs):
            response = original(**kwargs)
            response['Body'].read()
            response['Body'].close()
            os.kill(os.getpid(), signal.SIGKILL)
        s3.get_object = get_then_die
    elif point == 'after_write_before_response':
        original = s3.put_object
        def put_then_die(**kwargs):
            original(**kwargs)  # Wait for the actual S3 HTTP success before dying.
            os.kill(os.getpid(), signal.SIGKILL)
        s3.put_object = put_then_die
    else:
        raise ValueError('Unsupported crash point')
    process(s3, bucket, key, expected_hash)
    raise RuntimeError('Worker unexpectedly survived')


def run():
    bucket = 'part2-crash-' + uuid.uuid4().hex[:12]
    report = {'scope': 'real child SIGKILL after completed S3 requests; not in-flight network loss',
              'cases': [], 'cleanup': False}
    s3 = None
    created = False
    try:
        s3 = client()
        s3.create_bucket(Bucket=bucket)
        created = True
        body = 'S3 process crash\n실제 재시도\n'.encode()
        for point in ('after_read_before_write', 'after_write_before_response'):
            key = upload(s3, bucket, point.replace('_', '-'), body)
            child = subprocess.run([sys.executable, __file__, '--worker', bucket, key,
                                    digest(body), point], capture_output=True, timeout=30)
            if child.returncode != -signal.SIGKILL:
                raise RuntimeError('Child did not terminate with SIGKILL')
            output = output_key_for(key)
            objects = s3.list_objects_v2(Bucket=bucket, Prefix=output).get('Contents', [])
            before = len(objects)
            if before != int(point == 'after_write_before_response'):
                raise AssertionError('Unexpected pre-retry output state')
            prior = read(s3, bucket, output) if before else None
            # Separate fresh OS process models restarting the worker, not a function retry.
            code = ('from s3_pipeline import client, process; import sys; '
                    'process(client(), sys.argv[1], sys.argv[2], sys.argv[3])')
            subprocess.run([sys.executable, '-c', code, bucket, key, digest(body)],
                           cwd=ROOT, check=True, capture_output=True, timeout=30)
            after = read(s3, bucket, output)
            _, expected = process(s3, bucket, key, digest(body))
            final = s3.list_objects_v2(Bucket=bucket, Prefix=output).get('Contents', [])
            if after != expected or read(s3, bucket, output) != expected or len(final) != 1 or (prior is not None and prior != after):
                raise AssertionError('Retry did not converge')
            report['cases'].append({'case': point, 'pass': True, 'child_returncode': child.returncode,
                                    'retry_process_returncode': 0, 'objects_before_retry': before,
                                    'objects_after_retry': len(final), 'output_sha256': digest(after)})
        return report
    except Exception as exc:
        report['error_type'] = type(exc).__name__
        raise
    finally:
        try:
            if created:
                for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket):
                    for item in page.get('Contents', []):
                        s3.delete_object(Bucket=bucket, Key=item['Key'])
                s3.delete_bucket(Bucket=bucket)
                report['cleanup'] = True
        except Exception as exc:
            report['cleanup_error_type'] = type(exc).__name__
            if 'error_type' not in report:
                raise
        finally:
            (ROOT / 'results').mkdir(exist_ok=True)
            (ROOT / 'results/s3-crash-probe.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--worker':
        worker(*sys.argv[2:])
    else:
        run()
        print('Real SIGKILL probes passed; results/s3-crash-probe.json')
