"""Capture reproducibility metadata, never container environment or credentials."""
import datetime
import importlib.metadata
import json
import platform
import subprocess
from lab import RESULTS


def output(command):
    return subprocess.check_output(command, text=True, timeout=15).strip()


def record():
    metadata = {
        'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'os': platform.system(), 'architecture': platform.machine(), 'python': platform.python_version(),
        'packages': {name: importlib.metadata.version(name) for name in ['boto3', 'botocore', 'kafka-python']},
        'scope': 'local functional experiments, not production throughput',
        'images': {},
    }
    for name in ['queue-reliability-emulator:local', 'apache/kafka:4.4.0']:
        try:
            metadata['images'][name] = json.loads(output(['docker', 'image', 'inspect', name,
                                                         '--format', '{{json .Id}}']))
        except subprocess.CalledProcessError:
            metadata['images'][name] = 'not built/pulled yet'
    metadata['aws_emulator_version'] = output(['docker', 'run', '--rm', '--network', 'none', '--entrypoint', 'rpm', 'queue-reliability-emulator:local', '-q', '--qf', '%{VERSION}', 'aws-emulator'])
    try:
        metadata['commit'] = output(['git', 'rev-parse', 'HEAD'])
    except subprocess.CalledProcessError:
        metadata['commit'] = 'uncommitted source copy'
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    return metadata


if __name__ == '__main__':
    print(json.dumps(record(), indent=2))
