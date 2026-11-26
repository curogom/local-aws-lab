"""Exercise the real shell entry points with isolated, deterministic Docker doubles."""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parent
EMULATOR = 'queue-reliability-emulator'
KAFKA = 'queue-reliability-kafka'


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for directory in ('scripts', '.venv/bin', 'bin'):
            (self.root / directory).mkdir(parents=True)
        for name in ('run-all.sh', 'kafka-start.sh', 'lab_session.py',
                     'scripts/session.sh', 'scripts/emulator-start.sh',
                     'scripts/emulator-stop.sh', 'scripts/kafka-stop.sh', 'scripts/s3-run.sh'):
            shutil.copy2(ROOT / name, self.root / name)
        self.env = dict(os.environ, PATH=str(self.root / 'bin') + os.pathsep + os.environ['PATH'],
                        XDG_CACHE_HOME=str(self.root / 'cache'), FIXTURE=str(self.root))
        self.env.pop('LOCAL_AWS_LAB_LOCK_FD', None)
        self.executable('.venv/bin/python', '''
import os, pathlib, sys, time
root = pathlib.Path(os.environ['FIXTURE'])
args = sys.argv[1:]
if args and args[0].endswith('lab_session.py'):
    os.execv(sys.executable, [sys.executable, *args])
workload = args[0] if args else ''
if workload == os.environ.get('GATE_WORKLOAD'):
    (root / 'ready').touch()
    deadline = time.monotonic() + 10
    while not (root / 'release').exists():
        if time.monotonic() > deadline: sys.exit(98)
        time.sleep(.01)
if workload == os.environ.get('FAIL_WORKLOAD'): sys.exit(42)
''')
        self.executable('bin/curl', '''
from pathlib import Path
import sys
ca = Path(sys.argv[sys.argv.index('--cacert') + 1])
if not ca.read_text():
    sys.exit(1)
''')
        self.executable('bin/docker', '''
import json, os, pathlib, sys
root = pathlib.Path(os.environ['FIXTURE'])
state_path = root / 'state.json'
state = json.loads(state_path.read_text()) if state_path.exists() else {}
args = sys.argv[1:]
with (root / 'events').open('a') as log: log.write(json.dumps(args) + '\\n')
command = args[0]
if command == 'inspect':
    item = state.get(args[-1])
    if item is None: sys.exit(1)
    if '-f' in args:
        print(str(item['running'] if 'State.Running' in args[2] else item['owned']).lower())
elif command == 'exec': print('1.0.385')
elif command == 'container':
    if os.environ.get('FAIL_INSPECT'): sys.exit(9)
    name = args[-1].removeprefix('name=^/').removesuffix('$')
    if name in state: print(name)
elif command in ('run', 'start'):
    name = args[args.index('--name') + 1] if command == 'run' else args[-1]
    state[name] = {'running': True, 'owned': True}
    if name.endswith('emulator'):
        ca = root / '.runtime/emulator/9298/ca.pem'
        ca.parent.mkdir(parents=True, exist_ok=True)
        ca.write_text('fake test CA')
elif command == 'stop':
    name = args[-1]
    if os.environ.get('FAIL_STOP') == name: sys.exit(9)
    if name.endswith('emulator'): state.pop(name, None)
    else: state[name]['running'] = False
state_path.write_text(json.dumps(state))
''')

    def executable(self, name, source):
        path = self.root / name
        path.write_text('#!' + sys.executable + '\n' + source)
        path.chmod(0o755)

    def start(self, script='run-all.sh', **environment):
        process = subprocess.Popen([str(self.root / script)], cwd=self.root,
                                   env=dict(self.env, **environment), text=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   start_new_session=True)
        self.addCleanup(self.reap, process)
        return process

    def reap(self, process):
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.communicate(timeout=5)

    def finish(self, process):
        out, err = process.communicate(timeout=15)
        return process.returncode, out, err

    def events(self):
        path = self.root / 'events'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_competing_runners_and_stop_do_not_touch_active_session(self):
        first = self.start(GATE_WORKLOAD='lab.py')
        deadline = time.monotonic() + 10
        while not (self.root / 'ready').exists():
            self.assertIsNone(first.poll(), 'First runner exited before the overlap boundary')
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.01)
        before = self.events()
        for script in ('run-all.sh', 'scripts/s3-run.sh', 'scripts/emulator-stop.sh', 'scripts/kafka-stop.sh'):
            with self.subTest(script=script):
                status, _, err = self.finish(self.start(script))
                self.assertEqual(status, 75, err)
                self.assertEqual(self.events(), before)
        other = self.root / 'other-checkout'
        for name in ('run-all.sh', 'lab_session.py', 'scripts/session.sh', '.venv/bin/python'):
            (other / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.root / name, other / name)
        status, _, err = self.finish(self.start(str(other / 'run-all.sh'), FIXTURE=str(other)))
        self.assertEqual(status, 75, err)
        self.assertFalse((other / 'events').exists())
        (self.root / 'release').touch()
        status, _, err = self.finish(first)
        self.assertEqual(status, 0, err)
        status, _, err = self.finish(self.start('scripts/s3-run.sh'))
        self.assertEqual(status, 0, err)  # Existing lock file is harmless after release.

    def test_docker_inspection_failure_is_not_silently_ignored(self):
        # A stop command must distinguish an absent container from an unavailable daemon.
        status, _, err = self.finish(self.start('scripts/emulator-stop.sh', FAIL_INSPECT='1'))
        self.assertNotEqual(status, 0, err)

    def test_cleanup_rejects_foreign_container(self):
        (self.root / 'state.json').write_text(json.dumps({EMULATOR: {'running': True, 'owned': False}}))
        status, _, err = self.finish(self.start('scripts/emulator-stop.sh'))
        self.assertEqual(status, 1, err)
        self.assertFalse(any(event[0] == 'stop' for event in self.events()))

    def test_preexisting_emulator_is_never_stopped(self):
        (self.root / 'state.json').write_text(json.dumps({EMULATOR: {'running': True, 'owned': True}}))
        for script in ('run-all.sh', 'scripts/s3-run.sh'):
            with self.subTest(script=script):
                self.assertNotEqual(self.finish(self.start(script))[0], 0)
        self.assertFalse(any(event[0] == 'stop' for event in self.events()))

    def test_foreign_kafka_is_never_stopped(self):
        (self.root / 'state.json').write_text(json.dumps({KAFKA: {'running': False, 'owned': False}}))
        self.assertNotEqual(self.finish(self.start())[0], 0)
        self.assertFalse(any(event[0] in ('stop', 'run', 'start') for event in self.events()))

    def test_cleanup_failure_fails_otherwise_successful_run(self):
        status, _, err = self.finish(self.start('scripts/s3-run.sh', FAIL_STOP=EMULATOR))
        self.assertEqual(status, 1, err)
        self.assertIn('cleanup failed', err)

    def test_primary_failure_survives_cleanup_failure(self):
        status, _, err = self.finish(self.start(FAIL_WORKLOAD='s3_pipeline.py', FAIL_STOP=EMULATOR))
        self.assertEqual(status, 42, err)
        self.assertIn('cleanup failed', err)
        self.assertFalse(any(event[0] == 'run' and KAFKA in event for event in self.events()))

    def test_failed_start_is_cleaned_up(self):
        status, _, err = self.finish(self.start('scripts/s3-run.sh', EXPECTED_EMULATOR_VERSION='invalid'))
        self.assertEqual(status, 1, err)
        self.assertIn(['stop', EMULATOR], self.events())
        self.assertNotIn(EMULATOR, json.loads((self.root / 'state.json').read_text()))

    def test_internal_marker_cannot_bypass_lock(self):
        result = subprocess.run([str(self.root / 'scripts/emulator-stop.sh'), '--lab-lock-held'],
                                env=self.env, capture_output=True, text=True, timeout=5)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.events(), [])


if __name__ == '__main__':
    unittest.main()
