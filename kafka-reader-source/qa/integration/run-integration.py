#!/usr/bin/env python3
"""Test one desktop reader's headless entry point against a disposable real Kafka broker.

Requirements: Java21+, Python3, and a broker-classpath.txt generated from pom.xml.
Example: python3 run-integration.py --app csharp --exe /absolute/path/KafkaReader
"""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import queue
import shutil
import socket
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parent
WORK = ROOT / 'work'


def java_command(name):
    home = os.environ.get('JAVA_HOME')
    return str(Path(home) / 'bin' / name) if home else name


class Fixture:
    def __init__(self, classpath):
        self.log = (WORK / 'fixture-errors.log').open('w', encoding='utf-8')
        self.process = subprocess.Popen([java_command('java'), '-XX:-UsePerfData', '-Xmx256m', '-cp', classpath + os.pathsep + str(WORK), 'FixtureHost'], cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log, text=True, encoding='utf-8', bufsize=1)
        if not json.loads(self.process.stdout.readline()).get('ready'):
            raise RuntimeError('Fixture host did not start')

    def call(self, op, app, **fields):
        self.process.stdin.write(json.dumps(dict(op=op, app=app, **fields)) + '\n')
        self.process.stdin.flush()
        result = json.loads(self.process.stdout.readline())
        if result.get('error'): raise RuntimeError(result['error'])
        return result

    def close(self):
        self.process.stdin.close()
        try: self.process.wait(timeout=15)
        except subprocess.TimeoutExpired: self.process.kill(); self.process.wait()
        self.log.close()


def parse_summary(stdout):
    for line in reversed(stdout.splitlines()):
        try:
            value = json.loads(line)
            if isinstance(value, dict): return value
        except ValueError:
            pass
    raise AssertionError('Reader did not emit a JSON summary: ' + stdout[-1000:])


def test_reader(args, fixture):
    app = args.app
    output = WORK / app
    output.mkdir(exist_ok=True)
    (output / 'integration-report.json').unlink(missing_ok=True)
    info = fixture.call('seed', app)
    baseline = fixture.call('state', app)
    plain = output / 'plain.properties'
    restricted = output / 'restricted.properties'
    wrong = output / 'wrong-password.properties'
    plain.write_text('bootstrap.servers=127.0.0.1:19092\nsecurity.protocol=PLAINTEXT\nisolation.level=read_committed\ngroup.id=' + info['group'] + '\nenable.auto.commit=true\nenable.auto.offset.store=true\nallow.auto.create.topics=true\n')
    restricted.write_text('bootstrap.servers=127.0.0.1:19094\nsecurity.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.username=reader\nsasl.password=reader-fixture\nisolation.level=read_committed\n')
    wrong.write_text(restricted.read_text().replace('reader-fixture', 'wrong-fixture-password'))
    checks = []
    sequence = 0

    def check(condition, description):
        if not condition: raise AssertionError(description)
        checks.append(description)
        print(f'PASS {app} {len(checks)}: {description}', flush=True)

    def run(mode='earliest', value=0, partitions=None, maximum=1000, follow=False, config=plain, topic=None, inspect=False, after_reading=None, failure=False):
        nonlocal sequence
        sequence += 1
        export = output / f'records-{sequence:02d}.jsonl'
        command = [str(args.exe.resolve()), '--read-config', str(config), '--topic', topic or info['events'], '--mode', mode, '--value', str(value), '--max', str(maximum), '--seconds', '20', '--export', str(export)]
        if partitions is not None: command += ['--partitions', partitions]
        if follow: command += ['--follow']
        if inspect: command += ['--inspect']
        process = subprocess.Popen(command, cwd=args.exe.resolve().parent, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', bufsize=1)
        errors = []
        ready = threading.Event()
        def read_errors():
            for line in process.stderr:
                errors.append(line)
                if 'READING' in line.upper(): ready.set()
        thread = threading.Thread(target=read_errors, daemon=True)
        thread.start()
        if after_reading is not None:
            if not ready.wait(25):
                process.kill(); process.wait(); raise AssertionError('Reader did not signal READING after captured bounds: ' + ''.join(errors)[-1000:])
            after_reading()
        try:
            process.wait(timeout=45)
            stdout = process.stdout.read()
        finally:
            if process.poll() is None: process.kill(); process.wait()
        thread.join(timeout=2)
        (output / f'run-{sequence:02d}.log').write_text(stdout + '\nSTDERR\n' + ''.join(errors), encoding='utf-8')
        summary = parse_summary(stdout)
        records = [json.loads(line) for line in export.read_text(encoding='utf-8').splitlines() if line.strip()] if export.exists() else []
        if not failure and (process.returncode != 0 or str(summary.get('state', '')).upper() == 'FAILED'):
            raise AssertionError('Reader failed: ' + json.dumps(summary) + ' ' + ''.join(errors)[-2000:])
        return summary, records, process.returncode

    summary, _, _ = run(inspect=True)
    bounds = summary['partitions']
    check(len(bounds) == 3 and all(p.get('beginning', p.get('beginningOffset')) == 0 and p.get('end', p.get('endOffset')) == 20 for p in bounds), 'Explicit-topic metadata returns three partitions and exact bounds')
    _, earliest, _ = run()
    check(len(earliest) == 60, 'Finite earliest snapshot reads all 60 records')
    binary = next(r for r in earliest if r['partition'] == 0 and r['offset'] == 7)
    headers = binary['headers']
    check(base64.b64decode(binary['valueBase64']) == bytes([0,255,2]) and len(headers) == 4 and headers[0]['name'] == headers[1]['name'] == 'tag' and headers[2]['valueBase64'] is None and headers[3]['valueBase64'] == '', 'Binary payloads and ordered duplicate/null/empty headers survive the broker round trip')
    check(all(r['valueBase64'] is None for r in earliest if r['offset'] == 5) and all(r['valueBase64'] == '' for r in earliest if r['offset'] == 6), 'Null tombstones remain distinct from zero-byte values')
    _, recent, _ = run(mode='recent', value=5)
    check(len(recent) == 15 and all(r['offset'] >= 15 for r in recent), 'Recent window is five offset positions per partition')
    _, selected, _ = run(mode='offset', value=18, partitions='1')
    check(len(selected) == 2 and selected[0]['offset'] == 18 and all(r['partition'] == 1 for r in selected), 'Selected partition and explicit offset work')
    _, limited, _ = run(maximum=7)
    check(len(limited) == 7, 'Record limit is exact inside a fetched batch')
    _, timed, _ = run(mode='timestamp', value=info['epoch'] + 15000)
    check(len(timed) == 15 and all(r['timestamp'] >= info['epoch'] + 15000 for r in timed), 'Timestamp seek reads the requested range')
    _, future, _ = run(mode='timestamp', value=info['epoch'] + 999000)
    check(not future, 'Timestamp after the readable end returns zero records without rewinding')
    _, frozen, _ = run(after_reading=lambda: fixture.call('append', app, partition=0, value='after-capture'))
    check(len(frozen) == 60 and all(r['offset'] < 20 for r in frozen), 'Finite snapshot excludes an arrival after captured end offsets')
    _, following, _ = run(mode='latest', partitions='2', maximum=1, follow=True, after_reading=lambda: fixture.call('append', app, partition=2, value='new-follow-arrival'))
    check(len(following) == 1 and following[0]['offset'] == 20, 'Latest-follow mode reads a new arrival')
    _, topic_only, _ = run(config=restricted, mode='recent', value=1)
    check(len(topic_only) == 3, 'SASL/PLAIN topic-only account can read without consumer-group access')
    check(fixture.call('group-denied', app)['denied'], 'The restricted account is explicitly denied consumer-group commits')
    bad, _, code = run(config=restricted, topic=info['forbidden'], failure=True)
    check(code != 0 or str(bad.get('state', '')).upper() == 'FAILED', 'Unauthorized topic returns a controlled failure')
    bad, _, code = run(config=wrong, failure=True)
    check(code != 0 or str(bad.get('state', '')).upper() == 'FAILED', 'Wrong password returns a controlled failure')
    state = fixture.call('state', app)
    check(state['offsets'] == baseline['offsets'], 'Existing group offsets remain unchanged despite imported commit/group settings')
    check(state['groups'] == baseline['groups'], 'Reader sessions create no consumer groups')
    check(all(r.get('keyBase64') is None for r in earliest if r['offset'] == 4) and any('Москва' in (r.get('valueUtf8') or '') for r in earliest), 'JSONL export preserves null keys and Unicode values')
    _, committed, _ = run(topic=info['transactions'])
    check(len(committed) == 3 and all(not (r.get('valueUtf8') or '').startswith('aborted') for r in committed), 'read_committed skips aborted records and completes across transaction-marker gaps')
    fixture.call('open', app)
    try:
        started = time.monotonic()
        _, stable, _ = run(topic=info['transactions'])
        check(len(stable) == 3 and time.monotonic() - started < 15, 'Finite snapshot completes at the last stable offset while a transaction remains open')
    finally: fixture.call('abort', app)
    executable = args.exe.resolve()
    binaries = [executable, executable.with_suffix('.dll'), *executable.parent.glob('*librdkafka*')]
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in binaries if p.is_file()}
    report = dict(app=app, status='PASS', count=len(checks), checks=checks,
                  fixture='Apache Kafka 4.1.2, local KRaft, topic-only SASL/PLAIN reader',
                  completed_at_utc=datetime.now(timezone.utc).isoformat(),
                  tested_platform=platform.platform(), binary_sha256=hashes)
    (output / 'integration-report.json').write_text(json.dumps(report, indent=2) + '\n')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--app', required=True, choices=['csharp','rust'])
    parser.add_argument('--exe', type=Path, required=True)
    parser.add_argument('--classpath', type=Path, default=ROOT/'broker-classpath.txt')
    args = parser.parse_args()
    WORK.mkdir(exist_ok=True)
    broker_data = WORK / 'broker-data'
    if broker_data.exists(): shutil.rmtree(broker_data)
    cp = args.classpath.read_text().strip()
    subprocess.run([java_command('javac'), '--release', '21', '-proc:none', '-cp', cp, '-d', str(WORK), str(ROOT/'FixtureHost.java')], check=True)
    base = [java_command('java'), '-XX:-UsePerfData', '-Xmx512m', '-cp', cp]
    with (WORK/'broker.log').open('w') as log:
        subprocess.run(base + ['kafka.tools.StorageTool','format','-t','5whIxlPKSMORypa0_jVW_w','-c','server.properties'], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
        broker = subprocess.Popen(base + ['kafka.Kafka','server.properties'], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        fixture = None
        try:
            deadline = time.monotonic()+60
            while True:
                if broker.poll() is not None: raise RuntimeError('Broker exited; inspect work/broker.log')
                try:
                    with socket.create_connection(('127.0.0.1',19092),timeout=1): break
                except OSError:
                    if time.monotonic()>deadline: raise RuntimeError('Broker startup timed out')
                    time.sleep(.2)
            fixture=Fixture(cp)
            report=test_reader(args,fixture)
            print(json.dumps(report),flush=True)
        finally:
            if fixture: fixture.close()
            broker.terminate()
            try: broker.wait(timeout=12)
            except subprocess.TimeoutExpired: broker.kill();broker.wait()


if __name__=='__main__': main()
