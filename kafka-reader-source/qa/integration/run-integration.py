#!/usr/bin/env python3
"""Test one desktop reader's headless entry point against a disposable real Kafka broker.

Requirements: Java21+, Python3, and a broker-classpath.txt generated from pom.xml.
Example: python3 run-integration.py --app csharp --exe /absolute/path/KafkaReader
"""
import argparse
import base64
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import queue
import secrets
import socket
from string import Template
import subprocess
import tempfile
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parent
WORK = ROOT / 'work'


def java_command(name):
    home = os.environ.get('JAVA_HOME')
    return str(Path(home) / 'bin' / name) if home else name


class Fixture:
    def __init__(self, process, broker, cluster_id):
        self.process = process
        self.broker = broker
        self.replies = queue.Queue()
        threading.Thread(target=self.read_output, daemon=True).start()
        if self.reply().get('clusterId') != cluster_id:
            raise RuntimeError('Fixture helper did not verify the newly formatted cluster')

    def read_output(self):
        try:
            for line in self.process.stdout:
                self.replies.put(line)
        finally:
            self.replies.put(None)

    def reply(self):
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.broker.poll() is not None:
                raise RuntimeError('Test broker exited; see broker.log in the run directory')
            try:
                line = self.replies.get(timeout=0.5)
            except queue.Empty:
                continue
            if line is None:
                raise RuntimeError('Fixture helper exited; see fixture-errors.log in the run directory')
            return json.loads(line)
        raise TimeoutError('Timed out waiting for the fixture helper')

    def call(self, op, app, **fields):
        if self.broker.poll() is not None:
            raise RuntimeError('Test broker exited before the fixture operation')
        self.process.stdin.write(json.dumps(dict(op=op, app=app, **fields)) + '\n')
        self.process.stdin.flush()
        result = self.reply()
        if result.get('error'): raise RuntimeError(result['error'])
        return result


@contextmanager
def reserve_ports(count=3):
    with ExitStack() as sockets:
        ports = []
        for _ in range(count):
            listener = sockets.enter_context(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            listener.bind(('127.0.0.1', 0))
            ports.append(listener.getsockname()[1])
        yield ports


def stop_process(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=12)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def close_helper(process):
    try:
        process.stdin.close()
        process.wait(timeout=15)
    except (BrokenPipeError, subprocess.TimeoutExpired):
        stop_process(process)


def parse_summary(stdout):
    for line in reversed(stdout.splitlines()):
        try:
            value = json.loads(line)
            if isinstance(value, dict): return value
        except ValueError:
            pass
    raise AssertionError('Reader did not emit a JSON summary: ' + stdout[-1000:])


def test_reader(args, fixture, run_dir, session, plain_port, client_port, reader_password,
                issuer=None, oauth_port=None):
    app = args.app
    output = run_dir / app
    output.mkdir()
    info = fixture.call('seed', app)
    baseline = fixture.call('state', app)
    plain = session / 'plain.properties'
    restricted = session / 'restricted.properties'
    wrong = session / 'wrong-password.properties'
    plain.write_text(f'bootstrap.servers=127.0.0.1:{plain_port}\nsecurity.protocol=PLAINTEXT\nisolation.level=read_committed\ngroup.id=' + info['group'] + '\nenable.auto.commit=true\nenable.auto.offset.store=true\nallow.auto.create.topics=true\n', encoding='utf-8')
    restricted.write_text(f'bootstrap.servers=127.0.0.1:{client_port}\nsecurity.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.username=reader\nsasl.password={reader_password}\nisolation.level=read_committed\n', encoding='utf-8')
    wrong.write_text(restricted.read_text(encoding='utf-8').replace(reader_password, 'wrong-fixture-password'), encoding='utf-8')
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
        env = dict(os.environ, NO_PROXY='127.0.0.1,localhost', no_proxy='127.0.0.1,localhost') if issuer else None
        process = subprocess.Popen(command, cwd=args.exe.resolve().parent, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, encoding='utf-8', bufsize=1, env=env)
        errors = []
        ready = threading.Event()
        def read_errors():
            for line in process.stderr:
                errors.append(line)
                if 'READING' in line.upper(): ready.set()
        thread = threading.Thread(target=read_errors, daemon=True)
        thread.start()
        try:
            if after_reading is not None:
                if not ready.wait(25):
                    raise AssertionError('Reader did not signal READING after captured bounds: ' + ''.join(errors)[-1000:])
                after_reading()
            process.wait(timeout=45)
            stdout = process.stdout.read()
        finally:
            if process.poll() is None: process.kill(); process.wait()
            thread.join(timeout=2)
            process.stdout.close()
            process.stderr.close()
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
    if issuer:
        # The fixture CA exists only in this session and was never installed in the OS.
        untrusted = session / 'oauth-untrusted.properties'
        profile = issuer.profile(oauth_port).read_text(encoding='utf-8')
        untrusted.write_text('\n'.join(line for line in profile.splitlines()
                                       if not line.startswith('oauth.ssl.ca.location=')) + '\n', encoding='utf-8')
        failed, _, code = run(config=untrusted, failure=True)
        check(code != 0 and failed['state'] == 'FAILED' and issuer.grants == 0,
              'An untrusted HTTPS issuer is rejected before it receives the refresh token')
        meta, _, _ = run(config=issuer.profile(oauth_port), inspect=True)
        check(issuer.grants >= 1 and len(meta['partitions']) == 3,
              'HTTPS refresh-token exchange authenticates for exact-topic metadata over SASL_SSL')

        expected_grants = issuer.grants + 2
        def append_after_refresh():
            issuer.wait_for_grants(expected_grants)
            fixture.call('append', app, partition=1, value='oauth-after-renewal')

        _, records, _ = run(config=issuer.profile(oauth_port), mode='latest', partitions='1',
                            maximum=1, follow=True, after_reading=append_after_refresh)
        check(issuer.grants >= expected_grants, 'An active reader renews using the rotated refresh token')
        check(len(records) == 1 and records[0]['valueUtf8'] == 'oauth-after-renewal',
              'The OAuth-authenticated topic-only reader receives a record after renewal')
        failed, _, code = run(config=issuer.profile(oauth_port, invalid=True), failure=True)
        check(code != 0 and failed['state'] == 'FAILED' and '400' in failed['detail'],
              'A rejected refresh token surfaces the identity provider HTTP status')
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
                  fixture='Apache Kafka 4.1.2, local KRaft, topic-only SASL/PLAIN reader'
                          + (' and TLS OAuth refresh-token listener' if issuer else ''),
                  completed_at_utc=datetime.now(timezone.utc).isoformat(),
                  tested_platform=platform.platform(), binary_sha256=hashes)
    (output / 'integration-report.json').write_text(json.dumps(report, indent=2) + '\n')
    return report


def run_fixture(args):
    args.exe = args.exe.resolve(strict=True)
    deps = args.classpath.read_text(encoding='utf-8-sig').strip()
    if not deps:
        raise ValueError('The Maven classpath file is empty')
    WORK.mkdir(exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix='run-', dir=WORK))
    print(f'Fixture output: {run_dir}', flush=True)
    cluster_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode('ascii').rstrip('=')
    admin_password = secrets.token_hex(16)
    reader_password = secrets.token_hex(16)
    with tempfile.TemporaryDirectory(prefix='cluster-', dir=run_dir) as temporary, ExitStack() as resources:
        session = Path(temporary)
        # An argument file keeps the Maven classpath out of Windows' command-line length limit.
        entries = [Path(entry).resolve().as_posix() for entry in deps.split(os.pathsep)]
        cp = os.pathsep.join([session.as_posix(), *entries])
        (session / 'classpath.args').write_text('-classpath\n"' + cp.replace('\\', '\\\\').replace('"', '\\"') + '"\n', encoding='utf-8')
        subprocess.run([java_command('javac'), '--release', '21', '-encoding', 'UTF-8', '-proc:none',
                        '@classpath.args', '-d', '.', str(ROOT/'FixtureHost.java')], cwd=session, check=True)
        java = [java_command('java'), '-XX:-UsePerfData', '-Dfile.encoding=UTF-8', '-Xmx512m', '@classpath.args']
        log = resources.enter_context((run_dir / 'broker.log').open('w', encoding='utf-8'))
        issuer = None
        if getattr(args, 'oauth', False):
            from oauth_fixture import start_issuer
            issuer = resources.enter_context(start_issuer(session))
        with reserve_ports(4 if issuer else 3) as ports:
            plain_port, controller_port, client_port = ports[:3]
            oauth_port = ports[3] if issuer else None
            template = Template((ROOT / 'server.properties').read_text(encoding='utf-8'))
            config = template.substitute(plain_port=plain_port, controller_port=controller_port,
                                         client_port=client_port, admin_password=admin_password,
                                         reader_password=reader_password,
                                         oauth_listener=f',OAUTH://127.0.0.1:{oauth_port}' if issuer else '',
                                         oauth_protocol=',OAUTH:SASL_SSL' if issuer else '')
            if issuer:
                config += '\n' + issuer.broker_config()
            (session / 'server.properties').write_text(config, encoding='utf-8')
            subprocess.run(java + ['kafka.tools.StorageTool', 'format', '--cluster-id=' + cluster_id, '-c', 'server.properties'],
                           cwd=session, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=60)
        broker = resources.enter_context(subprocess.Popen(java + ['kafka.Kafka', 'server.properties'],
                                         cwd=session, stdout=log, stderr=subprocess.STDOUT))
        resources.callback(stop_process, broker)
        helper_log = resources.enter_context((run_dir / 'fixture-errors.log').open('w', encoding='utf-8'))
        process = resources.enter_context(subprocess.Popen(
            java + ['FixtureHost', cluster_id, str(plain_port), str(client_port)], cwd=session,
            env=dict(os.environ, KAFKA_FIXTURE_ADMIN_PASSWORD=admin_password, KAFKA_FIXTURE_READER_PASSWORD=reader_password),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=helper_log, text=True, encoding='utf-8', bufsize=1))
        resources.callback(close_helper, process)
        fixture = Fixture(process, broker, cluster_id)
        report = test_reader(args, fixture, run_dir, session, plain_port, client_port, reader_password,
                             issuer, oauth_port)
        print(json.dumps(report), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--app', required=True, choices=['csharp','rust'])
    parser.add_argument('--exe', type=Path, required=True)
    parser.add_argument('--classpath', type=Path, default=ROOT/'broker-classpath.txt')
    parser.add_argument('--oauth', action='store_true', help='Test OAuth against an ephemeral HTTPS issuer and SASL_SSL Kafka listener (requires OpenSSL)')
    args = parser.parse_args()
    if args.oauth and args.app != 'rust':
        parser.error('--oauth is supported by the Rust reader')
    run_fixture(args)


if __name__=='__main__': main()
