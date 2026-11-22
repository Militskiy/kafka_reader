#!/usr/bin/env python3
"""Check exact-topic metadata and read one record through a local Kafka bridge."""
import argparse
from contextlib import ExitStack, contextmanager
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import tempfile
import threading


@contextmanager
def start_bridge(script):
    script = script.resolve(strict=True)
    npx = shutil.which('npx.cmd' if os.name == 'nt' else 'npx')
    if not npx:
        raise FileNotFoundError('Install npm with npx to start the bridge')
    node = subprocess.check_output(
        [npx, '--yes', '--package=node@22', 'node', '-p', 'process.execPath'],
        text=True, encoding='utf-8', timeout=180,
    ).strip()
    with subprocess.Popen([node, str(script), 'kafka', '9192'], stdout=subprocess.PIPE) as bridge:
        startup = queue.Queue()

        def read_output():
            bound = False
            for line in bridge.stdout:
                if not bound and line.startswith(b'Kafka on localhost:9192 '):
                    bound = True
                    startup.put(True)
            if not bound:
                startup.put(False)

        observer = threading.Thread(target=read_output, daemon=True)
        try:
            observer.start()
            try:
                bound = startup.get(timeout=20)
            except queue.Empty:
                raise RuntimeError('Timed out waiting for the bridge to bind port 9192') from None
            if not bound or bridge.poll() is not None:
                raise RuntimeError('The bridge exited before confirming its listener on port 9192')
            print(f'Test bridge listening on port 9192 (PID {bridge.pid})', flush=True)
            yield
            if bridge.poll() is not None:
                raise RuntimeError('The bridge exited during the reader check')
        finally:
            if bridge.poll() is None:
                bridge.terminate()
            try:
                bridge.wait(timeout=5)
            except subprocess.TimeoutExpired:
                bridge.kill()
                bridge.wait()
            if observer.ident is not None:
                observer.join()
            print(f'Test bridge process exited (PID {bridge.pid})', flush=True)


def check_bootstrap(profile):
    addresses = {}
    for line in profile.read_text(encoding='utf-8').split('\n'):
        key, _, value = line.partition('=')
        key = key.strip()
        if key.lower() in ('bootstrap.servers', 'metadata.broker.list'):
            addresses[key] = value.strip().lower()
    if 'bootstrap.servers' not in addresses or any(
        value not in ('127.0.0.1:9192', 'localhost:9192') for value in addresses.values()
    ):
        raise ValueError('Use a bridge profile with bootstrap.servers=127.0.0.1:9192 or localhost:9192')


def check_reader(exe, profile, topic):
    run = subprocess.run(
        [str(exe.resolve()), '--read-config', str(profile), '--topic', topic,
         '--mode', 'earliest', '--max', '1', '--seconds', '15'],
        stdout=subprocess.PIPE, text=True, encoding='utf-8', timeout=45, check=True,
    )
    summary = json.loads(run.stdout)
    if summary['state'] != 'COMPLETED':
        raise ValueError(f"Reader finished in state {summary['state']}")
    partitions = summary['partitions']
    seen = set()
    for bounds in partitions:
        partition, begin, end = (bounds[key] for key in ('partition', 'beginning', 'end'))
        if partition < 0 or partition in seen or not 0 <= begin <= end:
            raise ValueError('Reader returned inconsistent partition bounds')
        seen.add(partition)
    if not seen:
        raise ValueError('Reader returned no partitions for the exact topic')
    print(f'PASS: received bounds for {len(seen)} partition(s)', flush=True)
    if summary['count'] != 1 or summary['skipped'] != 0:
        raise ValueError(f"Expected one readable record. {summary['detail']}. "
                         f"Delivered {summary['count']}, skipped {summary['skipped']}.")
    print('PASS: finite read delivered one record')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--exe', type=Path, required=True, help='built Kafka Reader executable')
    parser.add_argument('--topic', required=True, help='exact topic with a readable record')
    parser.add_argument('--config', type=Path, help='optional librdkafka profile for the bridge')
    parser.add_argument('--bridge-script', type=Path, help='start and stop this Sondahub bridge using Node 22')
    args = parser.parse_args()
    exe = args.exe.resolve(strict=True)
    with ExitStack() as resources:
        profile = args.config
        if profile:
            check_bootstrap(profile)
        else:
            directory = resources.enter_context(tempfile.TemporaryDirectory())
            profile = Path(directory) / 'bridge.properties'
            profile.write_text('bootstrap.servers=127.0.0.1:9192\n', encoding='utf-8')
        if args.bridge_script:
            resources.enter_context(start_bridge(args.bridge_script))
        check_reader(exe, profile, args.topic)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.SubprocessError) as error:
        raise SystemExit(f'Bridge check failed: {error}')
