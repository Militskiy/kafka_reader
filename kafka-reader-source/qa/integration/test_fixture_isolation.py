"""Offline regressions for Kafka fixture ownership and startup cleanup."""
import io
import json
from pathlib import Path
import runpy
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


runner = runpy.run_path(str(Path(__file__).with_name('run-integration.py')))
run_fixture = runner['run_fixture']


class FixtureIsolationTests(unittest.TestCase):
    def test_foreign_cluster_is_rejected_before_seeding_and_processes_are_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary) / 'work'
            previous = work / 'broker-data'
            previous.mkdir(parents=True)
            sentinel = previous / 'previous-run'
            sentinel.write_text('preserve me')
            classpath = Path(temporary) / 'broker-classpath.txt'
            classpath.write_text(str(Path(temporary) / 'library with spaces.jar'))
            args = SimpleNamespace(exe=Path(sys.executable), classpath=classpath, app='rust')
            broker, helper = MagicMock(), MagicMock()
            for child in (broker, helper):
                child.__enter__.return_value = child
                child.poll.return_value = None
            helper.stdout = io.StringIO('{"clusterId":"unrelated"}\n')

            def verify_start(command, **options):
                session = options['cwd']
                self.assertIn(work, session.parents)
                self.assertNotEqual(session, previous)
                config = (session / 'server.properties').read_text()
                self.assertIn('log.dirs=broker-data', config)
                self.assertNotIn('{plain_port}', config)
                if 'FixtureHost' in command:
                    self.assertNotEqual(command[-3], 'unrelated')
                    return helper
                self.assertIn('kafka.Kafka', command)
                return broker

            with patch.dict(run_fixture.__globals__, WORK=work), \
                    patch('subprocess.run') as command, \
                    patch('subprocess.Popen', side_effect=verify_start), \
                    self.assertRaisesRegex(RuntimeError, 'newly formatted cluster'):
                run_fixture(args)
            self.assertTrue(any('format' in c.args[0] for c in command.call_args_list))
            helper.stdin.write.assert_not_called()
            helper.stdin.close.assert_called_once()
            helper.wait.assert_called_once()
            broker.terminate.assert_called_once()
            broker.wait.assert_called_once()
            self.assertEqual(sentinel.read_text(), 'preserve me')
            run_dir, = work.glob('run-*')
            self.assertFalse(any(run_dir.glob('cluster-*')))

    def test_broker_exit_blocks_further_fixture_commands(self):
        helper, broker = MagicMock(), MagicMock()
        helper.stdout = io.StringIO(json.dumps({'clusterId': 'owned'}) + '\n')
        broker.poll.return_value = None
        fixture = runner['Fixture'](helper, broker, 'owned')
        broker.poll.return_value = 1
        with self.assertRaisesRegex(RuntimeError, 'broker exited'):
            fixture.call('seed', 'rust')
        helper.stdin.write.assert_not_called()

    def test_concurrent_reservations_allocate_distinct_ports(self):
        with runner['reserve_ports']() as first, runner['reserve_ports']() as second:
            self.assertEqual(len(set(first + second)), 6)

    def test_client_profiles_stay_in_temporary_session_on_reader_failure(self):
        args = SimpleNamespace(exe=Path(sys.executable), app='rust')
        fixture = MagicMock()
        fixture.call.side_effect = [{'events': 'fixture-events', 'group': 'fixture-group'}, {}]
        with tempfile.TemporaryDirectory() as root:
            run_dir = Path(root)
            with tempfile.TemporaryDirectory(dir=run_dir) as temporary:
                session = Path(temporary)
                with patch('subprocess.Popen', side_effect=OSError('reader failed')), \
                        self.assertRaisesRegex(OSError, 'reader failed'):
                    runner['test_reader'](args, fixture, run_dir, session, 1234, 5678, 'fixture-only')
                self.assertEqual(sorted(p.name for p in session.glob('*.properties')),
                                 ['plain.properties', 'restricted.properties', 'wrong-password.properties'])
            self.assertTrue((run_dir / args.app).is_dir())
            self.assertFalse(list(run_dir.rglob('*.properties')))


if __name__ == '__main__':
    unittest.main()
