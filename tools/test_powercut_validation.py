"""Require actual lost sharing and a verified recovery, not just a reboot."""
import json
import unittest

from test_btrfs_powercut_qemu import records, validate


def fixture():
    cut = json.dumps(dict(completed=1, unconfirmed=1, autocommit=True)) + '\r\n'
    base = dict(errors=[], stale=0, unsupported=0, btrfs_subvolumes=1,
                scanner_fallbacks=[], files_seen=0, bytes_hashed=0,
                jobs_attempted=0, discovery_jobs=0, jobs_completed=0, bytes_deduped=0)
    replay = dict(base, files_seen=1, jobs_completed=1, jobs_attempted=1, bytes_deduped=4096)
    return cut, replay, base


def serial(replay, idle):
    return (json.dumps(replay, indent=2) + '\r\n' + json.dumps(idle, indent=2)
            + '\r\nDEDUPIT_POWER_RECOVERY_OK\r\n')


class PowerCutValidationTests(unittest.TestCase):
    def test_pretty_json_records_and_complete_recovery_pass(self):
        cut, replay, idle = fixture()
        self.assertEqual(records(serial(replay, idle)), [replay, idle])
        validate(cut, serial(replay, idle))

    def test_cut_must_follow_durable_unconfirmed_completion(self):
        cut, replay, idle = fixture()
        for key, value in [('completed', 0), ('unconfirmed', 0), ('autocommit', False)]:
            state = dict(json.loads(cut), **{key: value})
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                validate(json.dumps(state), serial(replay, idle))

    def test_missing_rollback_or_native_discovery_or_success_fails(self):
        cut, replay, idle = fixture()
        for key, value in [('bytes_deduped', 0), ('jobs_completed', 0),
                           ('bytes_hashed', 4096), ('btrfs_subvolumes', 0),
                           ('scanner_fallbacks', ['portable']), ('errors', ['failure'])]:
            sample = dict(replay, **{key: value})
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                validate(cut, serial(sample, idle))

    def test_incomplete_or_nonidle_or_skipped_recovery_fails(self):
        cut, replay, idle = fixture()
        for output in [serial(replay, dict(idle, files_seen=1)),
                       serial(replay, idle).replace('DEDUPIT_POWER_RECOVERY_OK', ''),
                       serial(replay, idle) + 'SKIP: native\n',
                       json.dumps(replay) + '\nDEDUPIT_POWER_RECOVERY_OK\n']:
            with self.subTest(output=output), self.assertRaises(RuntimeError):
                validate(cut, output)
