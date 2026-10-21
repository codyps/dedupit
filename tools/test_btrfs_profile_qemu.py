"""Prevent partial, fallback, or unverified native scale runs from passing."""
import copy
import unittest

from profile_btrfs_qemu import REQUIRED, REPEATED, validate


def report():
    files, repetitions = 1024, 3
    samples = []
    for name in sorted(REQUIRED):
        for _ in range(repetitions if name in REPEATED else 1):
            stats = dict(errors=[], stale=0, unsupported=0, btrfs_subvolumes=1,
                         scanner_fallbacks=[], files_seen=0, files_hashed=0,
                         bytes_hashed=0, jobs_attempted=0, discovery_jobs=0)
            if name == 'initial-native-index':
                stats['files_hashed'] = files
            if name == 'oracle-full-scan':
                stats['files_hashed'] = files + repetitions
                stats['btrfs_subvolumes'] = 0
            samples.append(dict(case=name, stats=stats))
    return dict(fixture=dict(files=files), samples=samples,
                result=dict(verified_files=files+repetitions, contents_verified=True,
                            identities_preserved=True, native_matches_oracle=True,
                            initial_deduped_bytes=4096, initial_partial_range_jobs=1,
                            initial_whole_file_jobs=files-4-128+1))


class ProfileValidationTests(unittest.TestCase):
    def test_complete_native_run_passes(self):
        validate(report(), 'DEDUPIT_GUEST_EXIT=0\n', 0, 1024, 3)

    def test_missing_sample_result_or_requested_scale_fails(self):
        cases = []
        for key in ['contents_verified', 'identities_preserved', 'native_matches_oracle']:
            data = report()
            data['result'][key] = False
            cases.append(data)
        data = report()
        data['samples'].pop()
        cases.append(data)
        data = report()
        data['fixture']['files'] = 100
        cases.append(data)
        data = report()
        data['result']['initial_partial_range_jobs'] = 0
        cases.append(data)
        data = report()
        next(s for s in data['samples'] if s['case'] == 'initial-native-index')['stats']['files_hashed'] = 0
        cases.append(data)
        for index, data in enumerate(cases):
            with self.subTest(case=index), self.assertRaises(RuntimeError):
                validate(data, 'DEDUPIT_GUEST_EXIT=0\n', 0, 1024, 3)

    def test_fallback_errors_and_idle_old_file_work_fail(self):
        for key, value in [('btrfs_subvolumes', 0), ('scanner_fallbacks', ['portable']),
                           ('errors', ['failed']), ('stale', 1), ('unsupported', 1),
                           ('files_seen', 1), ('bytes_hashed', 4096), ('jobs_attempted', 1)]:
            data = report()
            idle = next(s for s in data['samples'] if s['case'] == 'idle-after-apply')
            idle['stats'][key] = value
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                validate(data, 'DEDUPIT_GUEST_EXIT=0\n', 0, 1024, 3)

    def test_guest_exit_and_skip_markers_are_required(self):
        for serial, code in [('', 0), ('DEDUPIT_GUEST_EXIT=1\n', 0),
                             ('DEDUPIT_GUEST_EXIT=0\nSKIP: native\n', 0),
                             ('DEDUPIT_GUEST_EXIT=0\n', 1)]:
            with self.subTest(serial=serial, code=code), self.assertRaises(RuntimeError):
                validate(copy.deepcopy(report()), serial, code, 1024, 3)
