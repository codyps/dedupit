import copy
import unittest

from compare_btrfs_profiles import compare


def report():
    return dict(validated=True,
                environment=dict(accelerator='kvm', cpus=2, memory_mib=4096, data_gib=24,
                                 state_gib=8, qemu_version='8.2.2', data_mount='btrfs',
                                 index_mount='ext4', build='release', git_commit='test'),
                fixture=dict(files=100000, repetitions=3, chunk_bytes=131072, file_bytes=4096,
                             groups=128, large_bytes=8388608, large_controls=2,
                             partial_bytes=393216, partial_controls=2,
                             kernel_release='6.8', sqlite_version='3.46'),
                samples=[dict(case=name, seconds=1, cpu_seconds=1, stats=dict(files_seen=0))
                         for name in ['initial-apply', 'post-apply-reconcile']])


class ComparisonTests(unittest.TestCase):
    def test_equivalent_conditions_pass_and_include_followup(self):
        data = compare(report(), report())
        self.assertEqual(data['after']['combined_seconds'], 2)
        self.assertEqual(data['combined_change_percent'], 0)

    def test_different_accelerators_resources_or_scale_fail(self):
        for section, key, value in [('environment', 'accelerator', 'tcg'),
                                    ('environment', 'memory_mib', 2048),
                                    ('fixture', 'files', 1000000),
                                    ('fixture', 'kernel_release', '6.18')]:
            after = copy.deepcopy(report())
            after[section][key] = value
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                compare(report(), after)

    def test_unverified_runs_cannot_be_compared(self):
        after = report()
        after['validated'] = False
        with self.assertRaises(RuntimeError):
            compare(report(), after)
