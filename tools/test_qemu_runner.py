"""Regression tests for the mandatory privileged CI test gate."""
import importlib.util
from pathlib import Path
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location(
    "qemu_runner", Path(__file__).with_name("test-btrfs-qemu.py")
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class AcceleratorTests(unittest.TestCase):
    def test_auto_uses_accessible_kvm_and_otherwise_tcg(self):
        for available, expected in [(True, 'kvm'), (False, 'tcg')]:
            with self.subTest(available=available), mock.patch.object(runner.os, 'access', return_value=available):
                self.assertEqual(runner.accelerator('auto'), expected)

    def test_explicit_accelerator_never_silently_changes(self):
        with mock.patch.object(runner.os, 'access', return_value=False):
            self.assertEqual(runner.accelerator('kvm'), 'kvm')
            self.assertEqual(runner.accelerator('tcg'), 'tcg')


def serial(passed=5, failed=0, ignored=0, filtered=0, marker="0"):
    return (
        f"test result: ok. {passed} passed; {failed} failed; {ignored} ignored; "
        f"0 measured; {filtered} filtered out; finished in 9.04s\n"
        f"DEDUPIT_GUEST_EXIT={marker}\n"
    )


class GuestValidationTests(unittest.TestCase):
    def test_complete_suite_passes(self):
        runner.validate_guest(serial(), 0, 5)

    def test_incomplete_or_skipped_suite_fails(self):
        cases = [
            serial(passed=0),
            serial(passed=4, ignored=1),
            serial(passed=4, filtered=1),
            serial(passed=4, failed=1),
            serial(passed=4),
            serial() + "SKIP: scanner unavailable\n",
            "DEDUPIT_GUEST_EXIT=0\n",
        ]
        for output in cases:
            with self.subTest(output=output), self.assertRaises(RuntimeError):
                runner.validate_guest(output, 0, 5)

    def test_guest_failure_or_missing_shutdown_fails(self):
        for output, code in [(serial(marker="1"), 0), (serial(), 1),
                             (serial().split("DEDUPIT")[0], 0)]:
            with self.subTest(output=output, code=code), self.assertRaises(RuntimeError):
                runner.validate_guest(output, code, 5)

    def test_empty_test_binary_fails(self):
        with self.assertRaises(RuntimeError):
            runner.validate_guest(serial(passed=0), 0, 0)

    def test_missing_batch_crash_feature_or_test_fails(self):
        listing = "\n".join(name + ": test" for name in runner.REQUIRED_BATCH_TESTS)
        self.assertEqual(runner.validate_listing(listing), len(runner.REQUIRED_BATCH_TESTS))
        for output in ["", "ordinary_discovery: test", listing.split("\n", 1)[1]]:
            with self.subTest(output=output), self.assertRaises(RuntimeError):
                runner.validate_listing(output)


if __name__ == "__main__":
    unittest.main()
