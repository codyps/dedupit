#!/usr/bin/env python3
"""Compare validated native profiles only when their VM/fixture settings match."""
import argparse
import json
from pathlib import Path


def compare(before, after):
    for report in [before, after]:
        if report.get('validated') is not True:
            raise RuntimeError('both benchmark runs must pass full verification')
    if before['environment']['cli_sha256'] == after['environment']['cli_sha256']:
        raise RuntimeError('baseline and current executable must be distinct builds')
    for key in ['accelerator', 'cpus', 'memory_mib', 'data_gib', 'state_gib',
                'qemu_version', 'data_mount', 'index_mount', 'build']:
        if before['environment'][key] != after['environment'][key]:
            raise RuntimeError(f'incomparable VM setting: {key}')
    for key in ['files', 'repetitions', 'chunk_bytes', 'file_bytes', 'groups',
                'large_bytes', 'large_controls', 'partial_bytes', 'partial_controls',
                'kernel_release', 'sqlite_version']:
        if before['fixture'][key] != after['fixture'][key]:
            raise RuntimeError(f'incomparable fixture: {key}')
    result = {'files': after['fixture']['files'], 'accelerator': after['environment']['accelerator']}
    for name, report in [('before', before), ('after', after)]:
        apply = next(s for s in report['samples'] if s['case'] == 'initial-apply')
        following = next(s for s in report['samples'] if s['case'] == 'post-apply-reconcile')
        result[name] = {'source_commit': report['environment']['git_commit'],
                        'apply_seconds': apply['seconds'], 'first_followup_seconds': following['seconds'],
                        'combined_seconds': apply['seconds'] + following['seconds'],
                        'combined_cpu_seconds': apply['cpu_seconds'] + following['cpu_seconds'],
                        'first_followup_files_seen': following['stats']['files_seen']}
    result['combined_change_percent'] = (result['after']['combined_seconds'] / result['before']['combined_seconds'] - 1) * 100
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('before', type=Path)
    parser.add_argument('after', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = compare(json.loads(args.before.read_text()), json.loads(args.after.read_text()))
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
