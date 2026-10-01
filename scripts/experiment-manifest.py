#!/usr/bin/env python3
"""Attach measured experiment provenance without changing the raw summary."""
import argparse
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--summary', type=Path, required=True)
parser.add_argument('--profile', choices=['isolated', 'shared', 'priority'], required=True)
parser.add_argument('--repetition', type=int, required=True)
parser.add_argument('--out', type=Path, required=True)
args = parser.parse_args()
directory = args.summary.parent
prefix = args.summary.name.removesuffix('-summary.json')
digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
summary = json.loads(args.summary.read_text())
trace_hash = digest(directory / 'trace.json')
if summary['traceSHA256'] != trace_hash:
    raise SystemExit('Summary and immutable input trace hashes disagree')
commit = (directory / 'commit.txt').read_text().strip()
quality = list(summary['quality'])
if commit == 'uncommitted' or len(commit) != 40:
    commit = None
    quality.append('uncommitted_source_snapshot')
incomplete = any(summary[key] for key in ['unfinished', 'submissionFailures', 'ambiguousUnresolvedCreates'])
status = 'incomplete' if incomplete else ('complete_with_observation_limits' if quality else 'complete')
manifest = {
    'schemaVersion': 'v1', 'experimentID': directory.name,
    'profile': args.profile, 'repetition': args.repetition,
    'commitSHA': commit, 'traceSHA256': trace_hash,
    'configurationSHA256': digest(directory / (prefix + '-profile.yaml')),
    'versionsLockSHA256': digest(directory / 'versions.lock.yaml'),
    'workerImageID': (directory / 'worker-image-id.txt').read_text().strip(),
    'host': json.loads((directory / 'docker.json').read_text()),
    'capacityCaveat': 'All kind nodes share one Docker VM; node capacities are not additive host capacity.',
    'qualityStatus': status, 'qualityReasons': quality,
    'rawSummarySHA256': digest(args.summary), 'measurements': summary,
}
with args.out.open('x') as output:
    json.dump(manifest, output, indent=2)
    output.write('\n')
