#!/usr/bin/env python3
"""Build a reproducible source checkpoint with an honest verification ledger."""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

parser = argparse.ArgumentParser()
parser.add_argument('--status-file', type=Path, required=True)
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
status = json.loads(args.status_file.read_text())
result = subprocess.run(
    ['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'],
    cwd=root, capture_output=True, check=True,
)
names = sorted(set(name.decode() for name in result.stdout.split(b'\0') if name))
source_roots = {'api', 'cmd', 'config', 'docs', 'internal', 'scripts', 'test', '.github'}
source_files = {
    '.dockerignore', '.gitignore', '.golangci.yml', 'AGENTS.md', 'CONTRIBUTING.md',
    'Dockerfile.manager', 'Dockerfile.worker', 'KubeFleet_Codex_Build_Specification.md',
    'LICENSE', 'Makefile', 'NOTICE', 'README.md', 'SECURITY.md', 'go.mod', 'go.sum',
    'versions.lock.yaml',
}
names = [name for name in names if name in source_files or name.split('/')[0] in source_roots]
evidence_names = sorted(
    str(path.relative_to(root)) for path in (root / 'evidence/verified').glob('*')
    if path.is_file() and path.name != 'checkpoint.json'
)
hashes = {}
evidence_hashes = {}
tar_bytes = io.BytesIO()
with tarfile.open(fileobj=tar_bytes, mode='w', format=tarfile.PAX_FORMAT) as archive:
    for name in names + evidence_names:
        path = root / name
        if path.is_symlink() or not path.is_file():
            raise SystemExit('Unexpected source entry: ' + name)
        data = path.read_bytes()
        target_hashes = evidence_hashes if name in evidence_names else hashes
        target_hashes[name] = hashlib.sha256(data).hexdigest()
        entry = tarfile.TarInfo(name)
        entry.size = len(data)
        entry.mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
        entry.uid = entry.gid = entry.mtime = 0
        archive.addfile(entry, io.BytesIO(data))
package = root / 'evidence/packages/kubefleet-development-checkpoint.tar.gz'
package.parent.mkdir(parents=True, exist_ok=True)
with package.open('wb') as target:
    with gzip.GzipFile(fileobj=target, mode='wb', filename='', mtime=0) as compressed:
        compressed.write(tar_bytes.getvalue())
commit = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=root, capture_output=True, text=True)
ledger = {
    'schemaVersion': 'v1', 'status': 'development-checkpoint-release-incomplete',
    'recordedAt': datetime.now(timezone.utc).isoformat(),
    'commit': commit.stdout.strip() if commit.returncode == 0 else None,
    'sourceTreeSHA256': hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest(),
    'sourceFiles': hashes,
    'evidenceFiles': evidence_hashes,
    'versionsLockSHA256': hashlib.sha256((root / 'versions.lock.yaml').read_bytes()).hexdigest(),
    'package': {
        'path': str(package.relative_to(root)), 'bytes': package.stat().st_size,
        'sha256': hashlib.sha256(package.read_bytes()).hexdigest(),
        'sourceFileCount': len(names),
        'evidenceFileCount': len(evidence_names),
    },
    'verification': status,
}
out = root / 'evidence/verified/checkpoint.json'
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(ledger, indent=2) + '\n')
print(json.dumps({'package': ledger['package'], 'sourceTreeSHA256': ledger['sourceTreeSHA256']}))
