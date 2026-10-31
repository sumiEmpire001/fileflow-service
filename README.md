# Fileflow Service

Resumable local file-ingestion engine with SQLite metadata, bounded chunks, SHA-256 verification, owner scoping, literal metadata search and expiry cleanup. Standard-library Python 3.10+.

```sh
python -m unittest discover -s tests -v
python demo.py
```

```python
from fileflow import Uploads
import hashlib
store = Uploads('local-storage')
data = b'synthetic sample'
upload = store.begin('demo-owner', 'sample.txt', len(data), hashlib.sha256(data).hexdigest())
store.append(upload, 'demo-owner', 0, data[:5])
store.append(upload, 'demo-owner', 5, data[5:])
store.finish(upload, 'demo-owner')
assert b''.join(store.read_chunks(upload, 'demo-owner')) == data
```

Identical committed chunk retries return the current offset; mismatching retries and gaps fail. A retry truncates bytes written beyond the committed offset. Finalisation recovers a verified object renamed before its metadata commit. Checksum failures persist as failed uploads. Search and all object operations require the matching owner.

`purge_expired()` permanently removes expired file bytes and generated orphan files; metadata remains. Expiry is checked when a read begins. Concurrent cleanup can interrupt reads. Call cleanup explicitly and inspect its error list.

Limits: 64 MiB per file, 1 MiB per chunk, TTL 1–86400 seconds. This is a library backend with a runnable demo; HTTP transport and authentication are not included. Caller-provided owners must come from a trusted application. Upload IDs are not credentials. Storage directories must be trusted; symlink checks do not protect against hostile concurrent filesystem changes. File bytes and SQLite are separate resources: tested recovery covers interrupted process writes and renames, without promising power-loss durability or distributed storage. Failed bytes remain until expiry. No encryption or malware scanning.

13 local tests cover restart, retries, conflicts, ownership, concurrent writes, both interruption recovery paths, checksum damage, empty files, search, path checks and retention. GitHub Actions runs tests and the synthetic demo on Python 3.10, 3.12 and 3.13; Actions → Tests and demo → Run workflow lets you repeat them.
