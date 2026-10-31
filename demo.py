import hashlib
import json
import tempfile
from pathlib import Path
from fileflow import Uploads

with tempfile.TemporaryDirectory() as directory:
    store = Uploads(Path(directory) / "storage")
    data = b"Synthetic resumable upload demonstration."
    upload = store.begin("demo-owner", "demo.txt", len(data), hashlib.sha256(data).hexdigest())
    store.append(upload, "demo-owner", 0, data[:10])
    store.append(upload, "demo-owner", 10, data[10:])
    print(json.dumps(store.finish(upload, "demo-owner"), indent=2))
    assert b"".join(store.read_chunks(upload, "demo-owner")) == data
    print("Recovered content matches the original bytes")
