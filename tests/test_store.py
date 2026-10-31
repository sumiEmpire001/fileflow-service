import hashlib
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from fileflow.store import Uploads, Conflict, IntegrityError


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = 1000
        self.store = Uploads(self.temp.name, lambda: self.now)

    def begin(self, data=b"abcdef", owner="alice", name="sample.txt", ttl=60):
        return self.store.begin(owner, name, len(data), hashlib.sha256(data).hexdigest(), ttl=ttl)

    def test_resume_after_restart(self):
        upload = self.begin()
        self.store.append(upload, "alice", 0, b"abc")
        self.store = Uploads(self.temp.name, lambda: self.now)
        self.assertEqual(self.store.status(upload, "alice")["offset"], 3)
        self.store.append(upload, "alice", 3, b"def")
        self.assertEqual(self.store.finish(upload, "alice")["state"], "complete")
        self.assertEqual(b"".join(self.store.read_chunks(upload, "alice")), b"abcdef")
        self.assertEqual(self.store.finish(upload, "alice")["state"], "complete")

    def test_retry_and_conflicting_offsets(self):
        upload = self.begin()
        self.assertEqual(self.store.append(upload, "alice", 0, b"abc"), 3)
        self.assertEqual(self.store.append(upload, "alice", 0, b"abc"), 3)
        for offset, data in [(0, b"xyz"), (1, b"bcde"), (4, b"ef"), (3, b"defg")]:
            with self.assertRaises(Conflict):
                self.store.append(upload, "alice", offset, data)
        self.assertEqual(self.store.status(upload, "alice")["offset"], 3)

    def test_incomplete_and_checksum_failure(self):
        upload = self.begin()
        with self.assertRaises(ValueError):
            self.store.finish(upload, "alice")
        self.store.append(upload, "alice", 0, b"xxxxxx")
        with self.assertRaises(IntegrityError):
            self.store.finish(upload, "alice")
        self.assertEqual(self.store.status(upload, "alice")["state"], "failed")
        with self.assertRaises(ValueError):
            self.store.append(upload, "alice", 0, b"abcdef")

    def test_recovers_uncommitted_tail(self):
        upload = self.begin()
        self.store.append(upload, "alice", 0, b"abc")
        with self.store.path(upload, "parts").open("ab") as stream:
            stream.write(b"uncommitted garbage")
        self.store.append(upload, "alice", 3, b"def")
        self.store.finish(upload, "alice")
        self.assertEqual(b"".join(self.store.read_chunks(upload, "alice")), b"abcdef")

    def test_recovers_rename_before_metadata_commit(self):
        upload = self.begin()
        self.store.append(upload, "alice", 0, b"abcdef")
        self.store.path(upload, "parts").replace(self.store.path(upload, "objects"))
        self.assertEqual(self.store.finish(upload, "alice")["state"], "complete")

    def test_owner_isolation(self):
        upload = self.begin()
        for operation in [lambda: self.store.status(upload, "bob"),
                          lambda: self.store.append(upload, "bob", 0, b"abc"),
                          lambda: self.store.finish(upload, "bob"),
                          lambda: list(self.store.read_chunks(upload, "bob"))]:
            with self.assertRaises(PermissionError):
                operation()
        self.assertEqual(self.store.search("bob"), [])

    def test_validation_and_unsafe_paths(self):
        for name in ["../escape", "dir/file", "dir\\file", "..", "", "a\x00b"]:
            with self.assertRaises(ValueError):
                self.begin(name=name)
        for size in [-1, True, 64 * 1024 * 1024 + 1]:
            with self.assertRaises(ValueError):
                self.store.begin("alice", "file", size, "0" * 64)
        with self.assertRaises(ValueError):
            self.store.path("../escape", "parts")
        upload = self.begin()
        part = self.store.path(upload, "parts")
        part.unlink()
        part.symlink_to(Path(self.temp.name) / "metadata.db")
        with self.assertRaises(IntegrityError):
            self.store.append(upload, "alice", 0, b"abc")

    def test_expiry_and_retention(self):
        upload = self.begin(ttl=1)
        self.store.append(upload, "alice", 0, b"abcdef")
        self.store.finish(upload, "alice")
        self.now += 1
        with self.assertRaises(ValueError):
            list(self.store.read_chunks(upload, "alice"))
        self.assertEqual(self.store.search("alice"), [])
        self.assertEqual(self.store.purge_expired()["expired_uploads"], 1)
        self.assertFalse(self.store.path(upload, "objects").exists())
        self.assertEqual(self.store.status(upload, "alice")["state"], "expired")
        self.assertEqual(self.store.purge_expired()["expired_uploads"], 0)

    def test_concurrent_identical_chunks(self):
        upload = self.begin()
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.store.append(upload, "alice", 0, b"abc"), range(16)))
        self.assertEqual(results, [3] * 16)
        self.assertEqual(self.store.path(upload, "parts").read_bytes(), b"abc")

    def test_empty_file(self):
        upload = self.begin(b"")
        self.store.finish(upload, "alice")
        self.assertEqual(list(self.store.read_chunks(upload, "alice")), [])

    def test_search_literal_and_pagination(self):
        self.begin(name="100%_demo.txt")
        self.begin(name="other.txt")
        self.begin(owner="bob", name="100%_secret.txt")
        self.assertEqual(len(self.store.search("alice", "%_")), 1)
        first = self.store.search("alice", limit=1)
        second = self.store.search("alice", limit=1, offset=1)
        self.assertNotEqual(first[0]["id"], second[0]["id"])

    def test_detects_missing_or_altered_committed_bytes(self):
        upload = self.begin()
        self.store.append(upload, "alice", 0, b"abc")
        self.store.path(upload, "parts").write_bytes(b"a")
        with self.assertRaises(IntegrityError):
            self.store.append(upload, "alice", 3, b"def")
        other = self.begin(b"xyz")
        self.store.append(other, "alice", 0, b"xyz")
        self.store.finish(other, "alice")
        self.store.path(other, "objects").write_bytes(b"bad")
        with self.assertRaises(IntegrityError):
            list(self.store.read_chunks(other, "alice"))

    def test_orphan_cleanup_preserves_unmanaged_files(self):
        folder = Path(self.temp.name) / "parts"
        (folder / ("a" * 32)).write_bytes(b"orphan")
        (folder / "notes.txt").write_text("unmanaged")
        self.assertEqual(self.store.purge_expired()["orphan_files"], 1)
        self.assertTrue((folder / "notes.txt").exists())


if __name__ == "__main__":
    unittest.main()
