import hashlib
import re
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path


class Conflict(ValueError):
    pass


class IntegrityError(ValueError):
    pass


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


class Uploads:
    def __init__(self, root, clock=time.time):
        self.root, self.clock = Path(root), clock
        if self.root.is_symlink():
            raise ValueError("Storage root cannot be a symlink")
        self.root.mkdir(parents=True, exist_ok=True)
        for name in ("parts", "objects"):
            folder = self.root / name
            if folder.is_symlink():
                raise ValueError("Storage folders cannot be symlinks")
            folder.mkdir(exist_ok=True)
        with self.db() as db:
            db.executescript("""CREATE TABLE IF NOT EXISTS uploads(
                id TEXT PRIMARY KEY,owner TEXT NOT NULL,name TEXT NOT NULL,size INTEGER NOT NULL,
                sha256 TEXT NOT NULL,offset INTEGER NOT NULL,state TEXT NOT NULL,created REAL NOT NULL,expires REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS retention ON uploads(expires,state);""")

    @contextmanager
    def db(self, write=False):
        db = sqlite3.connect(self.root / "metadata.db", timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if write:
                db.commit()
        except BaseException:
            if write:
                db.rollback()
            raise
        finally:
            db.close()

    def path(self, upload, folder):
        if not isinstance(upload, str) or not re.fullmatch(r"[a-f0-9]{32}", upload) or folder not in ("parts", "objects"):
            raise ValueError("Invalid storage identifier")
        path = self.root / folder / upload
        if path.is_symlink() or path.parent.is_symlink():
            raise IntegrityError("Unsafe storage path")
        return path

    def row(self, db, upload, owner):
        row = db.execute("SELECT * FROM uploads WHERE id=? AND owner=?", (upload, owner)).fetchone()
        if not row:
            raise PermissionError("Upload unavailable to owner")
        return row

    def active(self, row):
        if row["expires"] <= self.clock() or row["state"] == "expired":
            raise ValueError("Upload expired")

    def begin(self, owner, name, size, sha256, *, ttl=3600):
        if not isinstance(owner, str) or not owner.strip() or len(owner) > 128:
            raise ValueError("Owner must be 1..128 characters")
        if not isinstance(name, str) or not name.strip() or len(name) > 255 or name in (".", "..") or any(x in name for x in ("/", "\\", "\x00")):
            raise ValueError("Filename must be a display basename")
        if type(size) is not int or not 0 <= size <= 64 * 1024 * 1024:
            raise ValueError("File size must be 0..64 MiB")
        if not isinstance(sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", sha256):
            raise ValueError("Expected lowercase SHA-256")
        if type(ttl) is not int or not 1 <= ttl <= 86400:
            raise ValueError("TTL must be 1..86400 seconds")
        upload = uuid.uuid4().hex
        part = self.path(upload, "parts")
        try:
            with self.db(True) as db:
                with part.open("xb"):
                    pass
                db.execute("INSERT INTO uploads VALUES(?,?,?,?,?,0,'open',?,?)", (upload, owner, name, size, sha256, self.clock(), self.clock() + ttl))
        except BaseException:
            part.unlink(missing_ok=True)
            raise
        return upload

    def recover_tail(self, row):
        part = self.path(row["id"], "parts")
        if not part.is_file() or part.stat().st_size < row["offset"]:
            raise IntegrityError("Committed upload bytes are missing")
        if part.stat().st_size > row["offset"]:
            with part.open("r+b") as stream:
                stream.truncate(row["offset"])
        return part

    def append(self, upload, owner, offset, chunk):
        if type(offset) is not int or offset < 0 or not isinstance(chunk, bytes) or not 1 <= len(chunk) <= 1024 * 1024:
            raise ValueError("Provide nonnegative offset and 1..1048576 byte chunk")
        with self.db(True) as db:
            row = self.row(db, upload, owner)
            self.active(row)
            if row["state"] != "open":
                raise ValueError("Upload is not open")
            part = self.recover_tail(row)
            if offset < row["offset"]:
                if offset + len(chunk) <= row["offset"]:
                    with part.open("rb") as stream:
                        stream.seek(offset)
                        if stream.read(len(chunk)) == chunk:
                            return row["offset"]
                raise Conflict("Retry bytes differ from committed content")
            if offset != row["offset"] or offset + len(chunk) > row["size"]:
                raise Conflict("Chunk offset or size conflicts with upload")
            with part.open("r+b") as stream:
                stream.seek(offset)
                stream.write(chunk)
                stream.flush()
                import os
                os.fsync(stream.fileno())
            db.execute("UPDATE uploads SET offset=? WHERE id=?", (offset + len(chunk), upload))
            return offset + len(chunk)

    def finish(self, upload, owner):
        failure = None
        with self.db(True) as db:
            row = self.row(db, upload, owner)
            self.active(row)
            if row["state"] == "complete":
                return dict(row)
            if row["state"] != "open" or row["offset"] != row["size"]:
                raise ValueError("Upload is not open or all expected bytes have not arrived")
            target = self.path(upload, "objects")
            # A final rename may have succeeded before an interrupted metadata commit.
            source = target if target.exists() else self.recover_tail(row)
            if source.stat().st_size != row["size"] or digest(source) != row["sha256"]:
                db.execute("UPDATE uploads SET state='failed' WHERE id=?", (upload,))
                failure = "Final size or checksum mismatch"
            else:
                if source != target:
                    source.replace(target)
                db.execute("UPDATE uploads SET state='complete' WHERE id=?", (upload,))
            result = dict(self.row(db, upload, owner))
        if failure:
            raise IntegrityError(failure)
        return result

    def status(self, upload, owner):
        with self.db() as db:
            row = dict(self.row(db, upload, owner))
            if row["expires"] <= self.clock():
                row["state"] = "expired"
            return row

    def read_chunks(self, upload, owner):
        with self.db() as db:
            row = self.row(db, upload, owner)
            self.active(row)
            if row["state"] != "complete":
                raise ValueError("Only completed uploads can be read")
        path = self.path(upload, "objects")
        if not path.is_file() or path.stat().st_size != row["size"] or digest(path) != row["sha256"]:
            raise IntegrityError("Stored object failed verification")
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                yield chunk

    def search(self, owner, query="", *, limit=50, offset=0):
        if not isinstance(query, str) or len(query) > 255 or type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
            raise ValueError("Invalid search or pagination")
        with self.db() as db:
            return [dict(row) for row in db.execute("SELECT id,name,size,offset,state,expires FROM uploads WHERE owner=? AND expires>? AND state!='expired' AND instr(lower(name),lower(?))>0 ORDER BY created,id LIMIT ? OFFSET ?", (owner, self.clock(), query, limit, offset))]

    def purge_expired(self):
        deleted, errors, orphans = 0, [], 0
        with self.db(True) as db:
            for row in db.execute("SELECT * FROM uploads WHERE expires<=? AND state!='expired'", (self.clock(),)).fetchall():
                try:
                    for folder in ("parts", "objects"):
                        self.path(row["id"], folder).unlink(missing_ok=True)
                    db.execute("UPDATE uploads SET state='expired',offset=0 WHERE id=?", (row["id"],))
                    deleted += 1
                except (OSError, IntegrityError) as exc:
                    errors.append({"id": row["id"], "error": str(exc)})
            for folder in ("parts", "objects"):
                for path in (self.root / folder).iterdir():
                    if re.fullmatch(r"[a-f0-9]{32}", path.name) and not db.execute("SELECT id FROM uploads WHERE id=?", (path.name,)).fetchone():
                        try:
                            self.path(path.name, folder).unlink()
                            orphans += 1
                        except (OSError, IntegrityError) as exc:
                            errors.append({"id": path.name, "error": str(exc)})
        return {"expired_uploads": deleted, "orphan_files": orphans, "errors": errors}
