"""Local publisher state. No Livepeer payer credentials or chain transactions."""

import base64
import fcntl
import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path

from eth_hash.auto import keccak
from livepeer_swarm.feeds import sign_feed
from livepeer_swarm.stamp import BucketFull, personal_sign


class PublisherState:
    def __init__(
        self,
        path,
        signer,
        batch: bytes,
        *,
        depth: int,
        bucket_depth=16,
        immutable=False,
        new_batch=False,
        import_counters=None,
    ):
        if len(batch) != 32 or not 0 < bucket_depth < depth <= 32:
            raise ValueError("invalid batch configuration")
        self.signer, self.batch = signer, batch
        self.depth, self.bucket_depth, self.immutable = depth, bucket_depth, immutable
        self.lock = threading.RLock()
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        # All state for this batch must share this directory, including imports.
        self.file_lock = open(path.parent / (batch.hex() + ".lock"), "a")
        try:
            fcntl.flock(self.file_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file_lock.close()
            raise ValueError("batch already has an active publisher") from None
        self.db = sqlite3.connect(path, check_same_thread=False)
        os.chmod(path, 0o600)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS buckets (bucket INTEGER PRIMARY KEY, next INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS feeds (topic TEXT PRIMARY KEY, next INTEGER NOT NULL, pending TEXT);
            CREATE TABLE IF NOT EXISTS slots (address BLOB PRIMARY KEY, bucket INTEGER, slot INTEGER, stamp TEXT, UNIQUE(bucket, slot));
        """)
        config = json.dumps(
            [signer.address.lower(), batch.hex(), depth, bucket_depth, immutable]
        )
        try:
            row = self.db.execute(
                "SELECT value FROM meta WHERE key='config'"
            ).fetchone()
            if row and row[0] != config:
                raise ValueError(
                    "state belongs to a different publisher or batch configuration"
                )
            marker = path.parent / (batch.hex() + ".initialized")
            if not row and new_batch and marker.exists():
                raise ValueError(
                    "batch already initialized: restore its database or explicitly import its counters"
                )
            if (row or new_batch or import_counters) and not marker.exists():
                # Persist the one-time initialization assertion before permitting
                # any stamp allocations. Losing the SQLite file must fail closed,
                # even if unused=true remains in the publisher's configuration.
                with marker.open("x") as output:
                    os.chmod(marker, 0o600)
                    output.write(str(path.resolve()) + "\n")
                    output.flush()
                    os.fsync(output.fileno())
                directory = os.open(path.parent, os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            if not row:
                if not new_batch and not import_counters:
                    raise ValueError(
                        "existing batch requires counter import; use new_batch only for an unused batch"
                    )
                with self.db:
                    self.db.execute("INSERT INTO meta VALUES (?,?)", ("config", config))
                    if import_counters:
                        self._import(json.loads(Path(import_counters).read_text()))
        except BaseException:
            self.close()
            raise

    def _import(self, data):
        limit = 1 << (self.depth - self.bucket_depth)
        for bucket, count in data["buckets"].items():
            bucket, count = int(bucket), int(count)
            if not 0 <= bucket < (1 << self.bucket_depth) or not 0 <= count <= limit:
                raise ValueError("invalid imported bucket counter")
            self.db.execute("INSERT INTO buckets VALUES (?,?)", (bucket, count))
        for address, slot in data["slots"].items():
            address = bytes.fromhex(address)
            bucket = self.bucket(address)
            count = self.db.execute(
                "SELECT next FROM buckets WHERE bucket=?", (bucket,)
            ).fetchone()
            if not count or not 0 <= slot < count[0]:
                raise ValueError("imported slot is outside allocated counter")
            self.db.execute(
                "INSERT INTO slots VALUES (?,?,?,NULL)", (address, bucket, slot)
            )

    def bucket(self, address):
        if len(address) != 32:
            raise ValueError("chunk address must be 32 bytes")
        return int.from_bytes(address[:4], "big") >> (32 - self.bucket_depth)

    def _stamp(self, address):
        bucket = self.bucket(address)
        old = self.db.execute(
            "SELECT slot,stamp FROM slots WHERE address=?", (address,)
        ).fetchone()
        if old and old[1]:
            return old[1]
        if old:
            slot = old[0]
        else:
            row = self.db.execute(
                "SELECT next FROM buckets WHERE bucket=?", (bucket,)
            ).fetchone()
            slot = row[0] if row else 0
            if slot == (1 << (self.depth - self.bucket_depth)):
                if self.immutable:
                    raise BucketFull(bucket)
                slot = 0
            self.db.execute(
                "DELETE FROM slots WHERE bucket=? AND slot=?", (bucket, slot)
            )
            self.db.execute(
                "INSERT OR REPLACE INTO buckets VALUES (?,?)", (bucket, slot + 1)
            )
        index = bucket.to_bytes(4, "big") + slot.to_bytes(4, "big")
        timestamp = time.time_ns().to_bytes(8, "big")
        stamp = (
            self.batch
            + index
            + timestamp
            + personal_sign(
                keccak(address + self.batch + index + timestamp), self.signer
            )
        )
        self.db.execute(
            "INSERT OR REPLACE INTO slots VALUES (?,?,?,?)",
            (address, bucket, slot, stamp.hex()),
        )
        return stamp.hex()

    def stamps(self, addresses):
        with self.lock, self.db:
            result = [self._stamp(address) for address in addresses]
        return result

    def prepare_feed(self, topic, payload, *, first_index=0):
        with self.lock, self.db:
            row = self.db.execute(
                "SELECT next,pending FROM feeds WHERE topic=?", (topic,)
            ).fetchone()
            index = row[0] if row else first_index
            if row and row[1]:
                pending = json.loads(row[1])
                if base64.b64decode(pending["body"])[8:] == payload:
                    return pending
                raise ValueError("feed has an unacknowledged pending update")
            signed = sign_feed(topic, index, payload, self.signer)
            command = {
                "op": "soc",
                "request_id": uuid.uuid4().hex,
                "topic": topic,
                "index": index,
                "owner": signed.owner,
                "id": signed.id.hex(),
                "sig": signed.signature.hex(),
                "body": base64.b64encode(signed.body).decode(),
                "stamp": self._stamp(signed.address),
                "address": signed.address.hex(),
            }
            self.db.execute(
                "INSERT OR REPLACE INTO feeds VALUES (?,?,?)",
                (topic, index, json.dumps(command)),
            )
        return command

    def pending_feeds(self):
        with self.lock:
            return [
                json.loads(r[0])
                for r in self.db.execute(
                    "SELECT pending FROM feeds WHERE pending IS NOT NULL"
                )
            ]

    def ack_feed(self, request_id):
        with self.lock, self.db:
            for topic, index, pending in self.db.execute(
                "SELECT topic,next,pending FROM feeds WHERE pending IS NOT NULL"
            ):
                if json.loads(pending)["request_id"] == request_id:
                    self.db.execute(
                        "UPDATE feeds SET next=?,pending=NULL WHERE topic=?",
                        (index + 1, topic),
                    )
                    return

    def get(self, key, default=None):
        with self.lock:
            row = self.db.execute(
                "SELECT value FROM meta WHERE key=?", ("user:" + key,)
            ).fetchone()
            return json.loads(row[0]) if row else default

    def set(self, key, value):
        with self.lock, self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO meta VALUES (?,?)",
                ("user:" + key, json.dumps(value)),
            )

    def close(self):
        # A cancelled asyncio.to_thread call may still be committing stamps.
        with self.lock:
            self.db.close()
            self.file_lock.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
