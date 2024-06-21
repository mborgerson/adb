#!/usr/bin/env python

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Generator, Optional

import logging
import pathlib
import sys
import unittest

import flatbuffers
import lmdb

sys.path.append(str(pathlib.Path(__file__).parent.absolute()))  # Hack on current dir
import model


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class Serializable:
    """
    Mixin to provide serialization methods using flatbuffer codecs.
    """

    def to_bytes(self):
        fbb = flatbuffers.Builder(16)
        t = self.Pack(fbb)
        fbb.Finish(t)
        return fbb.Output()

    @classmethod
    def from_fb_obj(cls, obj):
        # Ugh. Iterate over the object's attributes and copy them over to construct the object. Slow. FIXME.
        init_kvs = {}
        for k, v in obj.__dict__.items():
            log.debug("looking at k %s v %s", k, v)
            expected_type = cls.__annotations__[k]
            if isinstance(v, expected_type):
                init_kvs[k] = v
            elif expected_type is bytes:
                init_kvs[k] = bytes(v)  # Convert from numpy array
            elif expected_type is str:
                init_kvs[k] = v.decode("utf-8")
            elif issubclass(expected_type, Serializable):
                init_kvs[k] = expected_type.from_fb_obj(v)
            else:
                raise RuntimeError(f"Unsupported serialization type {expected_type}")
        return cls(**init_kvs)

    @classmethod
    def from_bytes(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        pos = n + offset
        model_obj = cls.InitFromBuf(buf, pos)
        return cls.from_fb_obj(model_obj)


@dataclass
class ReplayEvent(Serializable, model.ReplayEventT):
    action: int
    key: bytes
    value: bytes


INVALID_KEY = int(0).to_bytes(8, byteorder="little")


class Database:

    def gen_new_id(self) -> bytes:
        raise NotImplementedError()

    def get(self, key: bytes) -> bytes:
        raise NotImplementedError()

    def put(self, key: bytes, value: bytes) -> None:
        raise NotImplementedError()

    def pop(self, key: bytes) -> bytes:
        raise NotImplementedError()


class DictDatabase(Database):
    """
    Dictionary backed mock database.
    """

    def __init__(self):
        self._data = {}
        self._next_id = 1

    def gen_new_id(self):
        id_ = self._next_id
        self._next_id += 1
        return id_.to_bytes(8, byteorder="little")

    def get(self, key):
        assert type(key) is bytes
        value = self._data.get(key, None)
        log.debug("db.get[%s] -> %s", key, value)
        return value

    def put(self, key, value):
        assert type(key) is bytes
        log.debug("db.put[%s] = %s", key, value)
        self._data[key] = value

    def pop(self, key):
        assert type(key) is bytes
        log.debug("db.pop[%s]", key)
        return self._data.pop(key)


class LmdbDatabase(Database):
    """
    LMDB backed database.
    """

    def __init__(self, name: str = "adb.lmdb"):
        self._env = lmdb.Environment(name, sync=False)
        GiB = 1024 * 1024 * 1024
        self._env.set_mapsize(128 * GiB)

        with self._env.begin(write=True) as txn:
            self._txn = txn

            next_id = self.get(b"#db-next-id")
            if next_id is None:
                self._next_id = 0
                self.gen_new_id()
            else:
                self._next_id = int.from_bytes(next_id, byteorder="little")

        self._txn = None

    def gen_new_id(self) -> bytes:
        new_id = self._next_id
        self._next_id += 1
        self.put(b"#db-next-id", self._next_id.to_bytes(8, byteorder="little"))
        return new_id.to_bytes(8, byteorder="little")

    def put(self, key: bytes, value: bytes):
        if self._txn is None:
            self.safe_put(key, value)
            return
        log.debug("db.get[%s] -> %s", key, value)
        self._txn.put(key, value)

    def safe_put(self, key: bytes, value: bytes):
        with self._env.begin(write=True) as txn:
            log.debug("db.get[%s] -> %s", key, value)
            txn.put(key, value)

    def pop(self, key: bytes):
        if self._txn is None:
            return self.safe_pop(key)
        log.debug("db.pop[%s]", key)
        return self._txn.pop(key)

    def safe_pop(self, key: bytes):
        with self._env.begin(write=True) as txn:
            log.debug("db.pop[%s]", key)
            return txn.pop(key)

    def get(self, key: bytes):
        if self._txn is None:
            return self.safe_get(key)
        value = self._txn.get(key)
        log.debug("db.get[%s] -> %s", key, value)
        return value

    def safe_get(self, key: bytes):
        with self._env.begin(write=False) as txn:
            value = txn.get(key)
            log.debug("db.get[%s] -> %s", key, value)
            return value


# ----------------------------------------------------------------------------------------------------------------------
# Basic database data structures


class DbSList:
    """
    Simple singly-linked list implementation.
    """

    # FIXME: Make this faster by adding a special head node that tracks tail
    #        Special head node could also be used to monitor keys and clear cache

    def __init__(self, db: Database, key: bytes):
        self._db = db
        self._key = key
        self._len = None
        self._last = None

    def refresh(self):
        """
        Flush any cached data.
        """
        self._len = None
        self._last = None

    def __len__(self):
        if self._len is not None:
            return self._len
        self._len = sum(1 for _ in self._iter_nodes())  # Slow
        return self._len

    def __iter__(self):
        # FIXME: Ensure list not modified while iterating!
        for _, _, data in self._iter_nodes():
            yield data

    def __getitem__(self, index):
        if index <= len(self):
            _, _, item_data = self._get_node_at_index(index)
            return item_data
        raise IndexError("list index out of range")

    def __setitem__(self, index, value):
        for idx, (current_key, next_key, item_data) in enumerate(self._iter_nodes()):
            if idx == index:
                self._db.put(current_key, self._pack_node(next_key, item_data))
                if idx == len(self) - 1:
                    self._last = (current_key, next_key, item_data)
        raise IndexError("list index out of range")

    def index(self, value_needle):
        for idx, value_haystack in enumerate(self):
            if value_needle == value_haystack:
                return idx
        raise ValueError(f"{value_needle} is not in list")

    def insert(self, index: int, value: bytes):
        num_items = len(self)
        if num_items == 0:
            log.debug(
                "append: Inserting new item at head position with key %s", self._key
            )
            self._db.put(self._key, self._pack_node(INVALID_KEY, value))
            self._last = (self._key, INVALID_KEY, value)
        else:
            new_item_key = self._db.gen_new_id()
            log.debug("insert: Inserting new item with key %s", new_item_key)
            index = min(max(0, index), num_items)
            if index == 0:
                old_head_data = self._db.pop(self._key)
                self._db.put(new_item_key, old_head_data)  # Move old head
                self._db.put(
                    self._key, self._pack_node(new_item_key, value)
                )  # Insert new head
                if num_items == 1:
                    self._last = (new_item_key, *self._unpack_node(old_head_data))
            else:
                pkey, nkey, pdata = self._get_node_at_index(index - 1)
                self._db.put(pkey, self._pack_node(new_item_key, pdata))  # Update n-1
                self._db.put(new_item_key, self._pack_node(nkey, value))  # Insert n
                if index == num_items:
                    self._last = (new_item_key, nkey, value)
        self._len += 1

    def append(self, value: bytes):
        self.insert(len(self), value)

    def pop(self, index: int = -1):
        num_items = len(self)
        if num_items == 0:
            raise IndexError("pop from empty list")
        index_to_remove = index
        if index_to_remove < 0:
            index_to_remove += num_items
        if index_to_remove >= num_items:
            raise IndexError("pop index out of range")
        if index_to_remove == 0:
            log.debug(
                "pop: Dropping item at index %s with key %s", index_to_remove, self._key
            )
            nkey, data = self._unpack_node(self._db.pop(self._key))
            if nkey != INVALID_KEY:
                log.debug("pop: Moving head to next item with key %s", nkey)
                self._db.put(self._key, self._db.pop(nkey))
        else:
            pkey, ckey, pdata = self._get_node_at_index(index_to_remove - 1)
            log.debug(
                "pop: Dropping item at index %s with key %s", index_to_remove, ckey
            )
            nkey, data = self._unpack_node(self._db.pop(ckey))
            log.debug(
                "pop: Updating previous item to point to next item with key %s", nkey
            )
            self._db.pop(pkey)
            self._db.put(pkey, self._pack_node(nkey, pdata))
        if index_to_remove == num_items - 1:
            self._last = None
        self._len -= 1
        return data

    def clear(self):
        while len(self):
            self.pop(0)

    @staticmethod
    def _pack_node(next_key: bytes, data: bytes):
        return next_key + data

    @staticmethod
    def _unpack_node(data: bytes):
        return data[0:8], data[8:]

    def _get_node_at_index(self, index: int):
        assert 0 <= index < len(self)
        is_last = index == (len(self) - 1)
        if is_last and self._last is not None:
            return self._last
        value = next(x for i, x in enumerate(self._iter_nodes()) if i == index)
        if is_last:
            self._last = value
        return value

    def _iter_nodes(self):
        next_key = self._key
        while True:
            current_key = next_key
            log.debug("_iter_nodes: Looking at %s", current_key)
            if current_key == INVALID_KEY:
                log.debug("_iter_nodes: Current key is invalid. Stopping iteration.")
                return
            data = self._db.get(current_key)
            if data is None:
                log.debug("_iter_nodes: Not found for key %s", current_key)
                if current_key != self._key:
                    raise RuntimeError("DB corrupt")
                return
            next_key, item_data = self._unpack_node(data)
            log.debug(
                "_iter_nodes: Found node: current_key=%s next_key=%s item_data=%s",
                current_key,
                next_key,
                item_data,
            )
            yield current_key, next_key, item_data


class TestDbSList(unittest.TestCase):
    def test_append(self):
        def act(l):
            assert len(l) == 0
            l.append(b"123")
            assert list(l) == [b"123"]
            l.append(b"456")
            assert list(l) == [b"123", b"456"]

        act(DbSList(DictDatabase(), b"test"))
        act(list())

    def test_insert(self):
        def act(l):
            l.insert(0, b"123")
            assert list(l) == [b"123"]
            l.insert(0, b"456")
            assert list(l) == [b"456", b"123"]
            l.insert(1, b"789")
            assert list(l) == [b"456", b"789", b"123"]
            l.insert(200, b"ABC")
            assert list(l) == [b"456", b"789", b"123", b"ABC"]
            l.insert(-500, b"DEF")
            assert list(l) == [b"DEF", b"456", b"789", b"123", b"ABC"]

        act(DbSList(DictDatabase(), b"test"))
        act(list())

    def test_pop_empty(self):
        list_ = DbSList(DictDatabase(), b"test")
        with self.assertRaises(IndexError):
            list_.pop(0)

    def test_pop_oob(self):
        list_ = DbSList(DictDatabase(), b"test")
        list_.append(b"123")
        with self.assertRaises(IndexError):
            list_.pop(1)

    def test_pop_head(self):
        list_ = DbSList(DictDatabase(), b"test")
        list_.append(b"123")
        list_.append(b"456")
        assert list(list_) == [b"123", b"456"]
        assert list_.pop(0) == b"123"
        assert list(list_) == [b"456"]

    def test_pop_tail(self):
        list_ = DbSList(DictDatabase(), b"test")
        list_.append(b"123")
        list_.append(b"456")
        list_.append(b"789")
        assert list_.pop(2) == b"789"
        assert list(list_) == [b"123", b"456"]

    def test_pop_not_head(self):
        list_ = DbSList(DictDatabase(), b"test")
        list_.append(b"123")
        list_.append(b"456")
        list_.append(b"789")
        assert list_.pop(1) == b"456"
        assert list(list_) == [b"123", b"789"]

    def test_pop_only(self):
        list_ = DbSList(DictDatabase(), b"test")
        list_.append(b"123")
        list_.pop(0)
        assert list(list_) == []


# -----------------------------------------------------------------------------------------------------------------------
# Snapshot database layer


@dataclass
class SnapshotMark:
    event_idx: int
    comment: str


class SnapshotDatabase(Database):
    """
    Database with snapshots: mutation recording, state marking, and replay/revert.
    """

    # FIXME: Implement in C++
    # FIXME: Add mark list for faster mark indexing
    # FIXME: Group events into blocks for bulk database writes

    def __init__(self, db: Database):
        self._db = db
        self._event_list = DbSList(self._db, b"#snapshot-event-list")
        self._playback_head = len(self._event_list)

    def gen_new_id(self) -> bytes:
        return self._db.gen_new_id()

    def get(self, key: bytes) -> bytes:
        return self._db.get(key)

    def put(self, key: bytes, value: bytes) -> None:
        self._truncate_to_playback_head()
        current_value = self._db.get(key)
        if current_value is not None:
            self._record_pop(key, current_value)
        self._db.put(key, value)
        self._record_put(key, value)

    def pop(self, key: bytes) -> bytes:
        self._truncate_to_playback_head()
        value = self._db.pop(key)
        self._record_pop(key, value)
        return value

    def mark(self, comment: str = "") -> SnapshotMark:
        """
        Create a new mark in the database.
        """
        self._truncate_to_playback_head()
        self._event_list.append(
            ReplayEvent(
                action=model.ReplayAction.Mark, key=b"", value=comment.encode("utf-8")
            ).to_bytes()
        )
        self._playback_head = len(self._event_list)
        return SnapshotMark(self._playback_head, comment)

    def get_marks(self) -> Generator[SnapshotMark, None, None]:
        """
        List all marks.
        """
        for idx, event_bytes in enumerate(self._event_list):
            event = ReplayEvent.from_bytes(event_bytes)
            if event.action == model.ReplayAction.Mark:
                yield SnapshotMark(idx + 1, event.value.decode("utf-8"))

    def commit(self) -> None:
        """
        Remove all saved snapshot data.
        """
        self._event_list.clear()

    def restore(self, mark: Optional[SnapshotMark] = None) -> None:
        """
        Restore marked database state.
        """
        log.debug(
            "Restoring database at mark %s (event %d)",
            self._playback_head,
            mark.comment,
            mark.event_idx,
        )

        if isinstance(mark, SnapshotMark):
            event_index = mark.event_idx
        elif mark is None:
            event_index = len(self._event_list)
        else:
            raise ValueError("Invalid mark value")

        forward = event_index >= self._playback_head

        assert 0 <= self._playback_head <= len(self._event_list)
        if not (0 <= event_index <= len(self._event_list)):
            raise IndexError("Target event index is out of bounds")

        if forward:
            for event_idx in range(self._playback_head, event_index):
                event = ReplayEvent.from_bytes(self._event_list[event_idx])
                self._replay_one_event(event)
        else:
            for event_idx in range(self._playback_head - 1, event_index - 1, -1):
                event = ReplayEvent.from_bytes(self._event_list[event_idx])
                self._revert_one_event(event)

        self._playback_head = event_index

    def _truncate_to_playback_head(self):
        if len(self._event_list) > self._playback_head:
            log.debug("Truncating playback head")
        while len(self._event_list) > self._playback_head:
            self._event_list.pop(self._playback_head)

    def _record_put(self, key: bytes, value: bytes):
        log.debug("Recording put of key %s", key)
        self._event_list.append(
            ReplayEvent(action=model.ReplayAction.Put, key=key, value=value).to_bytes()
        )
        self._playback_head = len(self._event_list)

    def _record_pop(self, key: bytes, value: bytes):
        log.debug("Recording pop of key %s", key)
        self._event_list.append(
            ReplayEvent(action=model.ReplayAction.Pop, key=key, value=value).to_bytes()
        )
        self._playback_head = len(self._event_list)

    def _replay_one_event(self, event: "ReplayEvent"):
        if event.action == model.ReplayAction.Pop:
            self._db.pop(event.key)
        elif event.action == model.ReplayAction.Put:
            self._db.put(event.key, event.value)

    def _revert_one_event(self, event: "ReplayEvent"):
        if event.action == model.ReplayAction.Pop:
            self._db.put(event.key, event.value)
        elif event.action == model.ReplayAction.Put:
            self._db.pop(event.key)


class TestSnapshotDatabase(unittest.TestCase):
    def _setup(self):
        db = SnapshotDatabase(DictDatabase())

        db.mark("mark0")
        self._verify_mark_state(db, 0)

        db.put(b"a", b"ABC")
        db.put(b"b", b"xxx")
        db.put(b"z", b"123")
        db.mark("mark1")
        self._verify_mark_state(db, 1)

        db.put(b"a", b"123")  # Replace a
        db.pop(b"b")  # Remove b
        db.put(b"c", b"456")  # Insert c
        # Retain z
        db.mark("mark2")
        self._verify_mark_state(db, 2)

        return db

    @staticmethod
    def _verify_mark_state(db, mark_idx):
        states = [
            {b"a": None, b"b": None, b"c": None, b"z": None},
            {b"a": b"ABC", b"b": b"xxx", b"c": None, b"z": b"123"},
            {b"a": b"123", b"b": None, b"c": b"456", b"z": b"123"},
        ]
        for k, v in states[mark_idx].items():
            assert db.get(k) == v

    def test_undo(self):
        db = self._setup()
        marks = list(db.get_marks())
        for idx in [1, 0]:  # Undo, Undo
            db.restore(marks[idx])
            self._verify_mark_state(db, idx)

    def test_redo(self):
        db = self._setup()
        marks = list(db.get_marks())
        for idx in [1, 2]:  # Undo, Redo
            db.restore(marks[idx])
            self._verify_mark_state(db, idx)

    def test_nop_replay(self):
        db = self._setup()
        marks = list(db.get_marks())
        db.restore(marks[2])  # Replay to current
        self._verify_mark_state(db, 2)

    def test_truncate(self):
        db = self._setup()
        marks = list(db.get_marks())
        db.restore(marks[1])
        self._verify_mark_state(db, 1)
        db._truncate_to_playback_head()
        assert len(db._event_list) == marks[1].event_idx

    def test_oob_replay(self):
        db = self._setup()
        marks = list(db.get_marks())
        db.restore(marks[0])
        db.put(b"a", b"123")  # marks[1:] now invalid
        with self.assertRaises(IndexError):
            db.restore(marks[1])


class Observable:
    def __setattr__(self, name, value):
        log.debug(f"setattr {name} = {value}")
        return super().__setattr__(name, value)


class Dispatch:
    """
    Rudimentary PubSub
    """

    # FIXME: Topic wildcards
    #        kb.function.<unique-function-id>.updated
    #        kb.function.*
    # FIXME: Pathlib for topic mgmt?
    # FIXME: How should handler register its own data? functools wrapper?
    # FIXME: Do we need a way for the handler to be able to query each path component?
    #        kb.functions
    #        --which kb?

    def __init__(self):
        self._subscribers = defaultdict(set)

    def subscribe(self, key, handler):
        self._subscribers[key].add(handler)

    def subscribe_seq(self, topic_handler_sequence):
        for topic, handler in topic_handler_sequence:
            self.subscribe(topic, handler)

    def unsubscribe(self, key, handler):
        self._subscribers[key].remove(handler)

    def unsubscribe_seq(self, topic_handler_sequence):
        for topic, handler in topic_handler_sequence:
            self.unsubscribe(topic, handler)

    def publish(self, key, data=None):
        for handler in self._subscribers[key]:
            handler(key, data)


if __name__ == "__main__":
    unittest.main()
