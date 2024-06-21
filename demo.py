#!/usr/bin/env python

from dataclasses import dataclass, field

import argparse
import unittest
import pathlib
import sys

from adb import (
    DbSList,
    DictDatabase,
    Dispatch,
    LmdbDatabase,
    Serializable,
    SnapshotDatabase,
    )

sys.path.append(str(pathlib.Path(__file__).parent.absolute()))  # Hack on current dir
import model


SPACEID_TO_NAME = {
    0: "invalid",
    1: "ram"
}

@dataclass
class Address(Serializable, model.AddressT):
    spaceId: int = 0
    offset: int = 0

    def __str__(self):
        return f"<{SPACEID_TO_NAME[self.spaceId]}>0x{self.offset:X}"

@dataclass
class Function(# Observable,
    Serializable, model.FunctionT):
    id: bytes = b""
    address: Address = field(default_factory=Address)
    name: str = "Unnamed"
    prototype: str = ""
    callingConvention: str = ""

    def __str__(self):
        return f"Function {self.name} at {self.address}"


class TestFunctionSerialization(unittest.TestCase):
    def test_serializable(self):
        f = Function(address=Address(1, 0xdeadbeef), name="my_func")
        d = f.to_bytes()
        f2 = Function.from_bytes(d)
        assert f2 == f


# FIXME: Indexing for faster lookups
# - Profile
# - alt: results cache
class KbSList:
    def __init__(self, name, cls, kb: "KnowledgeBase"):
        self._kb = kb
        self._name = name
        self._cls = cls
        self._items = DbSList(self._kb.db, self._name.encode("utf-8"))
        self._subscriptions = [("snapshot.restored", self.refresh)]
        self._kb.dispatch.subscribe_seq(self._subscriptions)

    def refresh(self, key, data) -> None:
        self._items.refresh()

    def add(self, item) -> None:
        assert isinstance(item, self._cls)
        assert not item.id, "Item already has an id"
        item.id = self._kb.db.gen_new_id()
        self._kb.db.put(item.id, item.to_bytes())
        self._items.append(item.id)
        self._kb.dispatch.publish(f"{self._name}.added", item)

    def update(self, item) -> None:
        assert isinstance(item, self._cls)
        self._kb.db.put(item.id, item.to_bytes())
        self._kb.dispatch.publish(f"{self._name}.updated", item)

    def remove(self, item) -> None:
        assert isinstance(item, self._cls)
        item = self._cls.from_bytes(self._kb.db.pop(self._items.pop(self._items.index(item.id))))
        self._kb.dispatch.publish(f"{self._name}.removed", item)

    def __len__(self):
        return len(self._items)

    def __iter__(self):
        for id_ in self._items:
            yield self._cls.from_bytes(self._kb.db.get(id_))


class KnowledgeBase:
    def __init__(self, db):
        self.db = SnapshotDatabase(db)
        self.dispatch = Dispatch()
        self.functions = KbSList("functions", Function, self)


class TestKbSList(unittest.TestCase):

    def test_subscription(self):

        notifications = []

        def on_function_added(key, data):
            notifications.append(('added', key, data))

        def on_function_updated(key, data):
            notifications.append(('updated', key, data))

        def on_function_removed(key, data):
            notifications.append(('removed', key, data))

        kb = KnowledgeBase(DictDatabase())

        # Test basic subscription
        kb.dispatch.subscribe("functions.added", on_function_added)
        kb.dispatch.subscribe("functions.updated", on_function_updated)
        kb.dispatch.subscribe("functions.removed", on_function_removed)
        
        # Create
        my_func = Function(0, Address(1, 0x12345), "myfunc")
        kb.functions.add(my_func)
        # ---
        assert "myfunc" in [f.name for f in kb.functions]
        assert len(notifications) == 1 and notifications[0][0] == "added"
        notifications.clear()

        # Update
        pre_update_mark = kb.db.mark("pre-update")
        my_func.name = "bob"
        kb.functions.update(my_func)
        # ---
        assert "myfunc" not in [f.name for f in kb.functions]
        assert "bob" in [f.name for f in kb.functions]
        assert len(notifications) == 1 and notifications[0][0] == "updated"
        notifications.clear()
        
        # Delete
        pre_removal_mark = kb.db.mark("pre-removal")
        kb.functions.remove(my_func)
        # ---
        assert "bob" not in [f.name for f in kb.functions]
        assert len(notifications) == 1 and notifications[0][0] == "removed"
        notifications.clear()

        # Restore to before delete
        kb.db.restore(pre_removal_mark)
        kb.dispatch.publish("snapshot.restored")
        assert "bob" in [f.name for f in kb.functions]

        # Restore to before update
        kb.db.restore(pre_update_mark)
        kb.dispatch.publish("snapshot.restored")
        # ---
        assert "myfunc" in [f.name for f in kb.functions]
        assert "bob" not in [f.name for f in kb.functions]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true")
    ap.add_argument("--dict-db", action="store_true")
    args = ap.parse_args()
    if args.test:
        sys.argv.pop(0)
        unittest.main()
        return

    if args.dict_db:
        db = DictDatabase()
        do_write(db)
        # do_read(db)
    else:
        db = LmdbDatabase()
        with db._env.begin(write=True) as txn:
            db._txn = txn
            do_write(db)
        with db._env.begin(write=False) as txn:
            db._txn = txn
            do_read(db)


def do_write(db):
    print('Writing')
    fm = KnowledgeBase(db)
    num_functions = len(fm.functions)
    for i in range(5000):
        fm.functions.add(Function(0, Address(1, 0xdeadc0de + i), f"my_func{num_functions + i}"))


def do_read(db):
    print('Reading')
    fm = KnowledgeBase(db)
    for idx, f in enumerate(fm.functions):
        print(f"{idx}: {f}")


if __name__ == "__main__":
    main()
