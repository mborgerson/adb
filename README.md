ADB
===

Experiments in building a fast application database with subscription and rollback.

- Fast serialization with [flatbuffers](https://flatbuffers.dev/)
- Fast, persistent storage with [LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database)
- Snapshotting with database journaling layer
- Simple pub/sub for responding to DB changes
