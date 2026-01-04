# Columnar

Columnar is a **local, embedded, append-only columnar data store written in Go**.

It is designed for **read-heavy analytical workloads over structured data**, where
predictable performance and simple failure modes matter more than flexibility.

This project is intentionally narrow in scope.

---

## What This Is

Columnar is:

- Local and embedded (no server, no network)
- Column-oriented (not row-based)
- Append-only with immutable segments
- Schema-driven and strongly typed
- Optimized for scans, filters, and aggregations

Typical use cases:

- Infrastructure inventories
- Audit and event snapshots
- Build or deployment metadata
- Local analytics over structured files
- Tooling that needs fast reads without running a database

---

## What This Is Not

Columnar is **not**:

- A SQL database
- A transactional system
- A key-value store
- A distributed system
- A general-purpose OLTP engine

If you need:
- frequent updates
- random writes
- joins
- transactions
- multi-user concurrency

You should use an existing database instead.

---

## Design Principles

- **Immutability over mutation**  
  Data is never updated in place. New data is appended.

- **Layout follows access patterns**  
  Columnar storage is chosen because most queries touch few fields across many rows.

- **Predictability over cleverness**  
  No hidden planners, no background magic, no adaptive heuristics.

- **Explicit trade-offs**  
  Missing features are intentional, not “not yet implemented”.

---

## High-Level Architecture

```

datastore/
├── schema.json
├── manifest.json
├── segments/
│   ├── seg_000001/
│   │   ├── metadata.json
│   │   ├── col_<name>.bin
│   │   └── ...
│   └── ...

```

- Data is written in **immutable segments**
- Each segment contains **one file per column**
- Metadata enables segment pruning before data is read

---

## Data Model Overview

- Rows are a **logical concept**, identified by position
- Columns are stored independently
- Nullable columns use a **null bitmap**
- Values are encoded in binary form, not stored as raw objects

The storage engine operates entirely on bytes.

---

## Query Model (Intentionally Limited)

Supported operations:
- Column filters (equality, ranges)
- Projection (select specific columns)
- Aggregations like `COUNT`
- Full scans (explicit)

Not supported:
- Joins
- Updates or deletes
- Arbitrary expressions
- User-defined functions

This is by design.

---

## Project Status

This project is **early-stage and experimental**.

The API is unstable.
The on-disk format may change.
Do not use it for critical data.

The primary goals right now are:
- correctness
- clarity
- learnability

---

## Why This Exists

Most local tools either:
- embed a full SQL database they barely need, or
- store large structured data as JSON and suffer for it later

Columnar explores a middle ground:
a simple, opinionated data store that matches read-heavy workloads
without pretending to be a general database.

---

## License

MIT

See `LICENSE` for details.
