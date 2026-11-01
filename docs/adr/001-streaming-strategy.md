# ADR 001: Streaming Strategy & Database Protection

* **Status:** Accepted  
* **Date:** November 2026  
* **Author:** Principal Elixir Engineering Team  
* **Deciders:** Core Architecture Team  

---

## 1. Context

Exporting large relational datasets to CSV or other tabular formats is a ubiquitous requirement in modern web applications. A traditional in-memory export (`Repo.all/2` followed by serialization) loads the entire result set into BEAM process heap memory. For datasets exceeding tens of thousands of rows, this causes:
- Massive BEAM memory spikes.
- Severe Garbage Collection (GC) pauses affecting concurrent processes.
- Out-of-memory (OOM) termination of the entire BEAM node.

To avoid memory exhaustion, Elixir developers often attempt to combine `Ecto.Repo.stream/2` with `Plug.Conn.chunk/2` to stream database records directly to the HTTP client.

However, database cursors and public HTTP clients have fundamentally incompatible performance and reliability characteristics. This ADR evaluates the architectural trade-offs of database streaming and defines the streaming strategy for `beam_export`.

---

## 2. Problem Statement

> **Is directly streaming an Ecto database cursor to an arbitrary HTTP client a safe production architecture?**

Specifically, what happens to database resources, connection pools, and query snapshots when data is streamed directly to a client over the public internet?

---

## 3. In-Depth Technical Analysis of Direct Database-to-HTTP Streaming

### 3.1 Database Cursor & Transaction Lifecycle
`Ecto.Repo.stream/2` operates by declaring a server-side cursor (e.g., `DECLARE <cursor_name> NO SCROLL CURSOR FOR <query>` in PostgreSQL via `postgrex`). 
- In relational databases like PostgreSQL, named cursors **can only exist within the boundaries of an open database transaction**.
- As a consequence, `Ecto.Repo.stream/2` **must** be wrapped in `Repo.transaction(..., timeout: :infinity)`.
- The database connection checked out from `DBConnection` is exclusively reserved for that transaction until the block exits.

### 3.2 Connection Pool Starvation
In a typical Elixir production deployment:
- The default Ecto repository pool size is **10 connections** per node (`pool_size: 10`).
- If a client with a slow internet connection (e.g., mobile 3G, congested Wi-Fi, or high latency) requests a 100 MB export, the download may take 10 to 30 minutes.
- For the entire duration of that 30-minute download, **1 database connection is permanently checked out of the pool**.
- Just 5 to 10 concurrent export requests will consume **100% of available database connections**.
- All other application requests (logins, dashboard views, transactional queries) are queued in `DBConnection`. After `queue_target` / `queue_interval` (default 5,000 ms), the application experiences cascading failures:
  ```text
  ** (DBConnection.ConnectionError) connection not available and request was queued for 5000ms
  ```
- **Direct HTTP streaming weaponizes slow clients into a denial-of-service vector against the database.**

### 3.3 PostgreSQL MVCC, Table Bloat, and Locks
In PostgreSQL, an open transaction maintains an active snapshot:
- `VACUUM` (including autovacuum) cannot clean up dead tuples modified after the oldest active transaction's snapshot.
- Long-running transactions delay dead-tuple cleanup across the entire database, causing acute table and index bloat.
- If the queried tables undergo concurrent schema migrations or DDL modifications, long-running queries can trigger lock queues, blocking read and write operations.
- Production databases often enforce `idle_in_transaction_session_timeout` or query execution limits, causing premature stream termination.

### 3.4 The Illusion of Backpressure Over HTTP
Elixir `Stream` is pull-based. Downstream calls pull items from upstream.
When piping `Repo.stream` to `Plug.Conn.chunk/2`:
1. The BEAM process calls `chunk(conn, iodata)`.
2. Cowboy or Bandit writes bytes into the OS TCP socket send buffer (typically 128 KB – 4 MB).
3. The BEAM process pulls rapidly from the database until the OS socket buffer is full.
4. Once the socket buffer fills, `chunk/2` blocks waiting for the client to acknowledge TCP packets.
5. **Backpressure does occur, but it backpressures the database connection!**
6. The database transaction is artificially frozen, waiting on a slow browser thousands of miles away.

### 3.5 Client Disconnections and Resource Teardown
If a user closes the browser tab or loses network connectivity mid-download:
- The TCP socket sends a FIN or RST packet.
- The web server detects the socket closure only on the next write attempt, returning `{:error, :closed}` from `Plug.Conn.chunk/2`.
- If the stream pipeline does not use an interruptible loop (e.g. `Enum.reduce_while/3`), the process may fail to halt gracefully, leading to hanging cursor transactions until the socket timeout fires.

---

## 4. Options Considered

### Option A: Direct Database → HTTP Streaming
```text
┌─────────────────┐       ┌───────────────┐       ┌─────────────┐       ┌─────────────┐
│ Database Cursor │ ----> │ Elixir Stream │ ----> │ CSV Encoder │ ----> │ HTTP Client │
│ (Holds DB Conn) │       │ (In Memory)   │       │ (iodata)    │       │ (Slow Conn) │
└─────────────────┘       └───────────────┘       └─────────────┘       └─────────────┘
```
* **Pros:** Lowest time-to-first-byte (TTFB); zero disk I/O.
* **Cons:** Holds DB connection for the entire client transfer; vulnerable to slow clients and pool starvation; risks MVCC bloat.
* **Verdict:** Dangerous as a default; acceptable only in low-latency internal microservices or trusted networks.

---

### Option B: Staged File Streaming (Database → Temporary File → HTTP)
```text
STAGE 1: FAST DATABASE DUMP TO DISK (Runs in seconds)
┌─────────────────┐       ┌───────────────┐       ┌─────────────┐       ┌───────────────┐
│ Database Cursor │ ----> │ Elixir Stream │ ----> │ CSV Encoder │ ----> │ Temporary     │
│ (Holds DB Conn) │       │ (Bounded Mem) │       │ (iodata)    │       │ File on Disk  │
└─────────────────┘       └───────────────┘       └─────────────┘       └───────┬───────┘
                                                                                │
[DATABASE TRANSACTION COMMITS & CONNECTION RETURNED TO POOL IMMEDIATELY]       │
                                                                                │
STAGE 2: SERVE FILE OVER HTTP                                                   │
                                                                                ▼
┌─────────────┐                                                         ┌───────────────┐
│ HTTP Client │ <------------------------------------------------------ │ Temporary     │
│ (Slow Conn) │          (Plug.Conn.send_file or Chunked Stream)        │ File on Disk  │
└─────────────┘                                                         └───────┬───────┘
                                                                                │
[FILE AUTOMATICALLY DELETED AFTER SERVING OR ON CLIENT DISCONNECT] <────────────┘
```
* **Pros:**
  - **Connection Isolation:** Database query runs at full local disk/network speed (often 100k rows in 1–2 seconds). The DB connection is checked out for seconds, not minutes.
  - **Bounded Memory:** Memory remains strictly bounded to stream chunk size during file generation.
  - **Zero Database Impact During Download:** Slow clients consume the file from disk; no database connections or transaction snapshots are held.
  - **Kernel Acceleration:** Serving a file from disk allows leveraging the OS `sendfile(2)` system call (zero-copy from page cache to socket).
* **Cons:** Requires ephemeral disk space (typically `/tmp` or configured cache directory); small TTFB delay while the file stages.
* **Verdict:** **The superior, production-safe architecture for interactive web downloads.**

---

### Option C: Asynchronous Background Export (Job Queue → Storage → Notification)
```text
User Request ──> Enqueue Oban Job ──> 202 Accepted
                                           │
                                           ▼
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ DB Stream to     │ ──> │ Upload to S3/GCS │ ──> │ Notify User      │
│ Temp File        │     │ Object Storage   │     │ (Email/LiveView) │
└──────────────────┘     └──────────────────┘     └──────────────────┘
```
* **Pros:** Ideal for massive files (> 500 MB) or long-running analytics queries. Completely removes export latency from HTTP request timeouts.
* **Cons:** Requires external infrastructure (job queue, cloud storage, notification delivery).
* **Verdict:** Excellent pattern for enterprise workloads, but out of scope for the MVP core library. However, the core library must provide the file export primitive so background jobs can use it trivially.

---

## 5. Architectural Decision

We decide to build `beam_export` around a **modular, destination-driven streaming architecture** with the following concrete policies:

1. **Staged Temp File Export is the Recommended HTTP Pattern:**
   For web downloads (`Plug`/`Phoenix`), `beam_export` will provide a turnkey staged export workflow:
   - Stream query directly into an ephemeral temporary file with bounded memory.
   - Commit the database transaction and return the connection to the pool immediately.
   - Stream the file to the client with automatic file deletion on completion or client termination.

2. **Direct File Export is a First-Class Primitive:**
   Writing directly to a target file path (`BeamExport.to_file/2` or `BeamExport.run(..., destination: {:file, path})`) will be a primary entry point, enabling use in CLI tasks, scheduled jobs, and Oban workers.

3. **Direct HTTP Streaming is Supported as an Opt-In Alternative:**
   Direct HTTP chunking (`Plug.Conn.chunk/2`) will be supported for developers who intentionally choose it (e.g. low-latency microservice pipelines), but:
   - It will feature explicit documentation warnings regarding database connection starvation.
   - It will implement graceful disconnection handling via `reduce_while` to abort the database cursor immediately if the socket closes.

4. **Zero-Copy Memory Model:**
   The pipeline will stream `iodata` chunks rather than allocating concatenated binary strings, minimizing BEAM garbage collection overhead.

---

## 6. Consequences & Trade-offs

### Positive Consequences
- **Production Resilience:** Applications will not experience database connection pool exhaustion due to slow export downloads.
- **Predictable Memory Footprint:** Memory usage remains constant regardless of whether the exported dataset contains 1,000 or 1,000,000 records.
- **Fast Connection Checkout:** Database transactions run at peak database speed and release resources immediately.
- **Deterministic Teardown:** Temporary files and database cursors are guaranteed to close using Elixir resource management (`try/after` and `Stream.resource/3`).

### Negative Consequences & Mitigations
- **Disk Space Requirement:** Staged exports consume disk space equal to the uncompressed export file.
  - *Mitigation:* Temporary files are written to system temp directory (or user-configurable path) and cleaned up immediately upon completion, failure, or timeout.
- **Time-to-First-Byte (TTFB):** Staged exports introduce a brief delay before HTTP headers are sent while the database dump completes.
  - *Mitigation:* For a typical 50,000-row export, database streaming to local disk takes under 1.5 seconds, which is well within acceptable web download tolerances.

---

## 7. Summary Comparison Matrix

| Quality Attribute | Option A: Direct HTTP | Option B: Staged File (Recommended) | Option C: Background Queue |
|---|---|---|---|
| **BEAM Memory** | Constant (Low) | Constant (Low) | Constant (Low) |
| **DB Connection Duration** | Equals Client Download Time (Minutes) | Equals Local Query Time (Seconds) | Equals Local Query Time (Seconds) |
| **Vulnerability to Slow Clients** | **Severe (Outage Risk)** | **None (Zero DB impact)** | **None** |
| **Complexity** | Low | Low–Medium | High (Requires Queues & S3) |
| **Client TTFB** | Instantaneous | Short staging delay | Asynchronous (Polling/Email) |
| **Disk Space Needed** | None | Ephemeral (`/tmp`) | Ephemeral + Object Storage |
| **Production Safety** | ⚠️ High Risk | ✅ Production-Grade | ✅ Production-Grade |
