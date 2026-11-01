# Adversarial Engineering Review & Threat Model: `BeamExport`

**Status:** Completed  
**Review Team:** Independent Adversarial Red Team  
**Date:** November 2026  
**Target:** `BeamExport` v0.1.0  

---

## Executive Summary

We performed an independent adversarial code, architecture, memory, concurrency, and security audit of `BeamExport`. Our explicit directive was to challenge all prior assumptions, ignore self-congratulatory benchmarks, and identify realistic failure modes that will break under real production conditions.

Our findings reveal **multiple critical vulnerabilities and severe architectural flaws**:
1. **Connection Deadlock on Nested Transactions:** Calling `to_file` inside an existing `Repo.transaction` deadlocks or triggers `DBConnection.ConnectionError` timeouts because the background worker checks out a second connection.
2. **Zombie Database Workers (Connection Leaks):** The database streaming worker in `BeamExport.Ecto.Streamer` has no timeout in its message reception loop. If a consumer halts or crashes without cleanly signalling `{:halt, ref}`, the worker remains frozen in an open transaction indefinitely, permanently leaking a database connection.
3. **Data Integrity / Row Count Corruption:** `count_newlines` blindly counts ASCII `\n` across the raw CSV. Any single record containing multiline text (fully valid under RFC 4180) corrupts the reported row count.
4. **Data Corruption on Non-Atomic File Failures:** When `atomic: false` (the default!), failures halfway through leave truncated, corrupt files on disk.
5. **International Filename Mangling:** Sanitization regex `[^\w\.\-\_]` converts all non-ASCII Unicode characters (e.g. French, German, Japanese, Chinese) into underscores (`_______.csv`).
6. **Premature File Deletion in Slow Downloads:** Spawning an uncoordinated cleanup monitor on the request process risks deleting temporary files while the web server transport layer is still actively transmitting to a slow client.
7. **Exaggerated Benchmark & Memory Claims:** The claim of "Strictly flat $O(1)$ memory" is technically misleading; memory is bounded by chunk size and row size ($O(\text{chunk\_size} \times \text{max\_row\_size})$). A single 50 MB row causes massive memory allocation.

---

## Detailed Threat & Vulnerability Matrix

| ID | Category | Severity | Description |
|---|---|---|---|
| **BE-001** | Database / Concurrency | **CRITICAL** | Deadlock & connection pool exhaustion when called inside existing transactions or under constrained pool size. |
| **BE-002** | Data Integrity | **CRITICAL** | Row count corruption when records contain multiline fields with embedded `\n` or `\r\n`. |
| **BE-003** | Reliability / Leaks | **HIGH** | Zombie worker processes leak database connections permanently if parent process halts unexpectedly. |
| **BE-004** | Data Integrity | **HIGH** | Default `atomic: false` leaves partially written, truncated files on disk on failure. |
| **BE-005** | HTTP / Internationalization | **MEDIUM** | Filename sanitization mangles non-ASCII Unicode filenames into underscores. |
| **BE-006** | Performance / Memory | **MEDIUM** | Pathological large fields cause linear memory bloat; claims of pure $O(1)$ memory are inaccurate. |
| **BE-007** | Error Handling | **MEDIUM** | Pattern matching on `:file.open` and `:file.write` raises `MatchError` rather than returning standard `{:error, reason}` tuples. |
| **BE-008** | Security | **LOW** | Formula injection sanitization can be bypassed if malicious formulas are preceded by whitespace. |

---

## Analysis by Phase

### 1. Database Architecture & Concurrency Attacks (BE-001, BE-003)

#### The Two-Connection Deadlock Flaw
In `BeamExport.Ecto.Streamer`, every invocation of `build/3` unconditionally calls `spawn_link` and executes `repo.transaction/2` in a separate process.
- **Scenario:** An Oban job or Phoenix controller is already executing within `Repo.transaction(fn -> ... end)`.
- **Failure:** The calling process holds Connection 1. The worker process attempts to checkout Connection 2. If `pool_size: 1` or if all other connections are checked out by concurrent requests, the worker process blocks waiting for a free connection. The calling process blocks waiting for the worker.
- **Result:** Immediate deadlock leading to `DBConnection.ConnectionError: client timed out because it queued and checked out the connection for longer than 2000ms`.
- **Proof of Concept:** Verified empirically via test script; reproducing reproducible timeout failure.

#### The Zombie Connection Leak Flaw
In `Streamer.run_worker_loop`:
```elixir
receive do
  {:request_batch, ^ref} -> send(parent, {:batch, ref, batch})
  {:halt, ^ref} -> {:halt, nil}
end
```
There is no `after` timeout on this `receive`. If the caller process exits abnormally, drops the stream reference, or if an intermediate supervisor terminates the pipeline, the worker process remains alive, waiting for a message that will never arrive, while holding a live PostgreSQL/SQLite transaction open.

---

### 2. CSV Data Integrity Attacks (BE-002, BE-008)

#### Multiline Field Row-Count Corruption
RFC 4180 Section 2.6 states: *"Fields containing line breaks (CRLF), double quotes, and commas should be enclosed in double-quotes."*
- `BeamExport.Destination.File` previously attempted to optimize line counting by counting raw `\n` bytes in emitted iodata chunks using `count_newlines/2`.
- If a table has 10 rows, and 5 rows have comments with 4 line breaks each, `count_newlines/2` reports **30 rows** instead of **10 rows**.
- Telemetry measurements and returned metadata are fundamentally inaccurate.

#### Leading Whitespace Formula Injection
In `BeamExport.CSV.Formatter`:
`sanitize_formula/1` only checks if the first byte is `=, +, -, @, \t, \r`.
- **Attack:** An attacker injects `" =SUM(A1:B1)"` (preceded by spaces) or `"\u00A0=1+1"` (non-breaking space).
- Spreadsheet programs (LibreOffice Calc, certain versions of Excel) strip whitespace and execute the formula.

---

### 3. File Destination & Atomicity Attacks (BE-004, BE-007)

#### Truncated File Residue
The default option for `BeamExport.to_file/3` is `atomic: false`.
- If an export with 500,000 rows fails at row 250,000 (e.g. mapper exception, query timeout, or node shutdown), the file at `target_path` remains on disk containing the first 250,000 rows.
- Consumers checking `File.exists?(target_path)` ingest corrupt, incomplete data with no indication of failure.
- `atomic: true` must be the default for all file operations to ensure absolute transactionality on the filesystem.

#### MatchError Exception Contract Violation
In `lib/beam_export/destination/file.ex`:
```elixir
{:ok, file} = :file.open(...)
:ok = :file.write(file, chunk)
```
If `:file.open` fails (e.g. permission denied `:eacces`, read-only filesystem `:erofs`), the code raises a `MatchError`, which is caught by `rescue e` and returned as `{:error, %MatchError{term: {:error, :eacces}}}` instead of the standard Elixir `{:error, :eacces}`.

---

### 4. HTTP & Web Attacks (BE-005)

#### Non-ASCII Filename Destruction
In `BeamExport.Plug`:
```elixir
String.replace(filename, ~r/[^\w\.\-\_]/, "_")
```
`\w` in standard regex without unicode modifiers matches only ASCII `[a-zA-Z0-9_]`.
- Passing `"Benutzerliste_März.csv"` generates `"Benutzerliste_M_rz.csv"`.
- Passing `"顧客一覧.csv"` (Japanese: Customer List) generates `"____.csv"`.
- This violates modern internationalization standards and breaks multi-language applications.

---

### 5. Benchmark Audit & Reality Check (BE-006)

#### The "Strictly Flat O(1) Memory" Myth
The previous benchmark claimed "Strictly Flat $O(1)$ memory".
- In reality, memory usage in any streaming pipeline is bounded by the **chunk size** and the **maximum row size**:
  $$\text{Memory} = O(\text{chunk\_size} \times \text{max\_row\_size})$$
- If a dataset contains rows with 10 MB blobs or long text columns, chunking 1,000 rows at a time will buffer $1,000 \times 10\text{ MB} = 10\text{ GB}$ of data in memory, crashing the node!
- The documentation and marketing claims must be corrected to explain chunk-level memory dynamics honestly.

---

## Action Plan for Hardening

1. **Fix BE-001 (Two-Connection Deadlock):**
   - Check if the calling process is already in a transaction (`repo.in_transaction?()`).
   - If in a transaction, stream directly in the current process without spawning an auxiliary worker.
   - For `to_file/3`, execute `repo.transaction` directly in the caller process so zero sub-processes and zero extra pool connections are required.
2. **Fix BE-002 (Row Count Corruption):**
   - Track row counts based on mapped item count during chunk generation rather than counting `\n` in emitted text.
3. **Fix BE-003 (Zombie Workers):**
   - Add monitor-based termination and explicit timeouts to the worker `receive` loop.
4. **Fix BE-004 (Truncated Files):**
   - Clean up partial files on failure even when `atomic: false`, or make `atomic: true` the default.
5. **Fix BE-005 (Unicode Filenames):**
   - Support UTF-8 encoded `Content-Disposition` headers (`filename*="UTF-8''..."`) according to RFC 6266 / RFC 5987.
6. **Fix BE-007 (Error Returns):**
   - Properly handle `{:error, reason}` from `:file.open` and `:file.write`.
7. **Fix BE-008 (Formula Injection):**
   - Strip leading whitespace before checking formula prefixes.
