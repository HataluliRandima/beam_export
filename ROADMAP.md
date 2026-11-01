# BeamExport Roadmap

This document outlines the planned future milestones and technical architecture for upcoming releases of `BeamExport`.

---

## 🎯 Release Horizons

```text
v0.1.0 (Current)              v0.2.0                        v0.3.0                        v0.4.0
┌───────────────────────┐     ┌───────────────────────┐     ┌───────────────────────┐     ┌───────────────────────┐
│ • Production CSV Core │ ──> │ • Cloud Storage Sinks │ ──> │ • Additional Formats  │ ──> │ • Oban Background     │
│ • Ecto Cursor Stream  │     │   (S3, GCS, R2)       │     │   (NDJSON, Parquet)   │     │   Worker Blueprint    │
│ • Plug Staged/Chunked │     │ • Multipart Streaming │     │ • Pluggable Format    │     │ • Dialyzer in CI      │
│ • Hardened Security   │     │ • Zero-Disk Uploads   │     │   Architecture        │     │ • Progress Webhooks   │
└───────────────────────┘     └───────────────────────┘     └───────────────────────┘     └───────────────────────┘
```

---

## ☁️ Milestone 1 (v0.2.0): Cloud Object Storage Sinks

### Goal
Directly stream large datasets to AWS S3, Google Cloud Storage, Cloudflare R2, or MinIO via S3-compatible multipart uploads without requiring intermediate local disk storage.

### Planned Architecture: `BeamExport.Destination.S3`
```text
┌─────────────────┐       ┌─────────────────┐       ┌────────────────────────┐       ┌─────────────────┐
│ Ecto DB Cursor  │ ────> │ CSV/Gzip Stream │ ────> │ 5MB Part Buffer Stream │ ────> │ S3 Multipart    │
│ (Repo.stream)   │       │ (iodata chunks) │       │ (Stream.chunk_while)   │       │ Upload Parts    │
└─────────────────┘       └─────────────────┘       └────────────────────────┘       └─────────────────┘
```

### Proposed API
```elixir
{:ok, meta} =
  BeamExport.to_s3(
    {MyApp.Repo, query},
    bucket: "company-exports",
    key: "exports/users_2026.csv.gz",
    s3_client: MyApp.S3Client, # e.g. ExAws.S3 or ReqS3
    compress: :gzip,
    part_size_mb: 5,
    headers: ["ID", "Email", "Name"],
    mapper: fn u -> [u.id, u.email, u.name] end
  )
```

### Key Considerations
- **Minimum Part Size:** S3 requires all multipart parts (except the last) to be $\ge 5\text{ MB}$. The pipeline will accumulate iodata up to 5 MB per part before dispatching asynchronously.
- **Connection Isolation:** Database cursor is held only while reading and uploading chunks; once complete, the transaction commits and the multipart upload finalizes.
- **Error Abort:** If an error occurs mid-stream, `AbortMultipartUpload` is automatically invoked to prevent orphaned, billed storage parts.

---

## 📊 Milestone 2 (v0.3.0): Multi-Format Extensibility (NDJSON & Parquet)

### Goal
Extend `BeamExport` beyond RFC 4180 CSV to support modern machine learning and analytics data interchange formats.

### Target Formats
1. **Newline-Delimited JSON (`NDJSON` / `.jsonl`):**
   - High-throughput streaming of structured JSON objects.
   - Built on `Jason.encode_to_iodata!/1` with zero extra string allocations.
   - Ideal for log exports, Elasticsearch indexing, and BigQuery data ingestion.

2. **Apache Parquet (`.parquet`):**
   - Columnar storage format for analytical queries (DuckDB, Snowflake, ClickHouse, Pandas).
   - High compression ratio and typed schema definitions.

### Proposed API
```elixir
# Streaming NDJSON export:
BeamExport.to_file(
  {MyApp.Repo, query},
  "exports/events.ndjson.gz",
  format: :ndjson,
  mapper: fn event -> %{id: event.id, type: event.type, payload: event.data} end
)
```

---

## ⚙️ Milestone 3 (v0.4.0): Oban Background Worker Blueprint

### Goal
Provide a production-ready, turnkey Oban worker behavior and recipe for handling asynchronous background export workflows.

### Planned Features: `BeamExport.Oban.Worker`
- **Standardized Job Lifecycle:** Automatic state transitions (`queued` $\to$ `exporting` $\to$ `uploaded` $\to$ `completed` / `failed`).
- **Telemetry-Driven Progress:** Emits chunk progress events so web apps can display live export progress bars over Phoenix LiveView or WebSockets.
- **Signed URL Delivery:** Generates short-lived pre-signed download URLs (e.g. valid for 24 hours) upon completion and triggers email or webhook notifications.
- **Resource Limits:** Built-in concurrency rate-limiting across Oban queues to prevent concurrent export jobs from starving primary application database pools.

### Proposed Usage
```elixir
defmodule MyApp.Workers.UserExportWorker do
  use BeamExport.Oban.Worker,
    repo: MyApp.Repo,
    storage: :s3,
    bucket: "my-app-exports"

  @impl BeamExport.Oban.Worker
  def build_query(%{"organization_id" => org_id}) do
    from(u in MyApp.User, where: u.org_id == ^org_id)
  end

  @impl BeamExport.Oban.Worker
  def mapper(user), do: [user.id, user.email, user.name]
end
```

---

## 🔍 Milestone 4: CI & Static Analysis (Dialyzer)

### Goal
Ensure comprehensive static type safety and contract verification across all optional dependency configurations.

### Action Items
- Add `dialyxir` to `mix.exs` `:dev` and `:test` environments.
- Generate and cache Dialyzer Persistent Lookup Tables (PLTs) in GitHub Actions (`.github/workflows/ci.yml`).
- Enforce clean typespec verification with zero Dialyzer warnings in pull request checks.
