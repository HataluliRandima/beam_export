# Each test run gets its own SQLite database so that runs (and leftovers from
# earlier runs) can never interfere with each other.
run_id = System.unique_integer([:positive])
db_path = Path.join(System.tmp_dir!(), "beam_export_test_#{run_id}.db")

# Switch the new database to WAL mode once, up front. Otherwise every pool
# connection races to change the journal mode and some fail with "database is locked".
for path <- [db_path] do
  {:ok, conn} = Exqlite.Sqlite3.open(path)
  :ok = Exqlite.Sqlite3.execute(conn, "PRAGMA journal_mode=WAL")
  :ok = Exqlite.Sqlite3.close(conn)
end

Application.put_env(:beam_export, BeamExport.Test.Repo,
  database: db_path,
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 4,
  busy_timeout: 10_000,
  journal_mode: :wal
)

{:ok, _} = BeamExport.Test.Repo.start_link()
Ecto.Adapters.SQL.Sandbox.mode(BeamExport.Test.Repo, :auto)
Ecto.Migrator.up(BeamExport.Test.Repo, 1, BeamExport.Test.Migration, log: false)
Ecto.Adapters.SQL.Sandbox.mode(BeamExport.Test.Repo, :manual)

# Not ExUnit.after_suite/1: that runs after every --repeat-until-failure iteration.
System.at_exit(fn _status ->
  for suffix <- ["", "-shm", "-wal"] do
    File.rm(db_path <> suffix)
  end
end)

# Disk-full tests write to /dev/full, which only exists on Linux.
exclude = if File.exists?("/dev/full"), do: [], else: [:requires_dev_full]
ExUnit.start(exclude: exclude)
