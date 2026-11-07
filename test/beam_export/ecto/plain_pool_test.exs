defmodule BeamExport.Ecto.PlainPoolTest do
  # Runs against a regular (non-sandbox) connection pool, like production.
  #
  # Each test starts its own repo under the test supervisor with a fresh database,
  # so tests that make DBConnection drop connections cannot affect anything else.
  use ExUnit.Case, async: false

  alias BeamExport.Test.{PlainRepo, User}

  setup do
    path =
      Path.join(System.tmp_dir!(), "beam_export_plain_#{System.unique_integer([:positive])}.db")

    on_exit(fn -> for s <- ["", "-shm", "-wal"], do: File.rm(path <> s) end)

    # Switch to WAL once, before the pool's connections race to do it.
    {:ok, conn} = Exqlite.Sqlite3.open(path)
    :ok = Exqlite.Sqlite3.execute(conn, "PRAGMA journal_mode=WAL")
    :ok = Exqlite.Sqlite3.close(conn)

    start_supervised!({PlainRepo, database: path, pool_size: 2, journal_mode: :wal})
    Ecto.Migrator.up(PlainRepo, 1, BeamExport.Test.Migration, log: false)
    now = ~N[2026-01-01 00:00:00]

    PlainRepo.insert_all(
      User,
      for(
        i <- 1..30,
        do: %{name: "P#{i}", email: "p#{i}@x.io", inserted_at: now, updated_at: now}
      )
    )

    :ok
  end

  test "to_stream outside a transaction streams every row" do
    lines =
      BeamExport.to_stream({PlainRepo, User}, mapper: &[&1.id], max_rows: 7)
      |> Enum.to_list()
      |> IO.iodata_to_binary()
      |> String.split("\r\n", trim: true)

    assert length(lines) == 30
  end

  test "to_file and to_iodata export every row" do
    path = Path.join(System.tmp_dir!(), "plain_#{System.unique_integer([:positive])}.csv")
    on_exit(fn -> File.rm(path) end)

    assert {:ok, %{rows: 30}} = BeamExport.to_file({PlainRepo, User}, path, mapper: &[&1.id])
    assert {:ok, _iodata, %{rows: 30}} = BeamExport.to_iodata({PlainRepo, User}, mapper: &[&1.id])
  end

  # An integer :timeout caps how long the transaction may hold its connection
  # (DBConnection semantics), so DBConnection drops the connection mid-export.
  # Depending on timing this surfaces as a rollback (ExportError), a
  # DBConnection.ConnectionError or a driver error from the next fetch. The
  # property under test: it is never a silent, successful truncation.
  @tag :capture_log
  test "an expiring :timeout fails the export instead of truncating it" do
    stream =
      BeamExport.to_stream({PlainRepo, User},
        mapper: &[&1.id],
        max_rows: 10,
        chunk_size: 10,
        timeout: 150
      )

    error =
      try do
        stream |> Stream.each(fn _ -> Process.sleep(400) end) |> Stream.run()
        :no_error
      rescue
        e -> e
      end

    refute error == :no_error, "an export cut off by :timeout must raise"
    assert is_exception(error)
  end
end
