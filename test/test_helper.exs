db_path = Path.join(System.tmp_dir!(), "beam_export_test.db")

Application.put_env(:beam_export, BeamExport.Test.Repo,
  database: db_path,
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 2,
  busy_timeout: 10_000,
  journal_mode: :wal
)

{:ok, _} = BeamExport.Test.Repo.start_link()
Ecto.Adapters.SQL.Sandbox.mode(BeamExport.Test.Repo, :auto)
Ecto.Migrator.up(BeamExport.Test.Repo, 1, BeamExport.Test.Migration, log: false)
Ecto.Adapters.SQL.Sandbox.mode(BeamExport.Test.Repo, :manual)

ExUnit.start()
