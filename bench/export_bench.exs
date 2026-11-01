# Comprehensive Benchmark Suite for BeamExport vs Traditional In-Memory Export

Application.ensure_all_started(:beam_export)

alias BeamExport.Test.Repo
alias BeamExport.Test.User

# Setup SQLite database for benchmark
db_path = Path.join(System.tmp_dir!(), "beam_export_bench.db")
File.rm(db_path)

Application.put_env(:beam_export, BeamExport.Test.Repo,
  database: db_path,
  pool_size: 5,
  journal_mode: :wal
)

{:ok, _} = BeamExport.Test.Repo.start_link()
Ecto.Migrator.up(BeamExport.Test.Repo, 1, BeamExport.Test.Migration, log: false)

measure_memory = fn name, func ->
  parent = self()

  pid =
    spawn(fn ->
      :erlang.garbage_collect()
      mem_before = :erlang.process_info(self(), :memory) |> elem(1)

      t0 = System.monotonic_time(:millisecond)
      result = func.()
      t1 = System.monotonic_time(:millisecond)

      mem_peak = :erlang.process_info(self(), :memory) |> elem(1)
      mem_diff_kb = Float.round((mem_peak - mem_before) / 1024, 2)
      duration_ms = t1 - t0

      send(parent, {:done, name, duration_ms, mem_diff_kb, result})
    end)

  receive do
    {:done, ^name, duration_ms, mem_diff_kb, _result} ->
      {duration_ms, mem_diff_kb}
  after
    120_000 ->
      Process.exit(pid, :kill)
      {:timeout, :timeout}
  end
end

seed_users = fn target_count ->
  current_count = Repo.aggregate(User, :count, :id) || 0
  needed = target_count - current_count

  if needed > 0 do
    now = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
    batch_size = 5_000

    chunks = div(needed + batch_size - 1, batch_size)

    for chunk_idx <- 1..chunks do
      start_id = current_count + (chunk_idx - 1) * batch_size + 1
      chunk_end = min(current_count + chunk_idx * batch_size, target_count)

      rows =
        for i <- start_id..chunk_end do
          %{
            name: "User #{i}",
            email: "user_#{i}@production-export-example.com",
            role: if(rem(i, 5) == 0, do: "admin", else: "member"),
            inserted_at: now,
            updated_at: now
          }
        end

      Repo.insert_all(User, rows)
    end
  end
end

test_sizes = [10_000, 50_000, 100_000]

IO.puts("\n==========================================================================")
IO.puts("   BEAM_EXPORT BENCHMARK: MEMORY & EXECUTION PROFILE")
IO.puts("==========================================================================")
IO.puts(String.pad_trailing("Dataset Size", 15) <>
        String.pad_trailing("Approach", 25) <>
        String.pad_trailing("Duration", 15) <>
        String.pad_trailing("Peak Memory", 18))
IO.puts(String.duplicate("-", 73))

for size <- test_sizes do
  seed_users.(size)

  # Traditional Repo.all
  trad_fn = fn ->
    users = Repo.all(User)
    rows = Enum.map(users, fn u -> [Integer.to_string(u.id), u.name, u.email, u.role] end)
    headers = [["ID", "Name", "Email", "Role"]]
    bin = NimbleCSV.RFC4180.dump_to_iodata(headers ++ rows) |> IO.iodata_to_binary()
    out = Path.join(System.tmp_dir!(), "trad_#{size}.csv")
    File.write!(out, bin)
    File.rm(out)
  end

  # BeamExport Streaming
  beam_fn = fn ->
    out = Path.join(System.tmp_dir!(), "beam_#{size}.csv")
    {:ok, _} =
      BeamExport.to_file(
        {Repo, User},
        out,
        headers: ["ID", "Name", "Email", "Role"],
        mapper: fn u -> [u.id, u.name, u.email, u.role] end,
        max_rows: 1_000
      )
    File.rm(out)
  end

  {t_ms_trad, mem_kb_trad} = measure_memory.("Trad #{size}", trad_fn)
  {t_ms_beam, mem_kb_beam} = measure_memory.("Beam #{size}", beam_fn)

  IO.puts(
    String.pad_trailing("#{size} rows", 15) <>
    String.pad_trailing("Traditional (Repo.all)", 25) <>
    String.pad_trailing("#{t_ms_trad} ms", 15) <>
    String.pad_trailing("#{Float.round(mem_kb_trad / 1024, 2)} MB", 18)
  )

  IO.puts(
    String.pad_trailing("#{size} rows", 15) <>
    String.pad_trailing("BeamExport (Streaming)", 25) <>
    String.pad_trailing("#{t_ms_beam} ms", 15) <>
    String.pad_trailing("#{Float.round(mem_kb_beam / 1024, 2)} MB", 18)
  )

  reduction = Float.round(mem_kb_trad / max(mem_kb_beam, 1.0), 1)
  IO.puts("   ↳ Memory Reduction: #{reduction}x lower peak process memory\n")
end

File.rm(db_path)
