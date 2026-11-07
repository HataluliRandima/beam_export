defmodule BeamExport.Ecto.StreamerTest do
  # Regression tests for audit finding C1 (silent truncation) and M8 (worker lifecycle).
  use BeamExport.DataCase, async: false

  alias BeamExport.Test.FakeRepo

  defmodule HangingRepo do
    use BeamExport.Test.FakeRepo
    def stream(_query, _opts), do: Stream.repeatedly(fn -> Process.sleep(:infinity) end)
  end

  defmodule EndlessRepo do
    use BeamExport.Test.FakeRepo
    def stream(_query, _opts), do: Stream.iterate(1, &(&1 + 1)) |> Stream.map(&%{id: &1})
  end

  defmodule RaisingRepo do
    use BeamExport.Test.FakeRepo

    def stream(_query, _opts) do
      Stream.map(1..10, fn
        5 -> raise "database exploded"
        i -> %{id: i}
      end)
    end
  end

  defmodule ExitingRepo do
    use BeamExport.Test.FakeRepo
    def stream(_query, _opts), do: Stream.map(1..10, fn _ -> exit(:driver_gone) end)
  end

  defmodule ThreeRowsRepo do
    use BeamExport.Test.FakeRepo
    def stream(_query, _opts), do: Enum.map(1..3, &%{id: &1})
  end

  defmodule CommitFailsRepo do
    use BeamExport.Test.FakeRepo
    def stream(_query, _opts), do: Enum.map(1..3, &%{id: &1})

    # Simulates a COMMIT that fails after every row was read.
    def transaction(fun, opts) do
      _ = super(fun, opts)
      {:error, :commit_failed}
    end
  end

  @protocol_messages [:batch, :done, :error, :request_batch]

  setup do
    FakeRepo.listen()
    :ok
  end

  defp protocol_residue do
    {:messages, messages} = Process.info(self(), :messages)

    Enum.filter(messages, fn
      tuple when is_tuple(tuple) -> elem(tuple, 0) in @protocol_messages
      _ -> false
    end)
  end

  defp worker_pid do
    assert_receive {:worker, pid}, 1_000
    pid
  end

  describe "C1: an export that stops early is never reported as complete" do
    test "slow consumer raises TimeoutError instead of silently truncating" do
      insert_users(50)
      query = from(u in User, order_by: u.id)

      stream =
        BeamExport.to_stream({Repo, query},
          mapper: &[&1.id],
          max_rows: 10,
          chunk_size: 10,
          consumer_timeout: 100
        )

      error =
        assert_raise BeamExport.TimeoutError, fn ->
          stream
          |> Stream.each(fn _chunk -> Process.sleep(300) end)
          |> Stream.run()
        end

      assert error.reason == :consumer_timeout
      assert error.timeout == 100
      assert Exception.message(error) =~ "INCOMPLETE"
    end

    test "a failed commit raises instead of ending the stream normally" do
      error =
        assert_raise BeamExport.ExportError, fn ->
          BeamExport.to_stream({CommitFailsRepo, User}, mapper: &[&1.id]) |> Stream.run()
        end

      assert error.reason == :commit_failed
    end

    test "a database that stops answering raises TimeoutError and kills the worker" do
      stream = BeamExport.to_stream({HangingRepo, User}, mapper: &[&1.id], batch_timeout: 100)

      error = assert_raise BeamExport.TimeoutError, fn -> Stream.run(stream) end
      assert error.reason == :batch_timeout

      worker = worker_pid()
      refute Process.alive?(worker)
      assert protocol_residue() == []
    end
  end

  describe "worker failure propagation" do
    test "an exception in the cursor is re-raised in the consumer with its original type" do
      assert_raise RuntimeError, "database exploded", fn ->
        BeamExport.to_stream({RaisingRepo, User}, mapper: &[&1.id], max_rows: 2)
        |> Stream.run()
      end

      refute Process.alive?(worker_pid())
    end

    test "an exit in the cursor is re-raised as an exit in the consumer" do
      assert catch_exit(
               BeamExport.to_stream({ExitingRepo, User}, mapper: &[&1.id])
               |> Stream.run()
             ) == :driver_gone
    end

    test "a worker killed from outside becomes an ExportError, not a linked crash" do
      parent = self()

      stream =
        BeamExport.to_stream({EndlessRepo, User}, mapper: &[&1.id], max_rows: 5, chunk_size: 5)

      error =
        assert_raise BeamExport.ExportError, fn ->
          stream
          |> Stream.each(fn _ ->
            receive do
              {:worker, pid} -> Process.exit(pid, :kill)
            after
              0 -> :ok
            end
          end)
          |> Stream.run()
        end

      assert error.reason == :killed
      assert Process.alive?(parent)
    end
  end

  describe "worker lifecycle" do
    test "halting early stops the worker and leaves no messages in the caller's mailbox" do
      result =
        BeamExport.to_stream({EndlessRepo, User}, mapper: &[&1.id], max_rows: 5, chunk_size: 5)
        |> Enum.take(2)

      assert length(result) == 2
      worker = worker_pid()
      refute Process.alive?(worker), "worker must have exited before the stream returned"
      assert protocol_residue() == []
    end

    test "the worker exits when its consumer dies" do
      consumer =
        spawn(fn ->
          BeamExport.to_stream({EndlessRepo, User}, mapper: &[&1.id], max_rows: 5)
          |> Stream.each(fn _ -> Process.exit(self(), :kill) end)
          |> Stream.run()
        end)

      worker = worker_pid()
      ref = Process.monitor(worker)
      assert_receive {:DOWN, ^ref, :process, ^worker, _reason}, 2_000
      refute Process.alive?(consumer)
    end

    test "to_iodata runs the cursor in the calling process (no worker)" do
      assert {:ok, iodata, %{rows: 3}} =
               BeamExport.to_iodata({ThreeRowsRepo, User}, mapper: &[&1.id])

      assert IO.iodata_to_binary(iodata) == "1\r\n2\r\n3\r\n"
      # The fake repo reports which process opened the transaction.
      assert worker_pid() == self()
    end

    test "to_iodata returns a commit failure as an error tuple" do
      assert {:error, %BeamExport.ExportError{reason: :commit_failed}} =
               BeamExport.to_iodata({CommitFailsRepo, User}, mapper: &[&1.id])
    end
  end
end
