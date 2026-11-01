defmodule BeamExport.TelemetryTest do
  use ExUnit.Case, async: false

  def handle_telemetry_event(event, measurements, metadata, %{test_pid: test_pid}) do
    send(test_pid, {:telemetry_event, event, measurements, metadata})
  end

  setup do
    test_pid = self()
    handler_id = "test-telemetry-handler-#{:rand.uniform(100_000)}"

    events = [
      [:beam_export, :export, :start],
      [:beam_export, :export, :chunk],
      [:beam_export, :export, :stop],
      [:beam_export, :export, :exception]
    ]

    :telemetry.attach_many(
      handler_id,
      events,
      &__MODULE__.handle_telemetry_event/4,
      %{test_pid: test_pid}
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    :ok
  end

  describe "Telemetry instrumentation" do
    test "emits start, chunk, and stop events on successful export to file" do
      tmp_path = Path.join(System.tmp_dir!(), "telemetry_test_#{:rand.uniform(10_000)}.csv")

      data = [
        [1, "Alpha"],
        [2, "Beta"]
      ]

      assert {:ok, _} = BeamExport.to_file(data, tmp_path, headers: ["ID", "Name"])

      assert_receive {:telemetry_event, [:beam_export, :export, :start], %{system_time: _},
                      %{destination: :file, format: :csv}}

      assert_receive {:telemetry_event, [:beam_export, :export, :chunk], %{bytes: bytes},
                      %{destination: :file}}

      assert bytes > 0

      assert_receive {:telemetry_event, [:beam_export, :export, :stop],
                      %{duration: duration, rows: rows, bytes: total_bytes},
                      %{destination: :file, format: :csv}}

      assert duration > 0
      assert rows == 3
      assert total_bytes > 0

      File.rm(tmp_path)
    end

    test "emits start and stop events on to_iodata" do
      assert {:ok, _, _} = BeamExport.to_iodata([[1, "Val"]], headers: ["Num", "Str"])

      assert_receive {:telemetry_event, [:beam_export, :export, :start], _,
                      %{destination: :iodata}}

      assert_receive {:telemetry_event, [:beam_export, :export, :stop], %{rows: 2},
                      %{destination: :iodata}}
    end

    test "emits exception event on failure without leaking row data" do
      tmp_path = Path.join(System.tmp_dir!(), "telemetry_fail_#{:rand.uniform(10_000)}.csv")

      # Failure in mapper
      assert {:error, _} =
               BeamExport.to_file(
                 [%{secret_ssn: "123-45-6789"}],
                 tmp_path,
                 mapper: fn _item -> raise("mapper error") end
               )

      assert_receive {:telemetry_event, [:beam_export, :export, :exception], %{duration: _},
                      metadata}

      assert metadata.destination == :file
      assert metadata.kind == :error

      # Ensure metadata does not contain the sensitive row data
      metadata_binary = inspect(metadata)
      refute metadata_binary =~ "123-45-6789"
    end
  end
end
