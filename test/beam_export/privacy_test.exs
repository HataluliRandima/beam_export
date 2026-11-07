defmodule BeamExport.PrivacyTest do
  # Regression tests for audit finding H2: record data must not leak into returned
  # errors, raised exceptions, stacktraces, telemetry or Plug exceptions by default.
  use ExUnit.Case, async: false

  @ssn "SSN-123-45-6789"
  @hash "$argon2id$v=19$secret-hash"
  @record %{id: 7, ssn: @ssn, password_hash: @hash, ok: false}

  setup do
    test_pid = self()
    handler_id = {__MODULE__, make_ref()}

    :telemetry.attach(
      handler_id,
      [:beam_export, :export, :exception],
      &__MODULE__.forward_exception_event/4,
      test_pid
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    dir =
      Path.join(System.tmp_dir!(), "beam_export_privacy_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, dir: dir}
  end

  def forward_exception_event(_event, _measurements, metadata, test_pid) do
    if self() == test_pid, do: send(test_pid, {:exception_event, metadata})
  end

  # Every kind of mapper failure that carries the record in some field.
  @leaky_mappers [
    key_error: &__MODULE__.key_error_mapper/1,
    function_clause: &__MODULE__.function_clause_mapper/1,
    match_error: &__MODULE__.match_error_mapper/1,
    bad_map: &__MODULE__.bad_map_mapper/1,
    raise_with_record: &__MODULE__.raise_with_record_mapper/1
  ]

  def key_error_mapper(record), do: [Map.fetch!(record, :missing_field)]
  def function_clause_mapper(%{ok: true} = record), do: [record.id]
  def match_error_mapper(record), do: [%{ok: true}] = [record]
  def bad_map_mapper(record), do: [Map.get(record.ssn, :x)]
  def raise_with_record_mapper(record), do: raise("could not map #{inspect(record)}")

  defp leaks?(term),
    do: inspect(term, limit: :infinity, printable_limit: :infinity) =~ ~r/#{@ssn}|argon2id/

  for {name, mapper} <- @leaky_mappers do
    @mapper mapper

    test "#{name}: to_file error, telemetry reason and telemetry stacktrace are redacted",
         %{dir: dir} do
      assert {:error, %BeamExport.MappingError{} = error} =
               BeamExport.to_file([@record], Path.join(dir, "out.csv"), mapper: @mapper)

      refute leaks?(error)
      refute Exception.message(error) =~ @ssn
      assert error.record == :redacted
      assert error.original_error == :redacted
      assert error.row == 1

      assert_received {:exception_event, metadata}
      refute leaks?(metadata.reason)
      refute leaks?(metadata.stacktrace)
    end

    test "#{name}: exceptions raised by to_stream carry no record data in message or stacktrace" do
      {error, stacktrace} =
        try do
          BeamExport.to_stream([@record], mapper: @mapper) |> Stream.run()
          flunk("expected a MappingError")
        rescue
          e in BeamExport.MappingError -> {e, __STACKTRACE__}
        end

      refute leaks?(error)
      refute leaks?(stacktrace)
      # This is what Logger prints for a crashed process.
      refute Exception.format(:error, error, stacktrace) =~ @ssn
    end

    test "#{name}: to_iodata returns a redacted error" do
      assert {:error, %BeamExport.MappingError{} = error} =
               BeamExport.to_iodata([@record], mapper: @mapper)

      refute leaks?(error)
    end
  end

  test "the redacted message still identifies the failure" do
    assert {:error, error} = BeamExport.to_iodata([@record], mapper: &key_error_mapper/1)
    assert error.row == 1
    assert error.reason == {:exception, KeyError}
    assert Exception.message(error) =~ "KeyError"
    assert Exception.message(error) =~ "key :missing_field not found"
    assert Exception.message(error) =~ "data row 1"
    assert Exception.message(error) =~ "include_record_in_errors: true"
  end

  test "the row number points at the failing record" do
    assert {:error, error} =
             BeamExport.to_iodata([%{a: 1}, %{a: 2}, @record], mapper: &[Map.fetch!(&1, :a)])

    assert error.row == 3
  end

  describe "include_record_in_errors: true (opt-in)" do
    test "includes the record and original error in the returned error" do
      assert {:error, error} =
               BeamExport.to_iodata([@record],
                 mapper: &key_error_mapper/1,
                 include_record_in_errors: true
               )

      assert error.record == @record
      assert %KeyError{} = error.original_error
      assert Exception.message(error) =~ @ssn
    end

    test "still never sends the record to telemetry", %{dir: dir} do
      assert {:error, _} =
               BeamExport.to_file([@record], Path.join(dir, "o.csv"),
                 mapper: &function_clause_mapper/1,
                 include_record_in_errors: true
               )

      assert_received {:exception_event, metadata}
      refute leaks?(metadata)
    end
  end

  describe "Plug staged export failures" do
    import Plug.Test

    test "raise an ExportError without record data", %{dir: dir} do
      error =
        assert_raise BeamExport.ExportError, fn ->
          BeamExport.Plug.send_staged_export(conn(:get, "/"), "x.csv", [@record],
            mapper: &key_error_mapper/1,
            tmp_dir: dir
          )
        end

      refute leaks?(error)
      assert Exception.message(error) =~ "KeyError"
      assert %BeamExport.MappingError{} = error.reason
      # The private temporary directory is removed on failure.
      assert File.ls!(dir) == []
    end
  end
end
