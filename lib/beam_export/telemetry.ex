defmodule BeamExport.Telemetry do
  @moduledoc """
  `:telemetry` events emitted by BeamExport.

  Events are emitted by `BeamExport.to_file/3` (`destination: :file`, including
  `BeamExport.Plug.send_staged_export/4`) and `BeamExport.to_iodata/2`
  (`destination: :iodata`). `BeamExport.to_stream/2` and
  `BeamExport.Plug.send_chunked_export/4` emit no events.

  Handlers run synchronously in the process performing the export.

    * `[:beam_export, :export, :start]` - an export begins (after option validation).
      * Measurements: `%{system_time: integer()}`
      * Metadata: `%{destination: :file | :iodata, format: :csv}`
    * `[:beam_export, :export, :chunk]` - a chunk was written (`:file` only).
      * Measurements: `%{rows: integer(), bytes: integer()}` - `bytes` are uncompressed
      * Metadata: `%{destination: :file}`
    * `[:beam_export, :export, :stop]` - the export completed successfully.
      * Measurements: `%{duration: integer(), rows: integer(), bytes: integer()}`.
        `duration` is in `:native` time units (convert with `System.convert_time_unit/3`),
        `rows` includes the header row, and `bytes` is the size on disk (compressed
        size for gzip files).
      * Metadata: `%{destination: :file | :iodata, format: :csv}`
    * `[:beam_export, :export, :exception]` - the export failed.
      * Measurements: `%{duration: integer()}` (`:native` time units)
      * Metadata: `%{destination: atom(), kind: :error, reason: Exception.t(),
        stacktrace: list(), format: :csv}`

  Exception metadata never contains record contents: `BeamExport.MappingError`s are
  always redacted (even with `include_record_in_errors: true`), and stacktrace
  entries carry arities instead of arguments. Exceptions raised by your own source or
  database driver are passed through unchanged.
  """

  @prefix [:beam_export, :export]

  @doc false
  def start(destination, opts) do
    :telemetry.execute(
      @prefix ++ [:start],
      %{system_time: System.system_time()},
      %{
        destination: destination,
        format: Keyword.get(opts, :format, :csv)
      }
    )
  end

  @doc false
  def chunk(destination, rows, bytes, _opts) do
    :telemetry.execute(
      @prefix ++ [:chunk],
      %{rows: rows, bytes: bytes},
      %{destination: destination}
    )
  end

  @doc false
  def stop(destination, %{duration: duration, rows: rows, bytes: bytes}, opts) do
    :telemetry.execute(
      @prefix ++ [:stop],
      %{duration: duration, rows: rows, bytes: bytes},
      %{
        destination: destination,
        format: Keyword.get(opts, :format, :csv)
      }
    )
  end

  @doc false
  def exception(destination, start_time, error, stacktrace, opts) do
    duration = System.monotonic_time() - start_time

    :telemetry.execute(
      @prefix ++ [:exception],
      %{duration: duration},
      %{
        destination: destination,
        kind: :error,
        reason: sanitize_reason(error),
        stacktrace: BeamExport.Errors.redact_stacktrace(stacktrace),
        format: Keyword.get(opts, :format, :csv)
      }
    )
  end

  # Mapping errors are always redacted here, even when `include_record_in_errors: true`
  # was given: that option is for the returned/raised error, not for telemetry
  # handlers, which typically forward to logs and APM tools.
  defp sanitize_reason(%BeamExport.MappingError{} = err), do: BeamExport.MappingError.redact(err)
  defp sanitize_reason(other), do: other
end
