defmodule BeamExport.Telemetry do
  @moduledoc """
  Telemetry integration for `beam_export`.

  Emits the following telemetry events:
  * `[:beam_export, :export, :start]` - fired at the beginning of an export.
    * Measurements: `%{system_time: integer()}`
    * Metadata: `%{destination: atom(), format: :csv}`
  * `[:beam_export, :export, :chunk]` - fired when a chunk of rows is encoded/written.
    * Measurements: `%{rows: integer(), bytes: integer()}`
    * Metadata: `%{destination: atom()}`
  * `[:beam_export, :export, :stop]` - fired upon successful completion.
    * Measurements: `%{duration: integer(), rows: integer(), bytes: integer()}`
    * Metadata: `%{destination: atom(), format: :csv}`
  * `[:beam_export, :export, :exception]` - fired when an unhandled error terminates the export.
    * Measurements: `%{duration: integer()}`
    * Metadata: `%{destination: atom(), kind: atom(), reason: term(), stacktrace: list(), format: :csv}`
  """

  @prefix [:beam_export, :export]

  @doc "Emits the start event."
  def start(destination, opts \\ []) do
    :telemetry.execute(
      @prefix ++ [:start],
      %{system_time: System.system_time()},
      %{
        destination: destination,
        format: Keyword.get(opts, :format, :csv)
      }
    )
  end

  @doc "Emits chunk measurement."
  def chunk(destination, rows, bytes, _opts \\ []) do
    :telemetry.execute(
      @prefix ++ [:chunk],
      %{rows: rows, bytes: bytes},
      %{destination: destination}
    )
  end

  @doc "Emits stop event with total metrics."
  def stop(destination, %{duration: duration, rows: rows, bytes: bytes}, opts \\ []) do
    :telemetry.execute(
      @prefix ++ [:stop],
      %{duration: duration, rows: rows, bytes: bytes},
      %{
        destination: destination,
        format: Keyword.get(opts, :format, :csv)
      }
    )
  end

  @doc "Emits exception event with sensitive row values redacted."
  def exception(destination, start_time, error, stacktrace, opts \\ []) do
    duration = System.monotonic_time() - start_time

    :telemetry.execute(
      @prefix ++ [:exception],
      %{duration: duration},
      %{
        destination: destination,
        kind: :error,
        reason: sanitize_reason(error),
        stacktrace: stacktrace,
        format: Keyword.get(opts, :format, :csv)
      }
    )
  end

  # Redact record content in MappingError to ensure PII and sensitive data
  # never leak into telemetry handlers, loggers, or APM monitoring tools.
  defp sanitize_reason(%BeamExport.MappingError{} = err) do
    %BeamExport.MappingError{
      message: "Failed to map record: #{inspect(err.original_error)}",
      original_error: err.original_error,
      record: :redacted
    }
  end

  defp sanitize_reason(other), do: other
end
