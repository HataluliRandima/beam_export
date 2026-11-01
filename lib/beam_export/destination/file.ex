defmodule BeamExport.Destination.File do
  @moduledoc """
  Streams export data directly into files with buffered I/O, optional streaming
  Gzip compression, atomic file writing, and telemetry instrumentation.
  """

  alias BeamExport.Stream, as: ExportStream
  alias BeamExport.Telemetry

  @doc """
  Streams data from `source` directly into a file at `target_path`.

  ## Options
    * `:atomic` - when `true`, streams into a temporary file in the target directory
      and atomically renames it to `target_path` upon successful completion.
      Defaults to `false`.
    * `:compress` - when `:gzip` or `true`, streams through Gzip compression on-the-fly.
      Automatically enabled if `target_path` ends with `.gz`. Defaults to `false`.
    * `:headers` - list of headers to prepend.
    * `:mapper` - mapper function.
    * `:separator` - CSV delimiter. Defaults to `","`.
    * `:line_terminator` - line endings. Defaults to `"\\r\\n"`.
    * `:sanitize_formulas` - boolean to sanitize Excel formulas. Defaults to `false`.
    * `:max_rows` - Ecto batch size. Defaults to `500`.
    * `:timeout` - Ecto transaction timeout. Defaults to `:infinity`.

  Returns `{:ok, %{path: String.t(), rows: non_neg_integer(), bytes: non_neg_integer(), duration: integer()}}`
  or `{:error, term()}`.
  """
  @spec write(Enumerable.t() | {module(), Ecto.Queryable.t()}, Path.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def write(source, target_path, opts \\ []) do
    start_time = System.monotonic_time()
    Telemetry.start(:file, opts)

    target_dir = Path.dirname(target_path)
    atomic? = Keyword.get(opts, :atomic, false)
    compress? = should_compress?(target_path, opts)

    write_path =
      if atomic? do
        random_suffix = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
        Path.join(target_dir, ".tmp_#{Path.basename(target_path)}_#{random_suffix}")
      else
        target_path
      end

    try do
      File.mkdir_p!(target_dir)

      # Build the lazy stream
      stream = ExportStream.build(source, opts)

      # Open Erlang file port with delayed_write and optional native gzip compression
      open_modes =
        if compress? do
          [:write, :compressed, :delayed_write]
        else
          [:write, :delayed_write]
        end

      {:ok, file} = :file.open(String.to_charlist(write_path), open_modes)

      # Single-pass execution: write chunks, compute row counts in-memory without secondary disk I/O
      {total_rows, total_bytes} =
        try do
          Enum.reduce(stream, {0, 0}, fn chunk, {rows_acc, bytes_acc} ->
            :ok = :file.write(file, chunk)
            bytes = IO.iodata_length(chunk)
            lines = count_newlines(chunk, 0)
            Telemetry.chunk(:file, lines, bytes, opts)
            {rows_acc + lines, bytes_acc + bytes}
          end)
        after
          :file.close(file)
        end

      # If atomic writing was requested, atomically rename the temp file to the final path
      if atomic? do
        File.rename!(write_path, target_path)
      end

      # For compressed files, the on-disk byte size will be smaller than the uncompressed stream bytes
      final_bytes =
        if compress? and File.exists?(target_path) do
          File.stat!(target_path).size
        else
          total_bytes
        end

      meta = %{
        path: target_path,
        rows: total_rows,
        bytes: final_bytes,
        compressed: compress?,
        duration: System.monotonic_time() - start_time
      }

      Telemetry.stop(:file, meta, opts)
      {:ok, meta}
    rescue
      e in [File.Error, BeamExport.MappingError, BeamExport.PreloadError] ->
        cleanup_temp_file(write_path, atomic?)
        Telemetry.exception(:file, start_time, e, __STACKTRACE__, opts)
        {:error, e}

      e ->
        cleanup_temp_file(write_path, atomic?)
        Telemetry.exception(:file, start_time, e, __STACKTRACE__, opts)
        {:error, e}
    end
  end

  defp should_compress?(path, opts) do
    case Keyword.get(opts, :compress) do
      :gzip -> true
      true -> true
      false -> false
      nil -> String.ends_with?(path, ".gz")
    end
  end

  defp cleanup_temp_file(path, true) do
    if File.exists?(path), do: File.rm(path)
  end

  defp cleanup_temp_file(_path, _atomic), do: :ok

  # Fast newline counter across nested iodata in L1/L2 cache, avoiding secondary disk reads
  defp count_newlines([], acc), do: acc
  defp count_newlines([head | tail], acc), do: count_newlines(tail, count_newlines(head, acc))

  defp count_newlines(bin, acc) when is_binary(bin) do
    acc + length(:binary.matches(bin, "\n"))
  end

  defp count_newlines(_other, acc), do: acc
end
