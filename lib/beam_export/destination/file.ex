defmodule BeamExport.Destination.File do
  @moduledoc false
  # File sink used by `BeamExport.to_file/3`. Options are documented there.
  #
  # Integrity rules:
  #
  #   * Options and the source are validated before any filesystem side effect.
  #   * Errors reported when the file is closed (buffered writes, gzip trailer) or
  #     synced fail the export. Atomic exports are synced to disk before the rename.
  #   * Cleanup only ever removes a regular file that this call created or truncated.
  #     A pre-existing target is never touched by a failure that happens before it
  #     was opened.
  #   * New files are created with mode `:file_mode` (default `0o600`) before any row
  #     is written.

  alias BeamExport.Stream, as: ExportStream
  alias BeamExport.{Options, Telemetry}

  @spec write(Enumerable.t() | {module(), Ecto.Queryable.t()}, Path.t(), keyword()) ::
          {:ok, map()} | {:error, Exception.t()}
  def write(source, target_path, opts) do
    opts = Options.validate!(opts, :to_file)
    # Invalid sources are programmer errors: raise, like invalid options.
    _ = BeamExport.Ecto.ecto_source?(source)
    target_path = to_string(target_path)
    start_time = System.monotonic_time()
    Telemetry.start(:file, opts)

    result =
      try do
        BeamExport.Ecto.run(source, opts, fn -> do_write(source, target_path, opts) end)
      rescue
        e -> {:error, e, __STACKTRACE__}
      end

    case result do
      {:ok, meta} ->
        meta = Map.put(meta, :duration, System.monotonic_time() - start_time)
        Telemetry.stop(:file, meta, opts)
        {:ok, meta}

      {:error, exception, stacktrace} ->
        Telemetry.exception(:file, start_time, exception, stacktrace, opts)
        {:error, exception}
    end
  end

  defp do_write(source, target_path, opts) do
    atomic? = Keyword.fetch!(opts, :atomic)
    compress? = compress?(target_path, opts)
    write_path = if atomic?, do: temp_path(target_path), else: target_path

    # Building the stream validates the source (for example raising
    # BeamExport.PreloadError) before anything touches the filesystem.
    with {:ok, stream} <- build_stream(source, opts),
         :ok <- ensure_dir(Path.dirname(target_path)),
         {:ok, file, created?} <- open(write_path, atomic?, compress?) do
      # From here on `write_path` was created or truncated by this call.
      closed_result =
        try do
          written_result =
            with :ok <- set_mode(write_path, created?, opts) do
              write_all(file, stream, write_path, opts)
            end

          # Always close; a close error (for example ENOSPC from buffered writes)
          # fails an otherwise successful export.
          close(file, write_path, written_result)
        catch
          # Exits and throws from the source are re-raised, not converted into
          # error tuples, but the handle is closed and the partial file removed.
          kind, reason ->
            :file.close(file)
            remove_partial(write_path)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      with {:ok, rows, bytes} <- closed_result,
           :ok <- if(atomic?, do: sync(write_path), else: :ok),
           :ok <- if(atomic?, do: rename(write_path, target_path), else: :ok) do
        {:ok,
         %{
           path: target_path,
           rows: rows,
           bytes: final_bytes(target_path, compress?, bytes),
           compressed: compress?
         }}
      else
        {:error, _exception, _stacktrace} = error ->
          remove_partial(write_path)
          error
      end
    end
  end

  defp build_stream(source, opts) do
    {:ok, ExportStream.build_with_counts(source, opts)}
  rescue
    e -> {:error, e, __STACKTRACE__}
  end

  defp ensure_dir(dir) do
    case File.mkdir_p(dir) do
      :ok -> :ok
      {:error, reason} -> file_error(reason, "create directory", dir)
    end
  end

  defp open(path, atomic?, compress?) do
    modes = [:write, :delayed_write] ++ if(compress?, do: [:compressed], else: [])

    if atomic? do
      # `:exclusive` guarantees the temp file is new: it cannot follow a planted
      # symlink or reuse an existing file.
      case :file.open(String.to_charlist(path), [:exclusive | modes]) do
        {:ok, file} -> {:ok, file, true}
        {:error, reason} -> file_error(reason, "create temporary file", path)
      end
    else
      existed? = File.exists?(path)

      case :file.open(String.to_charlist(path), modes) do
        {:ok, file} -> {:ok, file, not existed?}
        {:error, reason} -> file_error(reason, "open", path)
      end
    end
  end

  # Only files created by this call get `:file_mode`; an existing file written in
  # place (`atomic: false`) keeps its permissions.
  defp set_mode(_path, false, _opts), do: :ok

  defp set_mode(path, true, opts) do
    case :file.change_mode(String.to_charlist(path), Keyword.fetch!(opts, :file_mode)) do
      :ok -> :ok
      {:error, reason} -> file_error(reason, "set permissions on", path)
    end
  end

  defp write_all(file, stream, path, opts) do
    Enum.reduce_while(stream, {:ok, 0, 0}, fn {:chunk, rows, chunk}, {:ok, rows_acc, bytes_acc} ->
      case :file.write(file, chunk) do
        :ok ->
          bytes = IO.iodata_length(chunk)
          Telemetry.chunk(:file, rows, bytes, opts)
          {:cont, {:ok, rows_acc + rows, bytes_acc + bytes}}

        {:error, reason} ->
          {:halt, file_error(reason, "write to", path)}
      end
    end)
  rescue
    e -> {:error, e, __STACKTRACE__}
  end

  defp close(file, path, written_result) do
    case {:file.close(file), written_result} do
      {:ok, result} -> result
      # The write already failed; that error is the more precise one.
      {{:error, _}, {:error, _, _} = error} -> error
      {{:error, reason}, _ok} -> file_error(reason, "write (on close) to", path)
    end
  end

  # Flushes the file's data to stable storage so that the atomic rename can never
  # expose a file whose contents are not yet on disk.
  defp sync(path) do
    with {:ok, file} <- :file.open(String.to_charlist(path), [:read, :write, :raw, :binary]),
         result = :file.datasync(file),
         :ok <- :file.close(file) do
      case result do
        :ok -> :ok
        # Filesystems that cannot sync (e.g. some network mounts) are not an error.
        {:error, reason} when reason in [:enotsup, :einval] -> :ok
        {:error, reason} -> file_error(reason, "sync", path)
      end
    else
      {:error, reason} -> file_error(reason, "sync", path)
    end
  end

  defp rename(from, to) do
    case :file.rename(String.to_charlist(from), String.to_charlist(to)) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error,
         %File.RenameError{reason: reason, action: "rename", source: from, destination: to}, []}
    end
  end

  defp final_bytes(target_path, true, bytes) do
    case File.stat(target_path) do
      {:ok, %{size: size}} -> size
      {:error, _} -> bytes
    end
  end

  defp final_bytes(_target_path, false, bytes), do: bytes

  # Removes a partially written file. Only regular files are ever deleted, so a
  # device or directory path (for example /dev/full) can never be removed.
  defp remove_partial(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular}} -> File.rm(path)
      _ -> :ok
    end
  end

  defp temp_path(target_path) do
    random_suffix = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    Path.join(Path.dirname(target_path), ".tmp_#{Path.basename(target_path)}_#{random_suffix}")
  end

  defp compress?(path, opts) do
    case Keyword.get(opts, :compress) do
      nil -> String.ends_with?(path, ".gz")
      false -> false
      _true_or_gzip -> true
    end
  end

  defp file_error(reason, action, path) do
    {:error, %File.Error{reason: reason, action: action, path: path}, []}
  end
end
