defmodule BeamExport.Plug do
  @moduledoc """
  Plug and Phoenix integration for streaming and staged CSV downloads.

  ## Recommended Strategy: Staged HTTP Export (`send_staged_export/4`)
  As documented in ADR 001, directly streaming database cursors to public HTTP clients
  risks database connection pool starvation if clients download slowly.

  `send_staged_export/4` dumps the dataset into an ephemeral temporary file at full
  database speed (releasing the connection in seconds), serves the file via `Plug.Conn.send_file/5`,
  and automatically cleans up the temporary file when the request finishes or the client disconnects.

  ## Direct Chunked Export (`send_chunked_export/4`)
  For internal services or low-latency networks where connection starvation is not a factor,
  `send_chunked_export/4` streams chunks directly using chunked transfer encoding, halting
  immediately if the client socket closes.
  """

  @default_content_type "text/csv"

  @doc """
  Executes a staged file download.

  1. Streams dataset to a temporary file on disk.
  2. Closes database cursor and transaction immediately.
  3. Spawns an ephemeral cleanup monitor to guarantee temp file deletion.
  4. Sends file using `Plug.Conn.send_file/5`.

  ## Options
    * `:tmp_dir` - Directory for temporary export files. Defaults to `System.tmp_dir!()`.
    * `:content_type` - HTTP Content-Type header. Defaults to `"text/csv"`.
    * `:charset` - Content-Type charset. Defaults to `"utf-8"`.
    * All options supported by `BeamExport.to_file/3` (`:headers`, `:mapper`, `:max_rows`, etc.).
  """
  @spec send_staged_export(
          Plug.Conn.t(),
          String.t(),
          Enumerable.t() | {module(), Ecto.Queryable.t()},
          keyword()
        ) :: Plug.Conn.t()
  def send_staged_export(conn, filename, source, opts \\ []) do
    tmp_dir = Keyword.get(opts, :tmp_dir, System.tmp_dir!())
    content_type = Keyword.get(opts, :content_type, @default_content_type)
    charset = Keyword.get(opts, :charset, "utf-8")

    random_id = :crypto.strong_rand_bytes(12) |> Base.encode16(case: :lower)
    safe_filename = sanitize_filename(filename)
    tmp_path = Path.join(tmp_dir, "beam_export_#{random_id}_#{safe_filename}")

    # Ensure cleanup when request process terminates (normal, crash, or client disconnect)
    spawn_cleanup_monitor(self(), tmp_path)

    # Export directly to temp file
    case BeamExport.to_file(source, tmp_path, opts) do
      {:ok, _meta} ->
        conn
        |> Plug.Conn.put_private(:beam_export_file, tmp_path)
        |> Plug.Conn.put_resp_content_type(content_type, charset)
        |> Plug.Conn.put_resp_header(
          "content-disposition",
          ~s(attachment; filename="#{safe_filename}")
        )
        |> Plug.Conn.put_resp_header("cache-control", "no-cache, no-store, must-revalidate")
        |> Plug.Conn.send_file(200, tmp_path)

      {:error, reason} ->
        # Clean up temp file immediately if still present
        if File.exists?(tmp_path), do: File.rm(tmp_path)
        raise "BeamExport failed to generate staged export: #{inspect(reason)}"
    end
  end

  @doc """
  Streams CSV chunks directly to the HTTP client using chunked transfer encoding.

  Halts immediately and closes upstream database cursor if client disconnects (`{:error, :closed}`).

  ## Warning
  Direct streaming holds a database connection open for the entire duration of the
  download. For large datasets and public clients, prefer `send_staged_export/4`.
  """
  @spec send_chunked_export(
          Plug.Conn.t(),
          String.t(),
          Enumerable.t() | {module(), Ecto.Queryable.t()},
          keyword()
        ) :: Plug.Conn.t()
  def send_chunked_export(conn, filename, source, opts \\ []) do
    content_type = Keyword.get(opts, :content_type, @default_content_type)
    charset = Keyword.get(opts, :charset, "utf-8")
    safe_filename = sanitize_filename(filename)

    conn =
      conn
      |> Plug.Conn.put_resp_content_type(content_type, charset)
      |> Plug.Conn.put_resp_header(
        "content-disposition",
        ~s(attachment; filename="#{safe_filename}")
      )
      |> Plug.Conn.put_resp_header("cache-control", "no-cache, no-store, must-revalidate")
      |> Plug.Conn.send_chunked(200)

    stream = BeamExport.to_stream(source, opts)

    Enum.reduce_while(stream, conn, fn chunk, current_conn ->
      case Plug.Conn.chunk(current_conn, chunk) do
        {:ok, updated_conn} ->
          {:cont, updated_conn}

        {:error, :closed} ->
          # Client disconnected: halt stream, terminating upstream cursor immediately
          {:halt, current_conn}

        {:error, _other} ->
          {:halt, current_conn}
      end
    end)
  end

  defp sanitize_filename(name) do
    name
    |> Path.basename()
    |> String.replace(~r/[^\w\.\-\_]/, "_")
  end

  defp spawn_cleanup_monitor(caller_pid, file_path) do
    spawn(fn ->
      ref = Process.monitor(caller_pid)

      receive do
        {:DOWN, ^ref, :process, ^caller_pid, _reason} ->
          if File.exists?(file_path), do: File.rm(file_path)
      end
    end)
  end
end
