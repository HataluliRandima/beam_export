if Code.ensure_loaded?(Plug.Conn) do
  defmodule BeamExport.Plug do
    @moduledoc """
    Plug and Phoenix integration for CSV downloads.

    Requires the optional `:plug` dependency.

    ## Staged export (recommended): `send_staged_export/4`

    Streaming a database cursor straight to a slow HTTP client holds a database
    connection for the whole download. `send_staged_export/4` instead writes the
    export to a private temporary file first, releases the database connection, and
    then serves the file with `Plug.Conn.send_file/3`.

    Trade-off: the response starts only after the whole export has been written, so
    very large exports keep the request waiting (and can hit proxy or load balancer
    timeouts). For those, run `BeamExport.to_file/3` in a background job instead.

    ## Chunked export: `send_chunked_export/4`

    Streams chunks with chunked transfer encoding as they are produced. The database
    connection is held until the download finishes, so use it only for trusted,
    fast clients (internal tools, service-to-service).

    ## Errors

    Both functions raise when the export fails. `send_staged_export/4` raises
    `BeamExport.ExportError` before any response is sent (resulting in a 500 in
    Phoenix). `send_chunked_export/4` has already sent the status line when a
    mid-stream error happens; the exception aborts the connection without the final
    chunk, so HTTP clients detect the download as incomplete rather than receiving a
    truncated file that looks complete.

    Error messages never contain record data unless `include_record_in_errors: true`
    is passed.
    """

    alias BeamExport.{ExportError, Options}

    @default_content_type "text/csv"

    @doc """
    Exports `source` to a private temporary file, then sends it as a download.

    The temporary file lives in a new directory (mode `0700`) under `:tmp_dir`
    and is created with mode `0600`. Both are deleted when the request process
    exits, whether it finishes normally, crashes or the client disconnects.

    ## Options

      * `:tmp_dir` - parent directory for temporary exports. Defaults to
        `System.tmp_dir!()`. Never pass user input here.
      * `:content_type` - the response content type. Defaults to `"text/csv"`, or
        `"application/gzip"` when the export is gzip-compressed (`compress: true` or
        a filename ending in `.gz`).
      * `:charset` - the content type charset. Defaults to `"utf-8"` (none for gzip).
      * every option accepted by `BeamExport.to_file/3`.

    `filename` is only used for the `content-disposition` header (sanitized,
    RFC 6266 / RFC 5987 encoded) and the temporary file name (reduced to
    `[A-Za-z0-9._-]`); it can never influence the temporary file's location.
    """
    @spec send_staged_export(
            Plug.Conn.t(),
            String.t(),
            Enumerable.t() | {module(), Ecto.Queryable.t()},
            keyword()
          ) :: Plug.Conn.t()
    def send_staged_export(conn, filename, source, opts \\ []) do
      opts = Options.validate!(opts, :send_staged_export)
      {http_opts, export_opts} = Keyword.split(opts, Options.http_keys(:send_staged_export))

      tmp_root = Keyword.get(http_opts, :tmp_dir, System.tmp_dir!())
      dir = create_private_dir!(tmp_root)
      # Removes the directory when the request process terminates for any reason.
      spawn_cleanup_monitor(self(), dir)

      tmp_path = Path.join(dir, "export_" <> sanitize_fs_filename(filename))

      case BeamExport.to_file(source, tmp_path, export_opts) do
        {:ok, meta} ->
          {content_type, charset} = content_type(http_opts, meta.compressed)

          conn
          |> Plug.Conn.put_private(:beam_export_file, tmp_path)
          |> Plug.Conn.put_resp_content_type(content_type, charset)
          |> Plug.Conn.put_resp_header(
            "content-disposition",
            build_content_disposition(filename)
          )
          |> Plug.Conn.put_resp_header("cache-control", "no-cache, no-store, must-revalidate")
          |> Plug.Conn.send_file(200, tmp_path)

        {:error, reason} ->
          File.rm_rf(dir)

          raise ExportError,
            reason: reason,
            message:
              "BeamExport failed to generate staged export: " <> ExportError.describe(reason)
      end
    end

    @doc """
    Streams CSV chunks directly to the client using chunked transfer encoding.

    Stops reading the source as soon as the client disconnects. Holds the database
    connection (for Ecto sources) for the whole download: prefer
    `send_staged_export/4` for public or slow clients.

    ## Options

      * `:content_type` - defaults to `"text/csv"`.
      * `:charset` - defaults to `"utf-8"`.
      * every option accepted by `BeamExport.to_stream/2`.
    """
    @spec send_chunked_export(
            Plug.Conn.t(),
            String.t(),
            Enumerable.t() | {module(), Ecto.Queryable.t()},
            keyword()
          ) :: Plug.Conn.t()
    def send_chunked_export(conn, filename, source, opts \\ []) do
      opts = Options.validate!(opts, :send_chunked_export)
      {http_opts, export_opts} = Keyword.split(opts, Options.http_keys(:send_chunked_export))
      content_type = Keyword.get(http_opts, :content_type, @default_content_type)
      charset = Keyword.get(http_opts, :charset, "utf-8")

      # Built (and validated) before the response starts, so invalid sources fail
      # with a regular 500 instead of a broken chunked response.
      stream = BeamExport.to_stream(source, export_opts)

      conn =
        conn
        |> Plug.Conn.put_resp_content_type(content_type, charset)
        |> Plug.Conn.put_resp_header("content-disposition", build_content_disposition(filename))
        |> Plug.Conn.put_resp_header("cache-control", "no-cache, no-store, must-revalidate")
        |> Plug.Conn.send_chunked(200)

      Enum.reduce_while(stream, conn, fn chunk, current_conn ->
        case Plug.Conn.chunk(current_conn, chunk) do
          {:ok, updated_conn} -> {:cont, updated_conn}
          # Client disconnected: halting the stream closes the database cursor.
          {:error, _reason} -> {:halt, current_conn}
        end
      end)
    end

    defp content_type(http_opts, compressed?) do
      case Keyword.fetch(http_opts, :content_type) do
        {:ok, type} ->
          {type, Keyword.get(http_opts, :charset, "utf-8")}

        :error when compressed? ->
          {"application/gzip", Keyword.get(http_opts, :charset)}

        :error ->
          {@default_content_type, Keyword.get(http_opts, :charset, "utf-8")}
      end
    end

    defp create_private_dir!(root) do
      dir =
        Path.join(
          root,
          "beam_export_" <> Base.encode16(:crypto.strong_rand_bytes(12), case: :lower)
        )

      # `File.mkdir/1` fails if the path exists, so the directory is always new and
      # ours. It is restricted to the owner before any file is created inside it.
      with :ok <- File.mkdir_p(root),
           :ok <- File.mkdir(dir),
           :ok <- File.chmod(dir, 0o700) do
        dir
      else
        {:error, reason} ->
          raise ExportError,
            reason: %File.Error{reason: reason, action: "create temporary directory", path: dir},
            message:
              "BeamExport could not create a private temporary directory under " <>
                "#{inspect(root)}: #{:file.format_error(reason)}"
      end
    end

    # Builds standard RFC 6266 / RFC 5987 Content-Disposition header.
    # For pure ASCII names, outputs `attachment; filename="name.csv"`.
    # For Unicode names, provides ASCII fallback and UTF-8 encoded parameter:
    # `attachment; filename="fallback.csv"; filename*=UTF-8''encoded.csv`
    defp build_content_disposition(raw_filename) do
      clean_name =
        raw_filename
        |> to_string()
        |> String.replace(~r/[\r\n\0"]/, "")
        |> Path.basename()

      if clean_name =~ ~r/^[a-zA-Z0-9\.\-_]+$/ do
        ~s(attachment; filename="#{clean_name}")
      else
        ascii_fallback = String.replace(clean_name, ~r/[^a-zA-Z0-9\.\-_]/u, "_")
        encoded_utf8 = URI.encode(clean_name, &URI.char_unreserved?/1)
        ~s(attachment; filename="#{ascii_fallback}"; filename*=UTF-8''#{encoded_utf8})
      end
    end

    # Generates an ASCII-safe filename for local filesystem storage. The caller
    # prefixes it, so it can never be "", "." or "..".
    defp sanitize_fs_filename(raw_filename) do
      raw_filename
      |> to_string()
      |> String.replace(~r/[\r\n\0]/, "")
      |> Path.basename()
      |> String.replace(~r/[^a-zA-Z0-9\.\-_]/u, "_")
    end

    defp spawn_cleanup_monitor(caller_pid, dir) do
      spawn(fn ->
        ref = Process.monitor(caller_pid)

        receive do
          {:DOWN, ^ref, :process, ^caller_pid, _reason} -> File.rm_rf(dir)
        end
      end)
    end
  end
end
