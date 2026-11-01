defmodule BeamExport.Stream do
  @moduledoc """
  Constructs a lazy, memory-bounded stream pipeline for CSV export.
  """

  alias BeamExport.CSV.Encoder
  alias BeamExport.MappingError

  @default_chunk_size 500

  @doc """
  Builds a lazy `Stream` of `iodata` chunks from any enumerable or Ecto source.

  ## Options
    * `:headers` - list of column headers (strings or atoms) to prepend as the first row.
    * `:mapper` - a function `(term -> [term])` mapping each record to a row list.
      Defaults to identity.
    * `:chunk_size` - number of rows to batch per encoded chunk. Defaults to `500`.
    * `:separator` - CSV delimiter string. Defaults to `","`.
    * `:line_terminator` - line ending string. Defaults to `"\\r\\n"`.
    * `:sanitize_formulas` - boolean to prevent CSV formula injection. Defaults to `false`.
    * `:max_rows` - Ecto batch size when source is `{Repo, query}`. Defaults to `500`.
    * `:timeout` - Ecto transaction timeout. Defaults to `:infinity`.
  """
  @spec build(Enumerable.t() | {module(), Ecto.Queryable.t()}, keyword()) :: Enumerable.t()
  def build(source, opts \\ [])

  # When source is {Repo, query}, delegate to Ecto stream
  def build({repo, query}, opts) when is_atom(repo) do
    BeamExport.Ecto.stream(repo, query, opts)
    |> build_enumerable_stream(opts)
  end

  def build(enumerable, opts) do
    build_enumerable_stream(enumerable, opts)
  end

  defp build_enumerable_stream(enumerable, opts) do
    chunk_size = Keyword.get(opts, :chunk_size, @default_chunk_size)
    headers = Keyword.get(opts, :headers)
    mapper = Keyword.get(opts, :mapper)

    data_stream =
      enumerable
      |> Stream.map(&safe_map(&1, mapper, headers))
      |> Stream.chunk_every(chunk_size)
      |> Stream.map(&Encoder.encode_chunk(&1, opts))

    case headers do
      nil ->
        data_stream

      [] ->
        data_stream

      headers when is_list(headers) ->
        header_chunk = Encoder.encode_row(headers, opts)
        Stream.concat([[header_chunk]], data_stream)
    end
  end

  defp safe_map(record, nil, headers)
       when is_map(record) and is_list(headers) and headers != [] do
    # Convenience: if record is a map and headers are provided, try extracting values by key
    extract_from_map(record, headers)
  end

  defp safe_map(record, nil, _headers), do: record

  defp safe_map(record, mapper, _headers) when is_function(mapper, 1) do
    mapper.(record)
  rescue
    e ->
      reraise MappingError, [record: record, original_error: e], __STACKTRACE__
  end

  defp extract_from_map(map, headers) do
    Enum.map(headers, fn header ->
      case Map.fetch(map, header) do
        {:ok, val} ->
          val

        :error ->
          # If header is a string, check if atom exists
          atom_key =
            if is_binary(header) do
              try do
                String.to_existing_atom(header)
              rescue
                ArgumentError -> nil
              end
            end

          if atom_key, do: Map.get(map, atom_key), else: nil
      end
    end)
  end
end
