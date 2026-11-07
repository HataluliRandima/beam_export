defmodule BeamExport.Stream do
  @moduledoc false
  # Builds the lazy, memory-bounded CSV pipeline shared by every sink:
  #
  #   source -> records -> field lists -> encoded rows -> chunks of `:chunk_size` rows
  #
  # Records become field lists as follows:
  #
  #   * with a `:mapper`: its return value (a list, or a tuple converted to a list);
  #     anything else raises BeamExport.MappingError
  #   * lists are used as-is; tuples (e.g. Ecto `select: {u.id, u.name}`) become lists
  #   * maps and structs are read by `:headers` (atom keys, or strings naming
  #     existing atom keys); a header missing from a struct raises, and a map
  #     row matching none of the headers raises, instead of emitting blank columns
  #   * anything else raises BeamExport.EncodeError

  alias BeamExport.CSV.Encoder
  alias BeamExport.{EncodeError, Errors, MappingError}

  @default_chunk_size 500

  @doc """
  Builds a stream of `{:chunk, row_count, iodata}` tuples. `row_count` counts
  logical rows (records), so it stays exact when fields contain line breaks.
  """
  @spec build_with_counts(Enumerable.t() | {module(), Ecto.Queryable.t()}, keyword()) ::
          Enumerable.t()
  def build_with_counts(source, opts \\ []) do
    source
    |> BeamExport.Ecto.rows(opts)
    |> build_enumerable_counts_stream(opts)
  end

  @doc "Builds a lazy stream of iodata chunks."
  @spec build(Enumerable.t() | {module(), Ecto.Queryable.t()}, keyword()) :: Enumerable.t()
  def build(source, opts \\ []) do
    source
    |> build_with_counts(opts)
    |> Stream.map(fn {:chunk, _count, iodata} -> iodata end)
  end

  defp build_enumerable_counts_stream(enumerable, opts) do
    chunk_size = Keyword.get(opts, :chunk_size, @default_chunk_size)
    headers = Keyword.get(opts, :headers)
    ctx = Encoder.context(opts)

    to_fields =
      fields_fun(
        Keyword.get(opts, :mapper),
        headers,
        Keyword.get(opts, :include_record_in_errors, false)
      )

    data_stream =
      enumerable
      |> Stream.with_index(1)
      |> Stream.map(fn {record, row} -> encode_record(to_fields.(record, row), row, ctx) end)
      |> Stream.chunk_every(chunk_size)
      |> Stream.map(fn rows -> {:chunk, length(rows), rows} end)

    case headers do
      headers when headers in [nil, []] ->
        data_stream

      headers ->
        header_chunk = encode_header(headers, ctx)
        Stream.concat([{:chunk, 1, header_chunk}], data_stream)
    end
  end

  defp encode_header(headers, ctx) do
    Encoder.encode_fields(headers, ctx)
  end

  defp encode_record(fields, row, ctx) do
    Encoder.encode_fields(fields, ctx)
  rescue
    e in EncodeError -> reraise %{e | row: row}, __STACKTRACE__
  end

  ## Record -> field list

  defp fields_fun(mapper, _headers, include?) when is_function(mapper, 1) do
    fn record, row -> map_record(record, row, mapper, include?) end
  end

  defp fields_fun(nil, headers, _include?) do
    keys = resolve_header_keys(headers || [])
    fn record, row -> record_fields(record, row, headers, keys) end
  end

  defp map_record(record, row, mapper, include?) do
    result =
      try do
        mapper.(record)
      rescue
        e ->
          error =
            MappingError.exception(
              row: row,
              reason: {:exception, e.__struct__},
              detail: Errors.safe_detail(e),
              include_record: include?,
              record: record,
              original_error: e
            )

          # The stacktrace of e.g. a FunctionClauseError carries the record as arguments.
          stacktrace =
            if include?, do: __STACKTRACE__, else: Errors.redact_stacktrace(__STACKTRACE__)

          reraise error, stacktrace
      end

    case result do
      fields when is_list(fields) ->
        fields

      fields when is_tuple(fields) ->
        Tuple.to_list(fields)

      other ->
        raise MappingError,
          row: row,
          reason: {:invalid_return, BeamExport.Ecto.type_name(other)},
          include_record: include?,
          record: record
    end
  end

  defp record_fields(record, _row, _headers, _keys) when is_list(record), do: record

  defp record_fields(record, _row, _headers, _keys) when is_tuple(record),
    do: Tuple.to_list(record)

  defp record_fields(record, row, headers, _keys) when is_map(record) and headers in [nil, []] do
    raise EncodeError, reason: :map_without_headers, row: row
  end

  defp record_fields(%module{} = struct, row, _headers, keys) do
    Enum.map(keys, fn {header, key} ->
      case fetch_key(struct, header, key) do
        {:ok, value} -> value
        :error -> raise EncodeError, reason: {:unknown_header, header, module}, row: row
      end
    end)
  end

  defp record_fields(map, row, headers, keys) when is_map(map) do
    {values, matched?} =
      Enum.map_reduce(keys, false, fn {header, key}, matched? ->
        case fetch_key(map, header, key) do
          {:ok, value} -> {value, true}
          # Plain maps may be sparse: a missing key is an empty field...
          :error -> {nil, matched?}
        end
      end)

    # ...but a row matching none of the headers is a header/key mismatch.
    if matched?,
      do: values,
      else: raise(EncodeError, reason: {:no_matching_headers, headers}, row: row)
  end

  defp record_fields(other, row, _headers, _keys) do
    raise EncodeError, reason: {:invalid_row, BeamExport.Ecto.type_name(other)}, row: row
  end

  # Resolved once per export: a string header can also address the existing atom
  # key with the same name (headers never create atoms).
  defp resolve_header_keys(headers) do
    Enum.map(headers, fn
      header when is_binary(header) ->
        key =
          try do
            String.to_existing_atom(header)
          rescue
            ArgumentError -> nil
          end

        {header, key}

      header ->
        {header, nil}
    end)
  end

  defp fetch_key(map, header, key) do
    case Map.fetch(map, header) do
      {:ok, value} -> {:ok, value}
      :error when is_atom(key) and not is_nil(key) -> Map.fetch(map, key)
      :error -> :error
    end
  end
end
