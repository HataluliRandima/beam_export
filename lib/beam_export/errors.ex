defmodule BeamExport.MappingError do
  @moduledoc """
  Raised (or returned) when the `:mapper` function fails for a record.

  **Record contents are redacted by default.** Exceptions end up in logs, error
  trackers and telemetry handlers, so BeamExport never puts row data there unless
  you opt in with `include_record_in_errors: true` (for example in development).

  Fields:

    * `:row` - the 1-based position of the failing record in the source
      (the header row is not counted).
    * `:reason` - `{:exception, module}` when the mapper raised, or
      `{:invalid_return, type}` when it returned something other than a list or tuple.
    * `:record` - the record, or `:redacted`.
    * `:detail` - a short description of the failure that is safe to log (for
      example `key :email not found`), or `nil`.
    * `:original_error` - the exception the mapper raised, or `:redacted`.
  """
  defexception [:message, :row, :reason, :detail, record: :redacted, original_error: :redacted]

  @impl true
  def exception(opts) do
    include? = Keyword.get(opts, :include_record, false)
    row = Keyword.get(opts, :row)
    reason = Keyword.get(opts, :reason)
    record = if include?, do: Keyword.get(opts, :record), else: :redacted
    original = if include?, do: Keyword.get(opts, :original_error), else: :redacted
    detail = Keyword.get(opts, :detail)

    %__MODULE__{
      row: row,
      reason: reason,
      detail: detail,
      record: record,
      original_error: original,
      message: build_message(row, reason, detail, record, original)
    }
  end

  @doc false
  # Returns a copy without the record or original error, whatever was configured.
  def redact(%__MODULE__{} = error) do
    %{
      error
      | record: :redacted,
        original_error: :redacted,
        message: build_message(error.row, error.reason, error.detail, :redacted, :redacted)
    }
  end

  defp build_message(row, reason, detail, record, original) do
    base =
      case reason do
        {:invalid_return, type} ->
          "the :mapper returned a #{type} for data row #{row}, but it must return a " <>
            "list (or tuple) of field values"

        {:exception, module} ->
          "the :mapper raised #{inspect(module)} while mapping data row #{row}" <>
            if(detail, do: " (#{detail})", else: "")
      end

    if record == :redacted do
      base <>
        ". The record and original error are redacted; pass " <>
        "`include_record_in_errors: true` to include them"
    else
      original_message =
        if is_exception(original), do: "\n" <> Exception.message(original), else: ""

      base <> original_message <> "\nRecord: " <> inspect(record)
    end
  end
end

defmodule BeamExport.PreloadError do
  @moduledoc """
  Raised (or returned) when an exported Ecto query contains preloads.

  Database cursors (`Repo.stream/2`) cannot preload associations. Load the data with
  joins and `select`, or a `:mapper` over joined fields, instead. The query is checked
  before anything is written.
  """
  defexception [:message]

  @impl true
  def exception(opts) do
    msg =
      Keyword.get(
        opts,
        :message,
        "Preloaded associations are not supported in Ecto streaming. " <>
          "Please load associations using database joins or select fields directly."
      )

    %__MODULE__{message: msg}
  end
end

defmodule BeamExport.EncodeError do
  @moduledoc """
  Raised (or returned) when a record or value cannot be turned into CSV without
  losing or inventing data.

  BeamExport refuses to guess: lists, maps, tuples and other values without an
  obvious text form must be converted explicitly in your `:mapper`, and headers
  must match the records they extract from. The message never includes record
  values, only types, positions and header names.

  Fields:

    * `:reason` - one of:
      * `{:unsupported_value, type}` - a field value has no CSV representation.
      * `{:invalid_row, type}` - a record is not a list, tuple, map or struct.
      * `:map_without_headers` - a map/struct record without `:headers` or `:mapper`.
      * `{:unknown_header, header, struct_module}` - a header is not a field of the struct.
      * `{:no_matching_headers, headers}` - none of the headers is a key of the map.
    * `:row` - 1-based data row (header row not counted), or `nil` for the header row.
    * `:column` - 1-based column, when known.
  """
  defexception [:reason, :row, :column]

  @impl true
  def message(%__MODULE__{reason: reason, row: row, column: column}) do
    describe(reason, row, column)
  end

  defp describe({:unsupported_value, type}, row, column) do
    "cannot encode a #{type} value#{location(row, column)} as a CSV field. " <> hint(type)
  end

  defp describe({:invalid_row, type}, row, _column) do
    "data row #{row} is a #{type}. Each record must be a list or tuple of field " <>
      "values, or a map/struct combined with :headers. Pass a :mapper to convert records"
  end

  defp describe(:map_without_headers, row, _column) do
    "data row #{row} is a map or struct, but neither :headers nor :mapper was given, " <>
      "so the column order is unknown. Pass :headers with the keys to export " <>
      "(for example headers: [:id, :email]) or a :mapper returning a list"
  end

  defp describe({:unknown_header, header, module}, row, _column) do
    "header #{inspect(header)} is not a field of #{inspect(module)} (data row #{row}). " <>
      "Without a :mapper, headers must be the struct's field names (for example :id " <>
      "or \"id\"). To use human-readable column titles, pass a :mapper"
  end

  defp describe({:no_matching_headers, headers}, row, _column) do
    "none of the headers #{inspect(headers)} is a key of the map in data row #{row}, " <>
      "so every column would be blank. Use the map's keys as headers, or pass a :mapper"
  end

  defp location(nil, nil), do: ""
  defp location(nil, column), do: " in column #{column} of the header row"
  defp location(row, nil), do: " in data row #{row}"
  defp location(row, column), do: " in column #{column} of data row #{row}"

  defp hint("list"),
    do: "Convert it in your :mapper, for example with Enum.join(list, \";\")"

  defp hint("map"),
    do: "Convert it in your :mapper, for example with a JSON encoder"

  defp hint(_type), do: "Convert it to a string in your :mapper"
end

defmodule BeamExport.Errors do
  @moduledoc false
  # Helpers that keep row data out of exceptions, stacktraces and telemetry.

  @doc """
  Replaces argument lists in stacktrace entries with their arity.

  A `FunctionClauseError` raised by a mapper puts the record in the top stack
  frame's arguments; crash logs print those arguments.
  """
  def redact_stacktrace(stacktrace) when is_list(stacktrace) do
    Enum.map(stacktrace, fn
      {mod, fun, args, location} when is_list(args) -> {mod, fun, length(args), location}
      entry -> entry
    end)
  end

  def redact_stacktrace(other), do: other

  @doc """
  A short description of an exception that is safe to log: it is derived from
  fields that hold code-level information (keys, function names, types), never
  from the data being exported.
  """
  def safe_detail(%KeyError{key: key}) when is_atom(key), do: "key #{inspect(key)} not found"

  def safe_detail(%FunctionClauseError{module: m, function: f, arity: a})
      when is_atom(m) and is_atom(f),
      do: "no function clause matching in #{inspect(m)}.#{f}/#{a}"

  def safe_detail(%UndefinedFunctionError{module: m, function: f, arity: a})
      when is_atom(m) and is_atom(f),
      do: "function #{inspect(m)}.#{f}/#{a} is undefined"

  def safe_detail(%Protocol.UndefinedError{protocol: protocol, value: value}),
    do: "protocol #{inspect(protocol)} not implemented for #{BeamExport.Ecto.type_name(value)}"

  def safe_detail(%ArithmeticError{}), do: "bad argument in arithmetic expression"
  def safe_detail(_exception), do: nil
end

defmodule BeamExport.TimeoutError do
  @moduledoc """
  Raised (or returned) when an Ecto-backed export stops making progress.

  An export that fails with this error is **incomplete**: BeamExport aborts the
  database cursor and never reports a partial export as a successful one.

  Fields:

    * `:reason` - `:consumer_timeout` when the stream consumer did not ask for the
      next batch within `:consumer_timeout` milliseconds, or `:batch_timeout` when the
      database did not deliver the next batch within `:batch_timeout` milliseconds.
    * `:timeout` - the timeout that elapsed, in milliseconds.
  """
  defexception [:reason, :timeout]

  @impl true
  def message(%{reason: :consumer_timeout, timeout: timeout}) do
    "the export stream was not consumed for #{timeout}ms, so BeamExport aborted the " <>
      "database cursor and released its connection. The export is INCOMPLETE. " <>
      "Consume the stream faster, raise the :consumer_timeout option, or use " <>
      "BeamExport.to_file/3 to stage the export on disk first"
  end

  def message(%{reason: :batch_timeout, timeout: timeout}) do
    "the database did not return the next batch of rows within #{timeout}ms, so " <>
      "BeamExport aborted the export. The export is INCOMPLETE. Raise the " <>
      ":batch_timeout option if your query legitimately needs longer per batch"
  end
end

defmodule BeamExport.ExportError do
  @moduledoc """
  Raised or returned when an export fails for a reason that is not described by a
  more specific BeamExport exception.

  `:reason` holds the underlying cause: an exception or a term.
  """
  defexception [:message, :reason]

  @impl true
  def exception(opts) do
    reason = Keyword.get(opts, :reason)
    message = Keyword.get_lazy(opts, :message, fn -> "export failed: #{describe(reason)}" end)
    %__MODULE__{message: message, reason: reason}
  end

  @doc false
  def describe(reason) when is_exception(reason), do: Exception.message(reason)
  def describe(reason), do: inspect(reason)
end
