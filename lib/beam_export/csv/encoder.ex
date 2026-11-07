defmodule BeamExport.CSV.Encoder do
  @moduledoc false
  # RFC 4180 CSV encoder emitting iodata. Options are validated by
  # `BeamExport.Options` at the public entry points.

  alias BeamExport.CSV.Formatter
  alias BeamExport.EncodeError

  @default_separator ","
  @default_line_terminator "\r\n"

  @doc "Encodes a single row (list of terms) into iodata, including the line terminator."
  @spec encode_row(list(), keyword()) :: iodata()
  def encode_row(row, opts \\ []) when is_list(row), do: encode_fields(row, context(opts))

  @doc "Encodes a batch of rows into iodata."
  @spec encode_chunk([list()], keyword()) :: iodata()
  def encode_chunk(rows, opts \\ []) when is_list(rows) do
    ctx = context(opts)
    Enum.map(rows, &encode_fields(&1, ctx))
  end

  @doc "Encodes a single field according to RFC 4180 rules."
  @spec encode_field(term(), keyword(), byte() | nil) :: iodata()
  def encode_field(val, opts, sep_byte \\ nil) do
    sep = sep_byte || binary_first_byte(Keyword.get(opts, :separator, @default_separator))
    escape_field(Formatter.format(val, opts), sep)
  end

  @doc false
  # Precomputes everything that does not change between rows.
  def context(opts) do
    separator = Keyword.get(opts, :separator, @default_separator)

    %{
      separator: separator,
      sep_byte: binary_first_byte(separator),
      line_terminator: Keyword.get(opts, :line_terminator, @default_line_terminator),
      format_opts: [sanitize_formulas: Keyword.get(opts, :sanitize_formulas, true)]
    }
  end

  @doc false
  # Encodes one row. On failure, raises BeamExport.EncodeError with the column set.
  def encode_fields(row, ctx) do
    [join_fields(row, ctx), ctx.line_terminator]
  rescue
    e in EncodeError ->
      reraise %{e | column: e.column || failing_column(row, ctx)}, __STACKTRACE__
  end

  defp join_fields([], _ctx), do: []
  defp join_fields([value], ctx), do: [encode_value(value, ctx)]

  defp join_fields([value | rest], ctx),
    do: [encode_value(value, ctx), ctx.separator | join_fields(rest, ctx)]

  defp encode_value(value, ctx),
    do: escape_field(Formatter.format(value, ctx.format_opts), ctx.sep_byte)

  # Only runs on the error path, so the happy path carries no column bookkeeping.
  defp failing_column(row, ctx) do
    Enum.find_index(row, fn value ->
      try do
        Formatter.format(value, ctx.format_opts)
        false
      rescue
        EncodeError -> true
      end
    end)
    |> case do
      nil -> nil
      index -> index + 1
    end
  end

  # A field is quoted when it contains a double quote, CR, LF or the first byte of
  # the separator. Checking only the first byte of a multi-byte separator can only
  # over-quote, which is still valid CSV.
  defp escape_field(bin, sep_byte) when is_binary(bin) do
    if needs_escaping?(bin, sep_byte) do
      ["\"", :binary.replace(bin, "\"", "\"\"", [:global]), "\""]
    else
      bin
    end
  end

  defp needs_escaping?(<<>>, _sep), do: false

  defp needs_escaping?(<<byte, rest::binary>>, sep_byte) do
    if byte == ?" or byte == ?\r or byte == ?\n or byte == sep_byte do
      true
    else
      needs_escaping?(rest, sep_byte)
    end
  end

  defp binary_first_byte(<<byte, _::binary>>), do: byte
  defp binary_first_byte(_), do: ?,
end
