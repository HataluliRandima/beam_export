defmodule BeamExport.CSV.Encoder do
  @moduledoc """
  High-performance, RFC 4180-compliant CSV encoder emitting `iodata`.
  """

  alias BeamExport.CSV.Formatter

  @default_separator ","
  @default_line_terminator "\r\n"

  @doc """
  Encodes a single row (list of terms) into `iodata`, terminated with `line_terminator`.
  """
  @spec encode_row(list(), keyword()) :: iodata()
  def encode_row(row, opts \\ []) when is_list(row) do
    separator = Keyword.get(opts, :separator, @default_separator)
    line_terminator = Keyword.get(opts, :line_terminator, @default_line_terminator)
    sep_byte = binary_first_byte(separator)

    fields_iodata =
      row
      |> Enum.map(&encode_field(&1, opts, sep_byte))
      |> join_iodata(separator)

    [fields_iodata, line_terminator]
  end

  @doc """
  Encodes a batch/chunk of rows into `iodata`.
  """
  @spec encode_chunk([list()], keyword()) :: iodata()
  def encode_chunk(rows, opts \\ []) when is_list(rows) do
    separator = Keyword.get(opts, :separator, @default_separator)
    line_terminator = Keyword.get(opts, :line_terminator, @default_line_terminator)
    sep_byte = binary_first_byte(separator)

    Enum.map(rows, fn row ->
      fields_iodata =
        row
        |> Enum.map(&encode_field(&1, opts, sep_byte))
        |> join_iodata(separator)

      [fields_iodata, line_terminator]
    end)
  end

  @doc """
  Encodes a single field according to RFC 4180 rules.
  """
  @spec encode_field(term(), keyword(), byte() | nil) :: iodata()
  def encode_field(val, opts, sep_byte \\ nil) do
    formatted = Formatter.format(val, opts)
    sep = sep_byte || binary_first_byte(Keyword.get(opts, :separator, @default_separator))
    escape_field(formatted, sep)
  end

  # Check if field contains quotes, separator, CR, or LF.
  defp escape_field(bin, sep_byte) when is_binary(bin) do
    if needs_escaping?(bin, sep_byte) do
      ["\"", escape_quotes(bin), "\""]
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

  defp escape_quotes(bin) do
    :binary.replace(bin, "\"", "\"\"", [:global])
  end

  defp join_iodata([], _sep), do: []

  defp join_iodata([first | rest], sep) do
    [first | Enum.map(rest, &[sep, &1])]
  end

  defp binary_first_byte(<<byte, _::binary>>), do: byte
  defp binary_first_byte(_), do: ?,
end
