defmodule BeamExport.CSV.Formatter do
  @moduledoc false
  # Converts individual Elixir values into CSV field text.
  #
  # Rules (documented for users in the `BeamExport` moduledoc):
  #
  #   * nil -> "" ; booleans -> "true"/"false" ; atoms -> their name
  #   * integers -> decimal digits
  #   * floats -> the shortest text that parses back to the same float
  #     (`Float.to_string/1`, e.g. 0.1 -> "0.1", 1.0e-12 -> "1.0e-12")
  #   * Decimal -> plain notation, never scientific (Decimal.to_string(d, :normal))
  #   * Date/Time/NaiveDateTime/DateTime -> ISO 8601
  #   * other structs implementing String.Chars -> to_string/1
  #   * lists, maps, tuples, pids, references, functions, ports, structs without
  #     String.Chars and non-binary bitstrings -> BeamExport.EncodeError
  #
  # Text values (strings, atoms, String.Chars structs) are formula-sanitized when
  # `:sanitize_formulas` is true (the default).

  alias BeamExport.EncodeError

  @spec format(term(), keyword()) :: binary()
  def format(value, opts \\ [])

  def format(nil, _opts), do: ""
  def format(val, opts) when is_binary(val), do: maybe_sanitize(val, opts)
  def format(val, _opts) when is_integer(val), do: Integer.to_string(val)
  def format(val, _opts) when is_float(val), do: Float.to_string(val)
  def format(true, _opts), do: "true"
  def format(false, _opts), do: "false"
  def format(val, opts) when is_atom(val), do: maybe_sanitize(Atom.to_string(val), opts)

  def format(%Date{} = d, _opts), do: Date.to_iso8601(d)
  def format(%Time{} = t, _opts), do: Time.to_iso8601(t)
  def format(%DateTime{} = dt, _opts), do: DateTime.to_iso8601(dt)
  def format(%NaiveDateTime{} = ndt, _opts), do: NaiveDateTime.to_iso8601(ndt)

  if Code.ensure_loaded?(Decimal) do
    # Plain notation is used for every value a database can realistically return.
    # Beyond the decimal128 width (6_178 characters, the limit Decimal 3 enforces),
    # plain notation would materialize an arbitrarily long string (for example
    # 1E+1000000000), so such values fall back to exact scientific notation.
    @max_plain_width 6_178
    # Fast path without building strings: a coefficient below 10^178 has at most
    # 178 digits, and 178 + 6_000 <= 6_178.
    @fast_coef_limit Integer.pow(10, 178)

    def format(%Decimal{coef: coef, exp: exp} = dec, _opts)
        when is_integer(coef) and coef < @fast_coef_limit and exp >= -6_000 and exp <= 6_000,
        do: Decimal.to_string(dec, :normal)

    def format(%Decimal{coef: coef, exp: exp} = dec, _opts) when is_integer(coef) do
      if byte_size(Integer.to_string(coef)) + abs(exp) <= @max_plain_width do
        Decimal.to_string(dec, :normal)
      else
        Decimal.to_string(dec, :scientific)
      end
    end

    # NaN and infinities
    def format(%Decimal{} = dec, _opts), do: Decimal.to_string(dec, :normal)
  end

  def format(%module{} = struct, opts) do
    if String.Chars.impl_for(struct) do
      maybe_sanitize(to_string(struct), opts)
    else
      raise EncodeError, reason: {:unsupported_value, "struct #{inspect(module)}"}
    end
  end

  # Lists in particular must not go through String.Chars: to_string(["a", "b"])
  # silently produces "ab" and to_string([72, 105]) produces "Hi".
  def format(other, _opts) do
    raise EncodeError, reason: {:unsupported_value, BeamExport.Ecto.type_name(other)}
  end

  defp maybe_sanitize(val, opts) do
    if Keyword.get(opts, :sanitize_formulas, true), do: sanitize_formula(val), else: val
  end

  # Prevent CSV (formula) injection, following the OWASP recommendation: a cell
  # that starts with =, +, -, @, TAB or CR may be evaluated as a formula by
  # spreadsheet applications (Excel, LibreOffice Calc, Google Sheets). Leading
  # whitespace is ignored by some of them, so `"  =1+1"` is neutralized too.
  @formula_triggers [?=, ?+, ?-, ?@, ?\t, ?\r]

  @doc false
  def sanitize_formula(<<char, _rest::binary>> = val) when char in @formula_triggers,
    do: "'" <> val

  def sanitize_formula(val) when is_binary(val) do
    case String.trim_leading(val) do
      <<char, _rest::binary>> when char in @formula_triggers -> "'" <> val
      _ -> val
    end
  end
end
