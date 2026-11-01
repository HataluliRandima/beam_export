defmodule BeamExport.CSV.Formatter do
  @moduledoc """
  Handles serialization and sanitization of Elixir data types into CSV-compatible values.
  """

  @doc """
  Formats an individual field into a binary string representation.

  ## Options
    * `:sanitize_formulas` - when `true`, prepends a single quote (`'`) to strings
      starting with `=`, `+`, `-`, `@`, `\\t`, or `\\r` to prevent CSV formula injection.
      Defaults to `false`.
  """
  @spec format(term(), keyword()) :: binary()
  def format(value, opts \\ [])

  def format(nil, _opts), do: ""

  def format(val, opts) when is_binary(val) do
    if Keyword.get(opts, :sanitize_formulas, false) do
      sanitize_formula(val)
    else
      val
    end
  end

  def format(val, _opts) when is_integer(val), do: Integer.to_string(val)

  def format(val, _opts) when is_float(val) do
    :erlang.float_to_binary(val, [:compact, {:decimals, 10}])
  end

  def format(true, _opts), do: "true"
  def format(false, _opts), do: "false"

  def format(%Date{} = d, _opts), do: Date.to_iso8601(d)
  def format(%Time{} = t, _opts), do: Time.to_iso8601(t)
  def format(%DateTime{} = dt, _opts), do: DateTime.to_iso8601(dt)
  def format(%NaiveDateTime{} = ndt, _opts), do: NaiveDateTime.to_iso8601(ndt)

  if Code.ensure_loaded?(Decimal) do
    def format(%Decimal{} = dec, _opts), do: Decimal.to_string(dec)
  end

  def format(val, _opts) when is_atom(val), do: Atom.to_string(val)

  def format(val, _opts) do
    case String.Chars.impl_for(val) do
      nil -> inspect(val)
      _ -> to_string(val)
    end
  end

  # Prevent CSV injection: if a cell starts with =, +, -, @, \t, or \r,
  # spreadsheet applications (Excel, Calc) may interpret it as a formula or command.
  defp sanitize_formula(<<char, _rest::binary>> = val)
       when char in [?=, ?+, ?-, ?@, ?\t, ?\r] do
    "'" <> val
  end

  defp sanitize_formula(val), do: val
end

