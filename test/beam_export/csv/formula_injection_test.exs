defmodule BeamExport.CSV.FormulaInjectionTest do
  # Regression tests for audit finding M3 (CSV / formula injection).
  use ExUnit.Case, async: true

  alias BeamExport.CSV.Formatter

  defp export(rows, opts \\ []) do
    {:ok, iodata, _meta} = BeamExport.to_iodata(rows, opts)
    IO.iodata_to_binary(iodata)
  end

  test "sanitization is enabled by default" do
    assert export([["=HYPERLINK(\"http://evil\")"]]) ==
             "\"'=HYPERLINK(\"\"http://evil\"\")\"\r\n"

    assert export([["+cmd|' /C calc'!A0"]]) == "'+cmd|' /C calc'!A0\r\n"
    assert export([["@SUM(A1)"]]) == "'@SUM(A1)\r\n"
    assert export([["-2+3"]]) == "'-2+3\r\n"
  end

  test "a leading TAB or CR is neutralized (OWASP)" do
    assert Formatter.format("\tfoo", sanitize_formulas: true) == "'\tfoo"
    assert Formatter.format("\r=1+1", sanitize_formulas: true) == "'\r=1+1"
  end

  test "triggers after leading whitespace are neutralized" do
    assert Formatter.format("  =1+1", sanitize_formulas: true) == "'  =1+1"
    assert Formatter.format("\n=1+1", sanitize_formulas: true) == "'\n=1+1"
  end

  test "atoms are sanitized like strings" do
    assert Formatter.format(:"=cmd()", sanitize_formulas: true) == "'=cmd()"
    assert Formatter.format(:active, sanitize_formulas: true) == "active"
  end

  test "headers are sanitized too" do
    assert export([], headers: ["=evil", "Name"]) == "'=evil,Name\r\n"
  end

  test "numbers are never altered" do
    assert export([[-5, -1.5, Decimal.new("-2.50")]]) == "-5,-1.5,-2.50\r\n"
  end

  test "harmless text is not altered" do
    assert Formatter.format("Normal text", sanitize_formulas: true) == "Normal text"
    assert Formatter.format("a=b", sanitize_formulas: true) == "a=b"
    assert Formatter.format("", sanitize_formulas: true) == ""
  end

  test "sanitize_formulas: false disables sanitization (for machine consumers)" do
    assert export([["=1+1", "-5"]], sanitize_formulas: false) == "=1+1,-5\r\n"
  end

  test "a misspelt option raises instead of silently disabling protection" do
    error =
      assert_raise ArgumentError, fn ->
        BeamExport.to_iodata([["=1+1"]], sanitize_formula: false)
      end

    assert Exception.message(error) =~ "unknown option :sanitize_formula"
    assert Exception.message(error) =~ "Did you mean :sanitize_formulas?"
  end
end
