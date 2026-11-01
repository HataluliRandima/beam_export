defmodule BeamExport.CSV.EncoderTest do
  use ExUnit.Case, async: true

  alias BeamExport.CSV.Encoder
  alias BeamExport.CSV.Formatter
  alias BeamExport.Test.CSV, as: TestParser
  alias BeamExport.Test.SemicolonCSV, as: SemicolonParser

  describe "encode_field/3 and RFC 4180 escaping" do
    test "leaves simple alphanumeric strings unquoted" do
      assert IO.iodata_to_binary(Encoder.encode_field("hello", [])) == "hello"
      assert IO.iodata_to_binary(Encoder.encode_field("12345", [])) == "12345"
    end

    test "quotes strings containing commas" do
      encoded = Encoder.encode_field("hello, world", [])
      assert IO.iodata_to_binary(encoded) == ~s("hello, world")
    end

    test "escapes and quotes strings containing double quotes" do
      encoded = Encoder.encode_field(~s(John "The Rock" Doe), [])
      assert IO.iodata_to_binary(encoded) == ~s("John ""The Rock"" Doe")
    end

    test "quotes strings containing newlines or carriage returns" do
      encoded_lf = Encoder.encode_field("line1\nline2", [])
      assert IO.iodata_to_binary(encoded_lf) == ~s("line1\nline2")

      encoded_crlf = Encoder.encode_field("line1\r\nline2", [])
      assert IO.iodata_to_binary(encoded_crlf) == ~s("line1\r\nline2")
    end

    test "handles unicode strings safely" do
      unicode = "José, Günter & 🚀"
      encoded = Encoder.encode_field(unicode, [])
      assert IO.iodata_to_binary(encoded) == ~s("José, Günter & 🚀")
    end

    test "handles custom separators" do
      encoded = Encoder.encode_field("one;two", separator: ";")
      assert IO.iodata_to_binary(encoded) == ~s("one;two")
    end
  end

  describe "type formatting" do
    test "formats nil as empty string" do
      assert IO.iodata_to_binary(Encoder.encode_field(nil, [])) == ""
    end

    test "formats integers and floats" do
      assert IO.iodata_to_binary(Encoder.encode_field(42, [])) == "42"
      assert IO.iodata_to_binary(Encoder.encode_field(3.14159, [])) == "3.14159"
    end

    test "formats booleans" do
      assert IO.iodata_to_binary(Encoder.encode_field(true, [])) == "true"
      assert IO.iodata_to_binary(Encoder.encode_field(false, [])) == "false"
    end

    test "formats dates and times" do
      date = ~D[2026-11-01]
      time = ~T[12:30:00]
      datetime = DateTime.from_naive!(~N[2026-11-01 12:30:00], "Etc/UTC")

      assert IO.iodata_to_binary(Encoder.encode_field(date, [])) == "2026-11-01"
      assert IO.iodata_to_binary(Encoder.encode_field(time, [])) == "12:30:00"
      assert IO.iodata_to_binary(Encoder.encode_field(datetime, [])) == "2026-11-01T12:30:00Z"
    end

    test "formats Decimal" do
      dec = Decimal.new("12345.6789")
      assert IO.iodata_to_binary(Encoder.encode_field(dec, [])) == "12345.6789"
    end

    test "formats atoms" do
      assert IO.iodata_to_binary(Encoder.encode_field(:active, [])) == "active"
    end
  end

  describe "security: formula sanitization" do
    test "neutralizes formula prefixes when sanitize_formulas is true" do
      assert Formatter.format("=SUM(A1:A10)", sanitize_formulas: true) == "'=SUM(A1:A10)"

      assert Formatter.format("+cmd|' /C calc'!A0", sanitize_formulas: true) ==
               "'+cmd|' /C calc'!A0"

      assert Formatter.format("-10", sanitize_formulas: true) == "'-10"
      assert Formatter.format("@company", sanitize_formulas: true) == "'@company"
    end

    test "leaves formulas untouched when sanitize_formulas is false" do
      assert Formatter.format("=SUM(A1:A10)", sanitize_formulas: false) == "=SUM(A1:A10)"
    end
  end

  describe "encode_row/2 and round-trip verification" do
    test "encodes a full row with CRLF line ending" do
      row = [1, "Alice", "alice@example.com", Decimal.new("99.95"), ~D[2026-01-01]]
      encoded = Encoder.encode_row(row, [])
      binary = IO.iodata_to_binary(encoded)

      assert binary == "1,Alice,alice@example.com,99.95,2026-01-01\r\n"

      parsed = TestParser.parse_string(binary, skip_headers: false)
      assert parsed == [["1", "Alice", "alice@example.com", "99.95", "2026-01-01"]]
    end

    test "encodes rows containing quotes, commas, and newlines correctly" do
      row = [42, ~s(Line 1\nLine 2), ~s("Quotes"), "Commas, separated"]
      binary = Encoder.encode_row(row, []) |> IO.iodata_to_binary()

      parsed = TestParser.parse_string(binary, skip_headers: false)
      assert parsed == [["42", "Line 1\nLine 2", ~s("Quotes"), "Commas, separated"]]
    end

    test "supports custom separator and line terminator" do
      row = ["first", "second;val", "third"]

      binary =
        Encoder.encode_row(row, separator: ";", line_terminator: "\n") |> IO.iodata_to_binary()

      assert binary == "first;\"second;val\";third\n"

      parsed = SemicolonParser.parse_string(binary, skip_headers: false)
      assert parsed == [["first", "second;val", "third"]]
    end
  end

  describe "encode_chunk/2" do
    test "encodes multiple rows in a batch" do
      rows = [
        [1, "Alice"],
        [2, "Bob, Jr."],
        [3, "Charlie"]
      ]

      chunk_binary = Encoder.encode_chunk(rows, []) |> IO.iodata_to_binary()
      parsed = TestParser.parse_string(chunk_binary, skip_headers: false)

      assert parsed == [
               ["1", "Alice"],
               ["2", "Bob, Jr."],
               ["3", "Charlie"]
             ]
    end
  end
end
