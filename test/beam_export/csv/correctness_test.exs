defmodule BeamExport.CSV.CorrectnessTest do
  # Regression tests for audit findings M1 (lossy value formatting) and M2 (row
  # shapes, silent blank columns, invalid separators, invalid mapper returns).
  use BeamExport.DataCase, async: false
  use ExUnitProperties

  alias BeamExport.CSV.Formatter
  alias BeamExport.{EncodeError, MappingError}

  # Returns the CSV text, or raises the exception returned by to_iodata/2.
  defp export!(rows, opts \\ []) do
    case BeamExport.to_iodata(rows, opts) do
      {:ok, iodata, _meta} -> IO.iodata_to_binary(iodata)
      {:error, exception} -> raise exception
    end
  end

  describe "M1: floats are exact" do
    test "known problem values" do
      assert Formatter.format(1.0e-12) == "1.0e-12"
      assert Formatter.format(0.1 + 0.2) == "0.30000000000000004"
      assert Formatter.format(123_456_789.12345679) == "123456789.12345679"
      assert Formatter.format(1.0e20) == "1.0e20"
      assert Formatter.format(12.5) == "12.5"
      assert Formatter.format(-0.0) == "-0.0"
    end

    property "every float round-trips through its CSV text" do
      check all(f <- float()) do
        assert {^f, ""} = Float.parse(Formatter.format(f))
      end
    end
  end

  describe "M1: decimals are exact and never scientific" do
    test "known problem values" do
      assert Formatter.format(Decimal.new("1E+3")) == "1000"
      assert Formatter.format(Decimal.mult(Decimal.new("1.0E2"), 3)) == "300"
      assert Formatter.format(Decimal.new("-0.00012")) == "-0.00012"
      assert Formatter.format(Decimal.new("12345.6789")) == "12345.6789"
      assert Formatter.format(Decimal.new("NaN")) == "NaN"
    end

    test "pathological exponents fall back to exact scientific notation (no huge strings)" do
      huge = Decimal.new(1, 5, 1_000_000_000)
      {time_us, text} = :timer.tc(fn -> Formatter.format(huge) end)
      assert text == "5E+1000000000"
      assert time_us < 100_000
      assert Decimal.equal?(Decimal.new(text), huge)
    end

    test "values just inside and outside the plain-notation width" do
      # 1 digit + 6_177 zeros = 6_178 characters: still plain.
      inside = Decimal.new(1, 1, 6_177)
      assert byte_size(Formatter.format(inside)) == 6_178
      refute Formatter.format(inside) =~ "E"
      # One more digit: scientific.
      assert Formatter.format(Decimal.new(1, 1, 6_178)) == "1E+6178"
      # A 200-digit coefficient takes the precise path.
      big = Decimal.new(1, Integer.pow(10, 199) + 7, -2)
      assert Decimal.equal?(Decimal.new(Formatter.format(big)), big)
    end

    property "every decimal round-trips through its CSV text" do
      check all(
              coef <- integer(-1_000_000_000..1_000_000_000),
              exp <- integer(-20..20)
            ) do
        d = Decimal.new(if(coef < 0, do: -1, else: 1), abs(coef), exp)
        text = Formatter.format(d)
        refute text =~ ~r/[eE]/
        assert Decimal.equal?(Decimal.new(text), d)
      end
    end
  end

  describe "M1: values without a faithful text form raise instead of being mangled" do
    test "lists are no longer concatenated" do
      error = assert_raise EncodeError, fn -> export!([[1, ["a", "b"]]]) end
      assert error.reason == {:unsupported_value, "list"}
      assert error.row == 1
      assert error.column == 2
      assert Exception.message(error) =~ "column 2 of data row 1"
      assert Exception.message(error) =~ "Enum.join"
    end

    test "charlist-looking lists are not turned into text" do
      assert_raise EncodeError, fn -> export!([[[72, 105]]]) end
    end

    test "maps, tuples, pids, references and functions raise" do
      for value <- [%{a: 1}, {1, 2}, self(), make_ref(), fn -> :ok end] do
        assert_raise EncodeError, fn -> export!([["ok", value]]) end
      end
    end

    test "structs without String.Chars raise; structs with it are rendered" do
      assert_raise EncodeError, ~r/struct Range/, fn -> export!([[1..3]]) end
      assert export!([[URI.parse("https://example.com/x")]]) == "https://example.com/x\r\n"
      assert export!([[Version.parse!("1.2.3")]]) == "1.2.3\r\n"
    end

    test "the error never contains the offending value" do
      error = assert_raise EncodeError, fn -> export!([[%{ssn: "SSN-123-45-6789"}]]) end
      refute Exception.message(error) =~ "SSN"
    end

    test "to_file returns the error and leaves no file behind" do
      path = Path.join(System.tmp_dir!(), "enc_#{System.unique_integer([:positive])}.csv")
      assert {:error, %EncodeError{row: 2}} = BeamExport.to_file([[1], [[2]]], path)
      refute File.exists?(path)
    end
  end

  describe "M2: row shapes" do
    test "tuple rows (e.g. Ecto select: {u.id, u.name}) are supported" do
      insert_users(3)
      query = from(u in User, select: {u.id, u.name}, order_by: u.id)

      assert {:ok, iodata, %{rows: 4}} =
               BeamExport.to_iodata({Repo, query}, headers: ["ID", "Name"])

      assert IO.iodata_to_binary(iodata) ==
               "ID,Name\r\n1,User 1\r\n2,User 2\r\n3,User 3\r\n"
    end

    test "tuple rows from to_stream and plain enumerables are supported" do
      assert export!([{1, "a"}, {2, "b"}]) == "1,a\r\n2,b\r\n"
    end

    test "a mapper may return a tuple" do
      assert export!([%{id: 1}], mapper: &{&1.id, "x"}) == "1,x\r\n"
    end

    test "a mapper returning something else raises MappingError, not a protocol error" do
      error = assert_raise MappingError, fn -> export!([%{id: 1}], mapper: & &1) end
      assert error.reason == {:invalid_return, "map"}
      assert error.row == 1
      assert error.record == :redacted
      assert Exception.message(error) =~ "must return a list"

      assert {:error, %MappingError{reason: {:invalid_return, "nil"}}} =
               BeamExport.to_iodata([1], mapper: fn _ -> nil end)
    end

    test "structs are read by atom headers or by strings naming their fields" do
      insert_users(2)
      query = from(u in User, order_by: u.id)

      assert {:ok, iodata, _} = BeamExport.to_iodata({Repo, query}, headers: [:id, "name"])
      assert IO.iodata_to_binary(iodata) == "id,name\r\n1,User 1\r\n2,User 2\r\n"
    end

    test "human-readable headers without a mapper raise instead of producing blank columns" do
      insert_users(2)

      assert {:error, %EncodeError{} = error} =
               BeamExport.to_iodata({Repo, User}, headers: ["ID", "Name"])

      assert error.reason == {:unknown_header, "ID", User}
      assert Exception.message(error) =~ "pass a :mapper"
    end

    test "structs and maps without headers or mapper raise a clear error" do
      insert_users(1)

      assert {:error, %EncodeError{reason: :map_without_headers}} =
               BeamExport.to_iodata({Repo, User})

      assert {:error, %EncodeError{reason: :map_without_headers, row: 1}} =
               BeamExport.to_iodata([%{id: 1}])
    end

    test "plain maps: missing keys are blank, but no matching key at all raises" do
      assert export!([%{id: 1, name: "a"}, %{id: 2}], headers: [:id, :name]) ==
               "id,name\r\n1,a\r\n2,\r\n"

      assert {:error, %EncodeError{reason: {:no_matching_headers, ["ID", "Name"]}, row: 1}} =
               BeamExport.to_iodata([%{id: 1, name: "a"}], headers: ["ID", "Name"])
    end

    test "scalar rows without a mapper raise" do
      assert {:error, %EncodeError{reason: {:invalid_row, "integer"}, row: 1}} =
               BeamExport.to_iodata(1..3)
    end
  end

  describe "M2: invalid configuration" do
    test "separators that would corrupt the CSV are rejected" do
      for sep <- ["\"", "\n", "\r", "", ";\n", 44] do
        assert_raise ArgumentError, fn -> BeamExport.to_iodata([[1]], separator: sep) end
      end
    end

    test "only standard line terminators are accepted" do
      assert export!([[1]], line_terminator: "\n") == "1\n"
      assert_raise ArgumentError, fn -> BeamExport.to_iodata([[1]], line_terminator: ";;") end
    end

    test "headers must be strings or atoms" do
      assert_raise ArgumentError, ~r/:headers/, fn ->
        BeamExport.to_iodata([[1]], headers: [["nested"]])
      end
    end

    test "invalid sources are rejected with a clear message" do
      assert_raise ArgumentError, ~r/invalid export source/, fn ->
        BeamExport.to_stream({:ok, [[1]]})
      end

      assert_raise ArgumentError, ~r/not an Ecto repository/, fn ->
        BeamExport.to_stream({String, :query})
      end

      assert_raise ArgumentError, ~r/expected an Enumerable/, fn -> BeamExport.to_stream(42) end

      # Consistent across APIs: raised, not returned.
      assert_raise ArgumentError, fn -> BeamExport.to_iodata({:ok, [[1]]}) end
      assert_raise ArgumentError, fn -> BeamExport.to_file(42, "/tmp/never.csv") end
    end
  end

  describe "round-trip properties" do
    @special [",", ";", "\t", "\"", "\r\n", "\n", "\r", " ", "=", "-", "''", "é", "🚀"]

    defp field_gen do
      gen all(parts <- list_of(one_of([string(:printable, max_length: 8), member_of(@special)]))) do
        Enum.join(parts)
      end
    end

    # Rows have at least two columns: a one-column row holding "" encodes as an
    # empty line, which CSV parsers (legitimately) treat as no row at all.
    defp rows_gen do
      list_of(list_of(field_gen(), length: 3), min_length: 1, max_length: 20)
    end

    for {sep, parser} <- [
          {",", BeamExport.Test.CSV},
          {";", BeamExport.Test.SemicolonCSV},
          {"\t", BeamExport.Test.TabCSV}
        ],
        terminator <- ["\r\n", "\n"] do
      @sep sep
      @parser parser
      @terminator terminator

      property "arbitrary text survives encode -> parse (separator #{inspect(sep)}, terminator #{inspect(terminator)})" do
        check all(rows <- rows_gen()) do
          csv =
            export!(rows,
              separator: @sep,
              line_terminator: @terminator,
              sanitize_formulas: false,
              chunk_size: 7
            )

          assert @parser.parse_string(csv, skip_headers: false) == rows
        end
      end
    end

    property "row counts are logical, whatever the field contents" do
      check all(rows <- rows_gen()) do
        {:ok, _iodata, meta} = BeamExport.to_iodata(rows, headers: ["a", "b", "c"])
        assert meta.rows == length(rows) + 1
      end
    end
  end
end
