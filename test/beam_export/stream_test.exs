defmodule BeamExport.StreamTest do
  use ExUnit.Case, async: true

  alias BeamExport.Test.CSV, as: TestParser

  describe "BeamExport.to_stream/2 with enumerable sources" do
    test "streams empty collection with headers emits only header row" do
      stream = BeamExport.to_stream([], headers: ["ID", "Name"])
      result = stream |> Enum.to_list() |> IO.iodata_to_binary()

      assert result == "ID,Name\r\n"
      assert TestParser.parse_string(result, skip_headers: false) == [["ID", "Name"]]
    end

    test "streams empty collection without headers emits empty output" do
      stream = BeamExport.to_stream([], [])
      result = stream |> Enum.to_list() |> IO.iodata_to_binary()

      assert result == ""
    end

    test "streams rows using custom mapper" do
      users = [
        %{id: 1, name: "Alice", email: "alice@example.com"},
        %{id: 2, name: "Bob", email: "bob@example.com"}
      ]

      stream =
        BeamExport.to_stream(users,
          headers: ["User ID", "Name", "Email"],
          mapper: fn u -> [u.id, u.name, u.email] end
        )

      result = stream |> Enum.to_list() |> IO.iodata_to_binary()
      parsed = TestParser.parse_string(result, skip_headers: false)

      assert parsed == [
               ["User ID", "Name", "Email"],
               ["1", "Alice", "alice@example.com"],
               ["2", "Bob", "bob@example.com"]
             ]
    end

    test "automatically extracts map fields matching headers when mapper is omitted" do
      users = [
        %{id: 10, name: "Charlie"},
        %{id: 20, name: "Diana"}
      ]

      stream = BeamExport.to_stream(users, headers: [:id, :name])
      result = stream |> Enum.to_list() |> IO.iodata_to_binary()
      parsed = TestParser.parse_string(result, skip_headers: false)

      assert parsed == [
               ["id", "name"],
               ["10", "Charlie"],
               ["20", "Diana"]
             ]
    end

    test "respects chunk_size" do
      numbers = 1..10

      # 10 rows with chunk_size: 3 -> 4 data chunks + 1 header chunk = 5 chunks
      stream =
        BeamExport.to_stream(numbers,
          headers: ["Num"],
          mapper: fn n -> [n] end,
          chunk_size: 3
        )

      chunks = Enum.to_list(stream)
      assert length(chunks) == 5

      full_output = IO.iodata_to_binary(chunks)
      lines = String.split(full_output, "\r\n", trim: true)
      assert length(lines) == 11
    end

    test "stream evaluation is strictly lazy" do
      # Create an infinite stream that would crash or hang if evaluated eagerly
      infinite_source = Stream.iterate(1, &(&1 + 1))

      first_5 =
        BeamExport.to_stream(infinite_source,
          headers: ["Val"],
          mapper: fn n -> [n * 10] end,
          chunk_size: 2
        )
        |> Stream.take(3)
        |> Enum.to_list()
        |> IO.iodata_to_binary()

      parsed = TestParser.parse_string(first_5, skip_headers: false)
      # Header + 2 chunks of 2 rows each = 5 rows total
      assert parsed == [
               ["Val"],
               ["10"],
               ["20"],
               ["30"],
               ["40"]
             ]
    end

    test "wraps mapper exceptions in BeamExport.MappingError" do
      flawed_data = [
        %{id: 1, ok: true},
        %{id: 2, ok: false}
      ]

      stream =
        BeamExport.to_stream(flawed_data,
          mapper: fn item ->
            if item.ok, do: [item.id], else: raise("boom!")
          end
        )

      assert_raise BeamExport.MappingError, fn ->
        Enum.to_list(stream)
      end
    end
  end

  describe "BeamExport.to_iodata/2" do
    test "collects small dataset into iodata with metadata" do
      records = [
        [1, "Test 1"],
        [2, "Test 2"]
      ]

      assert {:ok, iodata, meta} = BeamExport.to_iodata(records, headers: ["ID", "Name"])
      binary = IO.iodata_to_binary(iodata)

      assert meta.rows == 3
      assert meta.bytes == byte_size(binary)
      assert binary =~ "ID,Name\r\n1,Test 1\r\n2,Test 2\r\n"
    end
  end
end
