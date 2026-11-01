defmodule BeamExportTest do
  use ExUnit.Case, async: true

  alias BeamExport.Test.CSV, as: TestParser

  describe "BeamExport facade" do
    test "to_stream/2 creates an enumerable stream" do
      stream = BeamExport.to_stream([["a", "b"], ["c", "d"]], headers: ["Col 1", "Col 2"])
      result = stream |> Enum.to_list() |> IO.iodata_to_binary()
      parsed = TestParser.parse_string(result, skip_headers: false)

      assert parsed == [
               ["Col 1", "Col 2"],
               ["a", "b"],
               ["c", "d"]
             ]
    end

    test "to_iodata/2 returns iodata and metadata" do
      {:ok, iodata, meta} = BeamExport.to_iodata([[1, 2], [3, 4]], headers: ["X", "Y"])
      binary = IO.iodata_to_binary(iodata)

      assert meta.rows == 3
      assert meta.bytes == byte_size(binary)
      assert binary =~ "X,Y\r\n1,2\r\n3,4\r\n"
    end
  end
end
