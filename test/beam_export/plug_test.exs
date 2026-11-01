defmodule BeamExport.PlugTest do
  use ExUnit.Case, async: false
  import Plug.Test
  import Plug.Conn

  alias BeamExport.Test.CSV, as: TestParser

  describe "send_staged_export/4" do
    test "sends staged file with appropriate CSV headers and content" do
      conn = conn(:get, "/export")

      data = [
        [1, "Alice", "alice@example.com"],
        [2, "Bob", "bob@example.com"]
      ]

      conn =
        BeamExport.Plug.send_staged_export(
          conn,
          "users.csv",
          data,
          headers: ["ID", "Name", "Email"]
        )

      assert conn.status == 200
      assert get_resp_header(conn, "content-type") == ["text/csv; charset=utf-8"]

      assert get_resp_header(conn, "content-disposition") == [
               ~s(attachment; filename="users.csv")
             ]

      assert get_resp_header(conn, "cache-control") == ["no-cache, no-store, must-revalidate"]

      file_path = conn.private[:beam_export_file]
      assert file_path != nil
      assert File.exists?(file_path)

      # Verify content in response body (Plug.Test loads file into resp_body)
      parsed = TestParser.parse_string(conn.resp_body, skip_headers: false)

      assert parsed == [
               ["ID", "Name", "Email"],
               ["1", "Alice", "alice@example.com"],
               ["2", "Bob", "bob@example.com"]
             ]
    end

    test "cleanup monitor deletes staged temp file when process exits" do
      data = [[1, "Test"]]
      parent = self()

      {pid, _ref} =
        spawn_monitor(fn ->
          conn = conn(:get, "/export")
          conn = BeamExport.Plug.send_staged_export(conn, "test.csv", data)
          send(parent, {:file_path, conn.private[:beam_export_file]})
        end)

      file_path =
        receive do
          {:file_path, path} -> path
        after
          1000 -> flunk("Timed out waiting for file_path")
        end

      receive do
        {:DOWN, _ref, :process, ^pid, :normal} -> :ok
      after
        1000 -> flunk("Process did not exit")
      end

      # Wait brief moment for the cleanup monitor to receive :DOWN and remove the file
      Process.sleep(50)
      refute File.exists?(file_path)
    end
  end

  describe "send_chunked_export/4" do
    test "streams chunked response with headers" do
      conn = conn(:get, "/stream_export")

      data = [
        [10, "Alpha"],
        [20, "Beta"]
      ]

      conn =
        BeamExport.Plug.send_chunked_export(
          conn,
          "streamed.csv",
          data,
          headers: ["Num", "Word"]
        )

      assert conn.status == 200
      assert get_resp_header(conn, "content-type") == ["text/csv; charset=utf-8"]

      assert get_resp_header(conn, "content-disposition") == [
               ~s(attachment; filename="streamed.csv")
             ]

      assert conn.state == :chunked

      # In Plug.Test, chunks are stored in adapter state
      {_adapter, %{chunks: body}} = conn.adapter

      parsed = TestParser.parse_string(body, skip_headers: false)

      assert parsed == [
               ["Num", "Word"],
               ["10", "Alpha"],
               ["20", "Beta"]
             ]
    end
  end
end
