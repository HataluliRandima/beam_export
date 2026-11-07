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

    test "supports compressed .csv.gz staged exports with application/gzip content type" do
      conn = conn(:get, "/export")

      data = [
        [1, "Alice", "alice@example.com"],
        [2, "Bob", "bob@example.com"]
      ]

      conn =
        BeamExport.Plug.send_staged_export(
          conn,
          "users.csv.gz",
          data,
          headers: ["ID", "Name", "Email"]
        )

      assert conn.status == 200
      assert get_resp_header(conn, "content-type") == ["application/gzip"]

      assert get_resp_header(conn, "content-disposition") == [
               ~s(attachment; filename="users.csv.gz")
             ]

      raw_body = conn.resp_body
      assert <<0x1F, 0x8B, _rest::binary>> = raw_body

      uncompressed = :zlib.gunzip(raw_body)
      parsed = TestParser.parse_string(uncompressed, skip_headers: false)
      assert length(parsed) == 3
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
      refute File.exists?(Path.dirname(file_path))
    end

    test "stages the file 0600 inside a fresh 0700 directory under :tmp_dir" do
      import Bitwise

      root =
        Path.join(System.tmp_dir!(), "beam_export_plug_#{System.unique_integer([:positive])}")

      on_exit(fn -> File.rm_rf(root) end)
      parent = self()

      spawn(fn ->
        conn = BeamExport.Plug.send_staged_export(conn(:get, "/"), "r.csv", [[1]], tmp_dir: root)
        path = conn.private[:beam_export_file]
        send(parent, {:staged, path, File.stat!(path).mode, File.stat!(Path.dirname(path)).mode})

        receive do
          :exit -> :ok
        end
      end)

      assert_receive {:staged, path, file_mode, dir_mode}, 2_000
      assert Path.dirname(Path.dirname(path)) == root
      assert (file_mode &&& 0o777) == 0o600
      assert (dir_mode &&& 0o777) == 0o700
    end

    test "hostile filenames cannot escape the private directory" do
      for name <- ["..", ".", "", "../../etc/passwd", "a/../../b.csv", "\r\nx.csv"] do
        conn = BeamExport.Plug.send_staged_export(conn(:get, "/"), name, [[1]])
        path = conn.private[:beam_export_file]
        assert Path.basename(Path.dirname(path)) =~ ~r/^beam_export_[0-9a-f]{24}$/
        assert Path.basename(path) =~ ~r/^export_[A-Za-z0-9._-]*$/
      end
    end

    test "content type follows what was written, not just the filename" do
      conn =
        BeamExport.Plug.send_staged_export(conn(:get, "/"), "plain.csv.gz", [[1]],
          compress: false
        )

      assert get_resp_header(conn, "content-type") == ["text/csv; charset=utf-8"]
      assert conn.resp_body == "1\r\n"
    end

    test "invalid options raise before any response is sent" do
      assert_raise ArgumentError, ~r/unknown option :tmpdir/, fn ->
        BeamExport.Plug.send_staged_export(conn(:get, "/"), "x.csv", [[1]], tmpdir: "/tmp")
      end
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

defmodule BeamExport.PlugEctoTest do
  use BeamExport.DataCase, async: false
  import Plug.Test

  test "send_chunked_export streams an Ecto query (worker path, sandbox allowance)" do
    insert_users(25)

    conn =
      BeamExport.Plug.send_chunked_export(
        conn(:get, "/"),
        "users.csv",
        {Repo, from(u in User, order_by: u.id)},
        headers: [:id, :name],
        max_rows: 10,
        chunk_size: 10
      )

    {_adapter, %{chunks: body}} = conn.adapter
    lines = String.split(body, "\r\n", trim: true)
    assert hd(lines) == "id,name"
    assert length(lines) == 26
  end

  test "send_chunked_export rejects an invalid query before starting the response" do
    assert_raise BeamExport.PreloadError, fn ->
      BeamExport.Plug.send_chunked_export(
        conn(:get, "/"),
        "users.csv",
        {Repo, from(u in User, preload: :posts)}
      )
    end
  end
end
