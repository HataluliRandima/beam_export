defmodule BeamExport.EdgeCasesTest do
  # Behaviour that is easy to miss: client disconnects, filesystem failures,
  # error messages users will read, and option validation.
  use ExUnit.Case, async: false

  import Plug.Test
  alias BeamExport.{EncodeError, MappingError}
  alias BeamExport.Test.FakeRepo

  defmodule EndlessRepo do
    use BeamExport.Test.FakeRepo
    def stream(_query, _opts), do: Stream.iterate(1, &(&1 + 1)) |> Stream.map(&[&1])
  end

  defmodule RaisingRepo do
    use BeamExport.Test.FakeRepo
    def stream(_query, _opts), do: Stream.map(1..3, fn _ -> raise "cursor failed" end)
  end

  # A Plug adapter whose client disconnects as soon as the first chunk is sent.
  defmodule DisconnectingAdapter do
    def send_chunked(state, _status, _headers), do: {:ok, nil, state}
    def chunk(state, _body), do: send(state.test, :chunk_attempted) && {:error, :closed}
    def get_http_protocol(_state), do: :"HTTP/1.1"
  end

  defp root? do
    {uid, 0} = System.cmd("id", ["-u"])
    String.trim(uid) == "0"
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "beam_export_edge_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    on_exit(fn ->
      File.chmod(dir, 0o755)
      File.rm_rf(dir)
    end)

    {:ok, dir: dir}
  end

  describe "Plug" do
    test "chunked export stops reading an endless Ecto source when the client disconnects" do
      FakeRepo.listen()
      conn = %{conn(:get, "/") | adapter: {DisconnectingAdapter, %{test: self()}}}

      task =
        Task.async(fn ->
          BeamExport.Plug.send_chunked_export(conn, "x.csv", {EndlessRepo, BeamExport.Test.User},
            chunk_size: 5,
            max_rows: 5
          )
        end)

      assert %Plug.Conn{} = Task.await(task, 5_000)
      assert_received :chunk_attempted
      assert_receive {:worker, worker}, 1_000
      refute Process.alive?(worker)
    end

    test "an explicit :content_type and :charset are used", %{dir: dir} do
      conn =
        BeamExport.Plug.send_staged_export(conn(:get, "/"), "x.tsv", [[1, 2]],
          separator: "\t",
          content_type: "text/tab-separated-values",
          charset: "utf-8",
          tmp_dir: dir
        )

      assert Plug.Conn.get_resp_header(conn, "content-type") == [
               "text/tab-separated-values; charset=utf-8"
             ]

      assert conn.resp_body == "1\t2\r\n"
    end

    test "a :tmp_dir that cannot hold a private directory raises ExportError", %{dir: dir} do
      blocker = Path.join(dir, "file")
      File.write!(blocker, "")

      assert_raise BeamExport.ExportError, ~r/private temporary directory/, fn ->
        BeamExport.Plug.send_staged_export(conn(:get, "/"), "x.csv", [[1]],
          tmp_dir: Path.join(blocker, "sub")
        )
      end
    end
  end

  describe "filesystem failures" do
    test "an unwritable target directory fails before anything is written", %{dir: dir} do
      if root?(), do: flunk("cannot test permission errors as root")
      target = Path.join(dir, "report.csv")
      File.write!(target, "previous\n")
      File.chmod!(dir, 0o555)

      assert {:error, %File.Error{action: "create temporary file", reason: :eacces}} =
               BeamExport.to_file([[1]], target)

      File.chmod!(dir, 0o755)
      assert File.read!(target) == "previous\n"
      assert File.ls!(dir) == ["report.csv"]
    end
  end

  describe "transaction/2 fallback (Ecto < 3.13)" do
    test "a failing cursor rolls back and returns the original exception" do
      assert {:error, %RuntimeError{message: "cursor failed"}} =
               BeamExport.to_iodata({RaisingRepo, BeamExport.Test.User})
    end
  end

  describe "error messages" do
    test "invalid rows name the row's type" do
      for {row, type} <- [{"text", "string"}, {:atom, "atom"}, {1.5, "float"}, {self(), "pid"}] do
        assert {:error, %EncodeError{} = error} = BeamExport.to_iodata([row])
        assert error.reason == {:invalid_row, type}
        assert Exception.message(error) =~ "data row 1 is a #{type}"
        assert Exception.message(error) =~ ":mapper"
      end
    end

    test "map and header mismatches explain how to fix them" do
      assert {:error, error} = BeamExport.to_iodata([%{id: 1}])
      assert Exception.message(error) =~ "neither :headers nor :mapper"

      assert {:error, error} = BeamExport.to_iodata([%{id: 1}], headers: ["Nope"])
      assert Exception.message(error) =~ "none of the headers"
    end

    test "mapper failures carry a safe description of common exceptions" do
      cases = [
        {fn _ -> apply(NoSuchModule, :f, []) end, "function NoSuchModule.f/0 is undefined"},
        {fn r -> Enum.map(r, & &1) end, "protocol Enumerable not implemented for integer"},
        # The divisor is opaque to the compiler's type checker on purpose.
        {fn r -> [div(r, Process.get(:zero, 0))] end, "bad argument in arithmetic expression"}
      ]

      for {mapper, detail} <- cases do
        assert {:error, %MappingError{detail: ^detail}} =
                 BeamExport.to_iodata([1], mapper: mapper)
      end
    end
  end

  describe "option validation" do
    test "options must be a keyword list" do
      assert_raise ArgumentError, ~r/keyword list/, fn -> BeamExport.to_iodata([[1]], %{}) end
    end

    test "every option rejects invalid values with a message naming it" do
      invalid = [
        headers: "id",
        mapper: fn -> :arity_0 end,
        chunk_size: -1,
        max_rows: 0,
        sanitize_formulas: "yes",
        include_record_in_errors: 1,
        timeout: 0,
        consumer_timeout: -5,
        batch_timeout: :never,
        separator: 1
      ]

      for {key, value} <- invalid do
        error = assert_raise ArgumentError, fn -> BeamExport.to_iodata([[1]], [{key, value}]) end
        assert Exception.message(error) =~ inspect(key)
      end

      for {key, value} <- [atomic: :yes, file_mode: -1, compress: :zip] do
        assert_raise ArgumentError, ~r/#{inspect(key)}/, fn ->
          BeamExport.to_file([[1]], "/tmp/never.csv", [{key, value}])
        end
      end

      for {key, value} <- [tmp_dir: "", content_type: 1, charset: 5] do
        assert_raise ArgumentError, ~r/#{inspect(key)}/, fn ->
          BeamExport.Plug.send_staged_export(conn(:get, "/"), "x.csv", [[1]], [{key, value}])
        end
      end
    end

    test "options of other APIs are rejected" do
      assert_raise ArgumentError, ~r/unknown option :atomic/, fn ->
        BeamExport.to_stream([[1]], atomic: true)
      end

      assert_raise ArgumentError, ~r/unknown option :tmp_dir/, fn ->
        BeamExport.to_file([[1]], "/tmp/never.csv", tmp_dir: "/tmp")
      end
    end
  end
end
