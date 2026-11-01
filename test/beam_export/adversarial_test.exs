defmodule BeamExport.AdversarialTest do
  use ExUnit.Case
  alias BeamExport.Test.Repo
  alias BeamExport.Test.User

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
  end

  describe "BE-001: Nested transaction safety and pool exhaustion" do
    test "to_file inside an existing Repo.transaction does not deadlock", %{test: test} do
      # Insert test records
      for i <- 1..10 do
        Repo.insert!(%User{
          name: "User #{i}",
          email: "user#{i}@example.com",
          role: "member"
        })
      end

      out_path = Path.join(System.tmp_dir!(), "#{test}_nested.csv")
      on_exit(fn -> File.rm(out_path) end)

      # Calling to_file inside an existing transaction must succeed without deadlock
      result =
        Repo.transaction(fn ->
          BeamExport.to_file(
            {Repo, User},
            out_path,
            headers: ["ID", "Name", "Email"],
            mapper: fn u -> [u.id, u.name, u.email] end
          )
        end)

      assert {:ok, {:ok, meta}} = result
      assert meta.rows == 11
      assert File.exists?(out_path)
    end
  end

  describe "BE-002: CSV multiline fields and row count accuracy" do
    test "multiline fields do not corrupt reported row count in to_file", %{test: test} do
      out_path = Path.join(System.tmp_dir!(), "#{test}_multiline.csv")
      on_exit(fn -> File.rm(out_path) end)

      # 3 rows, but fields contain multiple newlines (total 8 embedded newlines)
      rows = [
        [1, "Line 1\nLine 2\nLine 3", "alice@example.com"],
        [2, "Single Line", "bob@example.com"],
        [3, "Paragraph 1\r\nParagraph 2\nParagraph 3\nParagraph 4", "charlie@example.com"]
      ]

      assert {:ok, meta} =
               BeamExport.to_file(
                 rows,
                 out_path,
                 headers: ["ID", "Bio", "Email"]
               )

      # Must report exactly 4 rows (1 header + 3 data rows), NOT 12 rows!
      assert meta.rows == 4
    end

    test "multiline fields do not corrupt reported row count in to_iodata" do
      rows = [
        [1, "Line 1\nLine 2", "alice@example.com"],
        [2, "Line A\nLine B\nLine C", "bob@example.com"]
      ]

      assert {:ok, _iodata, meta} =
               BeamExport.to_iodata(
                 rows,
                 headers: ["ID", "Bio", "Email"]
               )

      # 1 header + 2 data rows = 3 rows
      assert meta.rows == 3
    end
  end

  describe "BE-004: Atomic writing and failure cleanup" do
    test "atomic: true is the default and cleans up temporary file on failure", %{test: test} do
      out_path = Path.join(System.tmp_dir!(), "#{test}_fail.csv")
      on_exit(fn -> File.rm(out_path) end)

      # Stream that fails halfway in mapper
      source = 1..100

      assert {:error, %BeamExport.MappingError{}} =
               BeamExport.to_file(
                 source,
                 out_path,
                 headers: ["ID", "Name"],
                 mapper: fn
                   50 -> raise "Simulated mid-stream mapper failure"
                   n -> [n, "Name #{n}"]
                 end
               )

      # Target file must NOT exist (no partial/corrupt file left on disk)
      refute File.exists?(out_path)

      # No temporary files matching pattern left in directory
      temp_files =
        Path.wildcard(Path.join(Path.dirname(out_path), ".tmp_#{Path.basename(out_path)}*"))

      assert temp_files == []
    end
  end

  describe "BE-005: International Unicode filenames in HTTP exports" do
    import Plug.Test

    test "send_staged_export preserves Unicode filenames via RFC 5987 / RFC 6266" do
      conn = conn(:get, "/export")
      data = [[1, "Test"]]

      conn =
        BeamExport.Plug.send_staged_export(
          conn,
          "顧客一覧_2026.csv",
          data,
          headers: ["ID", "Name"]
        )

      assert conn.status == 200
      [content_disposition] = Plug.Conn.get_resp_header(conn, "content-disposition")

      # Must include filename*=UTF-8''... per RFC 5987
      assert content_disposition =~ "filename*=UTF-8''"
      assert content_disposition =~ URI.encode("顧客一覧_2026.csv", &URI.char_unreserved?/1)
    end

    test "send_chunked_export preserves Unicode filenames" do
      conn = conn(:get, "/export")
      data = [[1, "Test"]]

      conn =
        BeamExport.Plug.send_chunked_export(
          conn,
          "données_utilisateurs.csv",
          data,
          headers: ["ID", "Name"]
        )

      assert conn.status == 200
      [content_disposition] = Plug.Conn.get_resp_header(conn, "content-disposition")
      assert content_disposition =~ "filename*=UTF-8''"

      assert content_disposition =~
               URI.encode("données_utilisateurs.csv", &URI.char_unreserved?/1)
    end
  end

  describe "BE-007: Proper error tuple returns instead of MatchError" do
    test "returns {:error, reason} when destination directory is unwritable" do
      # /root/unwritable_dir/export.csv cannot be created by normal user
      invalid_path = "/non_existent_root_dir_abc123/export.csv"

      assert {:error, reason} = BeamExport.to_file([[1, "Alice"]], invalid_path)
      # Should return standard File.Error or POSIX atom, not MatchError
      refute is_struct(reason, MatchError)
    end
  end

  describe "BE-008: Formula injection leading whitespace sanitization" do
    test "sanitizes formulas preceded by spaces, tabs, or newlines" do
      assert BeamExport.CSV.Formatter.format("=1+1", sanitize_formulas: true) == "'=1+1"
      assert BeamExport.CSV.Formatter.format("  =1+1", sanitize_formulas: true) == "'  =1+1"

      assert BeamExport.CSV.Formatter.format("\t+SUM(A1:B1)", sanitize_formulas: true) ==
               "'\t+SUM(A1:B1)"

      assert BeamExport.CSV.Formatter.format("   -2+3*cmd", sanitize_formulas: true) ==
               "'   -2+3*cmd"

      assert BeamExport.CSV.Formatter.format("  @SUM(A1)", sanitize_formulas: true) ==
               "'  @SUM(A1)"
    end

    test "does not alter regular text or numbers" do
      assert BeamExport.CSV.Formatter.format("Normal Text", sanitize_formulas: true) ==
               "Normal Text"

      assert BeamExport.CSV.Formatter.format(123, sanitize_formulas: true) == "123"
      assert BeamExport.CSV.Formatter.format("", sanitize_formulas: true) == ""
    end
  end
end
