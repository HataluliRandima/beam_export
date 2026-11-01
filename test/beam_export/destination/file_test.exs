defmodule BeamExport.Destination.FileTest do
  use ExUnit.Case, async: false

  alias BeamExport.Test.CSV, as: TestParser
  alias BeamExport.Test.Repo
  alias BeamExport.Test.User

  setup do
    tmp_dir = Path.join(System.tmp_dir!(), "beam_export_file_test_#{:rand.uniform(100_000)}")
    File.mkdir_p!(tmp_dir)

    on_exit(fn ->
      File.rm_rf(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  describe "BeamExport.to_file/3" do
    test "writes an enumerable stream to a file with headers and metadata", %{tmp_dir: tmp_dir} do
      out_path = Path.join(tmp_dir, "users.csv")

      users = [
        [1, "Alice", "alice@example.com"],
        [2, "Bob", "bob@example.com"]
      ]

      assert {:ok, meta} = BeamExport.to_file(users, out_path, headers: ["ID", "Name", "Email"])

      assert File.exists?(out_path)
      assert meta.path == out_path
      assert meta.rows == 3
      assert meta.bytes > 0

      content = File.read!(out_path)
      parsed = TestParser.parse_string(content, skip_headers: false)

      assert parsed == [
               ["ID", "Name", "Email"],
               ["1", "Alice", "alice@example.com"],
               ["2", "Bob", "bob@example.com"]
             ]
    end

    test "automatically creates parent directory if it does not exist", %{tmp_dir: tmp_dir} do
      out_path = Path.join([tmp_dir, "nested", "reports", "export.csv"])

      assert {:ok, meta} = BeamExport.to_file([["a", "b"]], out_path, headers: ["ColA", "ColB"])
      assert File.exists?(out_path)
      assert meta.rows == 2
    end

    test "supports atomic writing and renames on success", %{tmp_dir: tmp_dir} do
      out_path = Path.join(tmp_dir, "atomic_export.csv")

      assert {:ok, meta} =
               BeamExport.to_file(
                 1..10,
                 out_path,
                 headers: ["Number"],
                 mapper: fn n -> [n] end,
                 atomic: true
               )

      assert File.exists?(out_path)
      assert meta.rows == 11
    end

    test "cleans up atomic temp file when export fails", %{tmp_dir: tmp_dir} do
      out_path = Path.join(tmp_dir, "failed_export.csv")

      # Failure in mapper
      assert {:error, %BeamExport.MappingError{}} =
               BeamExport.to_file(
                 [1, 2, 3],
                 out_path,
                 mapper: fn
                   2 -> raise("fatal mapper error")
                   n -> [n]
                 end,
                 atomic: true
               )

      # Target file must not exist
      refute File.exists?(out_path)

      # No residual temp files should remain in the directory
      files_in_dir = File.ls!(tmp_dir)
      assert files_in_dir == []
    end

    test "writes an Ecto query to file directly", %{tmp_dir: tmp_dir} do
      Ecto.Adapters.SQL.Sandbox.checkout(Repo)
      Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})

      Repo.delete_all(User)

      users =
        for i <- 1..20 do
          %{name: "User #{i}", email: "user#{i}@example.com", role: "member"}
        end

      Repo.insert_all(
        User,
        Enum.map(
          users,
          &Map.merge(&1, %{
            inserted_at: ~N[2026-01-01 00:00:00],
            updated_at: ~N[2026-01-01 00:00:00]
          })
        )
      )

      out_path = Path.join(tmp_dir, "ecto_users.csv")

      assert {:ok, meta} =
               BeamExport.to_file(
                 {Repo, User},
                 out_path,
                 headers: ["Name", "Email"],
                 mapper: fn u -> [u.name, u.email] end,
                 max_rows: 5
               )

      assert File.exists?(out_path)
      assert meta.rows == 21

      content = File.read!(out_path)
      parsed = TestParser.parse_string(content, skip_headers: false)
      assert length(parsed) == 21
      assert hd(parsed) == ["Name", "Email"]
    end

    test "streams compressed gzip archive when target ends with .gz", %{tmp_dir: tmp_dir} do
      out_path = Path.join(tmp_dir, "compressed_export.csv.gz")

      users = [
        [1, "Alice", "alice@example.com"],
        [2, "Bob", "bob@example.com"]
      ]

      assert {:ok, meta} = BeamExport.to_file(users, out_path, headers: ["ID", "Name", "Email"])

      assert File.exists?(out_path)
      assert meta.compressed == true
      assert meta.rows == 3

      raw_bytes = File.read!(out_path)
      # Check GZIP magic bytes 0x1F, 0x8B
      assert <<0x1F, 0x8B, _rest::binary>> = raw_bytes

      # Decompress using OTP :zlib
      uncompressed = :zlib.gunzip(raw_bytes)
      parsed = TestParser.parse_string(uncompressed, skip_headers: false)

      assert parsed == [
               ["ID", "Name", "Email"],
               ["1", "Alice", "alice@example.com"],
               ["2", "Bob", "bob@example.com"]
             ]
    end

    test "streams compressed gzip archive when target ends with .gz", %{tmp_dir: tmp_dir} do
      out_path = Path.join(tmp_dir, "compressed_export.csv.gz")
      users = [
        [1, "Alice", "alice@example.com"],
        [2, "Bob", "bob@example.com"]
      ]

      assert {:ok, meta} = BeamExport.to_file(users, out_path, headers: ["ID", "Name", "Email"])

      assert File.exists?(out_path)
      assert meta.compressed == true
      assert meta.rows == 3

      raw_bytes = File.read!(out_path)
      # Check GZIP magic bytes 0x1F, 0x8B
      assert <<0x1F, 0x8B, _rest::binary>> = raw_bytes

      # Decompress using OTP :zlib
      uncompressed = :zlib.gunzip(raw_bytes)
      parsed = TestParser.parse_string(uncompressed, skip_headers: false)

      assert parsed == [
        ["ID", "Name", "Email"],
        ["1", "Alice", "alice@example.com"],
        ["2", "Bob", "bob@example.com"]
      ]
    end
  end
end
