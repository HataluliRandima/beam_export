defmodule BeamExport.Destination.FileIntegrityTest do
  # Regression tests for audit findings H1 (close errors ignored), H3 (destructive
  # cleanup with atomic: false) and M4 (world-readable temporary files).
  use BeamExport.DataCase, async: false

  import Bitwise

  setup do
    dir =
      Path.join(System.tmp_dir!(), "beam_export_integrity_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.chmod(dir, 0o755) && File.rm_rf(dir) end)
    {:ok, dir: dir}
  end

  defp mode(path), do: File.stat!(path).mode &&& 0o777
  defp temp_files(dir), do: dir |> File.ls!() |> Enum.filter(&String.starts_with?(&1, ".tmp_"))

  defp root? do
    {uid, 0} = System.cmd("id", ["-u"])
    String.trim(uid) == "0"
  end

  describe "H1: write errors are never reported as success" do
    @tag :requires_dev_full
    test "ENOSPC surfaced when the file is closed fails the export" do
      assert {:error, %File.Error{reason: :enospc, path: "/dev/full"} = error} =
               BeamExport.to_file([["x"]], "/dev/full", atomic: false)

      assert Exception.message(error) =~ "no space left on device"
      # Cleanup must never delete non-regular files.
      assert File.exists?("/dev/full")
    end

    @tag :requires_dev_full
    test "ENOSPC from a gzip stream fails the export" do
      assert {:error, %File.Error{reason: :enospc}} =
               BeamExport.to_file([["x"]], "/dev/full", atomic: false, compress: true)
    end

    @tag :requires_dev_full
    test "ENOSPC from a large write fails the export" do
      rows = Stream.map(1..20_000, &[&1, String.duplicate("x", 50)])

      assert {:error, %File.Error{reason: :enospc}} =
               BeamExport.to_file(rows, "/dev/full", atomic: false)
    end

    test "a failed atomic rename returns an error and leaves no temporary file", %{dir: dir} do
      target = Path.join(dir, "is_a_directory")
      File.mkdir_p!(Path.join(target, "child"))

      assert {:error, %File.RenameError{destination: ^target}} =
               BeamExport.to_file([[1]], target)

      assert temp_files(dir) == []
    end
  end

  describe "H3: existing files are never destroyed by a failed export" do
    test "atomic: false keeps a pre-existing file when the query is rejected", %{dir: dir} do
      target = Path.join(dir, "precious.csv")
      File.write!(target, "important existing data\n")

      assert {:error, %BeamExport.PreloadError{}} =
               BeamExport.to_file({Repo, from(u in User, preload: :posts)}, target, atomic: false)

      assert File.read!(target) == "important existing data\n"
    end

    test "atomic: false keeps a pre-existing file it is not allowed to open", %{dir: dir} do
      if root?(), do: flunk("cannot test permission errors as root")
      target = Path.join(dir, "readonly.csv")
      File.write!(target, "do not touch\n")
      File.chmod!(target, 0o444)

      assert {:error, %File.Error{reason: :eacces}} =
               BeamExport.to_file([[1]], target, atomic: false)

      assert File.read!(target) == "do not touch\n"
    end

    test "atomic: false keeps a pre-existing file when the directory cannot be created",
         %{dir: dir} do
      blocker = Path.join(dir, "not_a_dir")
      File.write!(blocker, "a file, not a directory")

      assert {:error, %File.Error{action: "create directory"}} =
               BeamExport.to_file([[1]], Path.join(blocker, "out.csv"), atomic: false)

      assert File.read!(blocker) == "a file, not a directory"
    end

    test "atomic: true leaves the existing target untouched when the export fails mid-way",
         %{dir: dir} do
      target = Path.join(dir, "report.csv")
      File.write!(target, "previous good export\n")

      assert {:error, %BeamExport.MappingError{}} =
               BeamExport.to_file(1..1_000, target,
                 chunk_size: 10,
                 mapper: fn
                   500 -> raise "boom"
                   n -> [n]
                 end
               )

      assert File.read!(target) == "previous good export\n"
      assert temp_files(dir) == []
    end

    test "atomic: true replaces the existing target on success", %{dir: dir} do
      target = Path.join(dir, "report.csv")
      File.write!(target, "old\n")
      assert {:ok, %{rows: 2}} = BeamExport.to_file([["new"], ["rows"]], target)
      assert File.read!(target) == "new\r\nrows\r\n"
    end

    test "atomic: false removes its own partial output when the export fails mid-way",
         %{dir: dir} do
      target = Path.join(dir, "partial.csv")

      assert {:error, %BeamExport.MappingError{}} =
               BeamExport.to_file(1..100, target,
                 atomic: false,
                 chunk_size: 10,
                 mapper: fn
                   50 -> raise "boom"
                   n -> [n]
                 end
               )

      refute File.exists?(target)
    end

    test "an exit from the source is re-raised and leaves no temporary file", %{dir: dir} do
      source =
        Stream.map(1..100, fn
          50 -> exit(:source_gone)
          n -> [n]
        end)

      assert catch_exit(BeamExport.to_file(source, Path.join(dir, "x.csv"), chunk_size: 5)) ==
               :source_gone

      assert File.ls!(dir) == []
    end
  end

  describe "M4: file permissions" do
    test "the in-flight temporary file and the final file are 0600 by default", %{dir: dir} do
      target = Path.join(dir, "secret.csv")
      parent = self()

      assert {:ok, _} =
               BeamExport.to_file(1..3, target,
                 chunk_size: 1,
                 mapper: fn n ->
                   if n == 3,
                     do:
                       send(
                         parent,
                         {:temp_modes, Enum.map(temp_files(dir), &mode(Path.join(dir, &1)))}
                       )

                   [n]
                 end
               )

      assert_received {:temp_modes, [0o600]}
      assert mode(target) == 0o600
    end

    test ":file_mode sets the permissions of the created file", %{dir: dir} do
      target = Path.join(dir, "shared.csv")
      assert {:ok, _} = BeamExport.to_file([[1]], target, file_mode: 0o640)
      assert mode(target) == 0o640
    end

    test "atomic: false keeps the permissions of an existing file", %{dir: dir} do
      target = Path.join(dir, "existing.csv")
      File.write!(target, "")
      File.chmod!(target, 0o664)
      assert {:ok, _} = BeamExport.to_file([[1]], target, atomic: false)
      assert mode(target) == 0o664
    end

    test "gzip output is created with the requested mode", %{dir: dir} do
      target = Path.join(dir, "secret.csv.gz")
      assert {:ok, %{compressed: true}} = BeamExport.to_file([[1]], target)
      assert mode(target) == 0o600
      assert :zlib.gunzip(File.read!(target)) == "1\r\n"
    end
  end

  describe "concurrency" do
    test "concurrent atomic exports to the same path produce one complete file", %{dir: dir} do
      target = Path.join(dir, "same.csv")

      results =
        1..20
        |> Task.async_stream(fn i ->
          BeamExport.to_file(Enum.map(1..500, &[i, &1]), target)
        end)
        |> Enum.map(fn {:ok, result} -> elem(result, 0) end)

      assert Enum.all?(results, &(&1 == :ok))
      lines = target |> File.read!() |> String.split("\r\n", trim: true)
      assert length(lines) == 500
      assert lines |> Enum.map(&hd(String.split(&1, ","))) |> Enum.uniq() |> length() == 1
      assert temp_files(dir) == []
    end
  end

  describe "invalid configuration" do
    test "invalid options raise ArgumentError before touching the filesystem", %{dir: dir} do
      target = Path.join(dir, "never.csv")

      assert_raise ArgumentError, ~r/:compress/, fn ->
        BeamExport.to_file([[1]], target, compress: "yes")
      end

      assert_raise ArgumentError, ~r/:chunk_size/, fn ->
        BeamExport.to_file([[1]], target, chunk_size: 0)
      end

      assert_raise ArgumentError, ~r/:file_mode/, fn ->
        BeamExport.to_file([[1]], target, file_mode: 0o1777)
      end

      refute File.exists?(target)
    end
  end
end
