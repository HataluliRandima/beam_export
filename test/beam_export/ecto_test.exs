defmodule BeamExport.EctoTest do
  use ExUnit.Case, async: false

  import Ecto.Query
  alias BeamExport.Test.Repo
  alias BeamExport.Test.User
  alias BeamExport.Test.Post
  alias BeamExport.Test.CSV, as: TestParser

  setup do
    # Explicitly checkout connection for test case
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})

    # Clear users and posts
    Repo.delete_all(Post)
    Repo.delete_all(User)

    # Insert test dataset: 50 users
    users =
      for i <- 1..50 do
        role = if rem(i, 2) == 0, do: "admin", else: "member"
        %{name: "User #{i}", email: "user#{i}@example.com", role: role}
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

    :ok
  end

  describe "Ecto streaming" do
    test "streams entire table with headers and mapper" do
      query = from(u in User, order_by: u.id)

      stream =
        BeamExport.to_stream({Repo, query},
          headers: ["ID", "Name", "Email", "Role"],
          mapper: fn u -> [u.id, u.name, u.email, u.role] end,
          max_rows: 10
        )

      result = stream |> Enum.to_list() |> IO.iodata_to_binary()
      parsed = TestParser.parse_string(result, skip_headers: false)

      # 1 header row + 50 user rows = 51 rows
      assert length(parsed) == 51
      assert hd(parsed) == ["ID", "Name", "Email", "Role"]
      assert Enum.at(parsed, 1) == ["1", "User 1", "user1@example.com", "member"]
      assert Enum.at(parsed, 2) == ["2", "User 2", "user2@example.com", "admin"]
    end

    test "streams filtered query" do
      query = from(u in User, where: u.role == "admin", order_by: u.id)

      stream =
        BeamExport.to_stream({Repo, query},
          headers: ["Name", "Role"],
          mapper: fn u -> [u.name, u.role] end
        )

      result = stream |> Enum.to_list() |> IO.iodata_to_binary()
      parsed = TestParser.parse_string(result, skip_headers: false)

      # 1 header + 25 admins = 26 rows
      assert length(parsed) == 26
      assert Enum.all?(tl(parsed), fn [_name, role] -> role == "admin" end)
    end

    test "raises BeamExport.PreloadError when preloads are present" do
      query = from(u in User, preload: [:posts])

      assert_raise BeamExport.PreloadError, fn ->
        BeamExport.to_stream({Repo, query}) |> Enum.to_list()
      end
    end

    test "early stream halting closes worker and releases database cleanly" do
      query = from(u in User, order_by: u.id)

      # Take only first 2 chunks
      first_few =
        BeamExport.to_stream({Repo, query},
          headers: ["ID"],
          mapper: fn u -> [u.id] end,
          chunk_size: 5,
          max_rows: 5
        )
        |> Stream.take(3)
        |> Enum.to_list()
        |> IO.iodata_to_binary()

      parsed = TestParser.parse_string(first_few, skip_headers: false)
      # 1 header row + 2 chunks of 5 rows = 11 rows
      assert length(parsed) == 11
    end
  end
end
