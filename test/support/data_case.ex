defmodule BeamExport.DataCase do
  @moduledoc false
  # Test case for tests that touch the database.
  #
  # Each test gets its own sandbox owner process. The sandbox stays in :manual mode,
  # so processes BeamExport spawns must be allowed explicitly, which exercises the
  # library's sandbox allowance instead of hiding it behind global shared mode.
  use ExUnit.CaseTemplate

  using do
    quote do
      import Ecto.Query
      import BeamExport.DataCase, only: [insert_users: 1, insert_users: 2]
      alias BeamExport.Test.Repo
      alias BeamExport.Test.User
      alias BeamExport.Test.Post
    end
  end

  setup _tags do
    owner = Ecto.Adapters.SQL.Sandbox.start_owner!(BeamExport.Test.Repo, shared: false)
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(owner) end)
    :ok
  end

  @doc "Inserts `count` users named `User 1`..`User N`."
  def insert_users(count, attrs \\ fn _i -> %{} end) do
    now = ~N[2026-01-01 00:00:00]

    rows =
      for i <- 1..count//1 do
        Map.merge(
          %{
            name: "User #{i}",
            email: "user#{i}@example.com",
            role: if(rem(i, 2) == 0, do: "admin", else: "member"),
            inserted_at: now,
            updated_at: now
          },
          attrs.(i)
        )
      end

    rows
    |> Enum.chunk_every(1_000)
    |> Enum.each(&BeamExport.Test.Repo.insert_all(BeamExport.Test.User, &1))
  end
end
