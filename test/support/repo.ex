defmodule BeamExport.Test.Repo do
  use Ecto.Repo,
    otp_app: :beam_export,
    adapter: Ecto.Adapters.SQLite3
end

defmodule BeamExport.Test.PlainRepo do
  # A repo with a regular (non-sandbox) connection pool and its own database, like a
  # production repo. Used for tests that must not disturb the shared sandbox, such
  # as tests that make DBConnection drop a connection. Seeded with 30 users.
  use Ecto.Repo,
    otp_app: :beam_export,
    adapter: Ecto.Adapters.SQLite3
end

defmodule BeamExport.Test.Migration do
  use Ecto.Migration

  def change do
    create table(:users) do
      add(:name, :string)
      add(:email, :string)
      add(:role, :string)

      timestamps()
    end

    create table(:posts) do
      add(:title, :string)
      add(:user_id, references(:users))

      timestamps()
    end
  end
end
