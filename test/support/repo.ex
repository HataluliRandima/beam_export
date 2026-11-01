defmodule BeamExport.Test.Repo do
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
