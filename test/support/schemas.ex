defmodule BeamExport.Test.User do
  use Ecto.Schema

  schema "users" do
    field(:name, :string)
    field(:email, :string)
    field(:role, :string)
    has_many(:posts, BeamExport.Test.Post)

    timestamps()
  end
end

defmodule BeamExport.Test.Post do
  use Ecto.Schema

  schema "posts" do
    field(:title, :string)
    belongs_to(:user, BeamExport.Test.User)

    timestamps()
  end
end
