defmodule BeamExport.Ecto do
  @moduledoc """
  Ecto database stream integration with managed transactions and preload safety.
  """

  @doc """
  Streams an Ecto query. If the caller process is already inside a transaction,
  it streams directly via `Repo.stream/2` to avoid checking out a second connection.
  Otherwise, it wraps the stream in a managed `Repo.transaction/2` resource.
  """
  def stream(repo, query, opts \\ []) do
    if repo.in_transaction?() do
      validated_query = BeamExport.Ecto.Streamer.validate_query!(query)
      max_rows = Keyword.get(opts, :max_rows, 500)
      repo.stream(validated_query, max_rows: max_rows)
    else
      BeamExport.Ecto.Streamer.build(repo, query, opts)
    end
  end
end
