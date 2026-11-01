defmodule BeamExport.Ecto do
  @moduledoc """
  Ecto database stream integration with managed transactions and preload safety.
  """

  @doc """
  Streams an Ecto query using `Repo.stream/2` wrapped inside a managed `Repo.transaction/2`.
  """
  def stream(repo, query, opts \\ []) do
    BeamExport.Ecto.Streamer.build(repo, query, opts)
  end
end

