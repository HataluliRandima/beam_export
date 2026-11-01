defmodule BeamExport.Ecto.Streamer do
  @moduledoc """
  Encapsulates Ecto repository query streaming within a supervised streaming resource.
  """

  alias BeamExport.PreloadError

  @default_max_rows 500
  @default_timeout :infinity

  @doc """
  Builds an `Enumerable` stream that runs an Ecto query in a managed transaction.
  """
  @spec build(module(), Ecto.Queryable.t(), keyword()) :: Enumerable.t()
  def build(repo, queryable, opts \\ []) do
    validated_query = validate_query!(queryable)
    max_rows = Keyword.get(opts, :max_rows, @default_max_rows)
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    Stream.resource(
      fn -> start_worker(repo, validated_query, max_rows, timeout) end,
      &next_batch/1,
      &stop_worker/1
    )
  end

  @doc """
  Validates that the query does not contain preloads.
  Raises `BeamExport.PreloadError` if preloaded associations are detected.
  """
  def validate_query!(queryable) do
    query = Ecto.Queryable.to_query(queryable)

    if query.preloads != [] do
      raise PreloadError
    else
      query
    end
  end

  defp start_worker(repo, query, max_rows, timeout) do
    parent = self()
    ref = make_ref()

    pid =
      spawn_link(fn ->
        run_worker_loop(repo, query, max_rows, timeout, parent, ref)
      end)

    # If running inside an Ecto sandbox (e.g. test environment), allow worker process
    allow_sandbox(repo, parent, pid)

    %{pid: pid, ref: ref, repo: repo}
  end

  defp run_worker_loop(repo, query, max_rows, timeout, parent, ref) do
    parent_mon = Process.monitor(parent)
    idle_timeout = worker_idle_timeout(timeout)

    try do
      repo.transaction(
        fn ->
          query
          |> repo.stream(max_rows: max_rows)
          |> Stream.chunk_every(max_rows)
          |> Enum.reduce_while(nil, fn batch, _acc ->
            send(parent, {:ready_for_next, ref})

            receive do
              {:request_batch, ^ref} ->
                send(parent, {:batch, ref, batch})
                {:cont, nil}

              {:halt, ^ref} ->
                {:halt, nil}

              {:DOWN, ^parent_mon, :process, ^parent, _reason} ->
                {:halt, nil}
            after
              idle_timeout ->
                {:halt, nil}
            end
          end)
        end,
        timeout: timeout
      )

      send(parent, {:done, ref})
    rescue
      e ->
        send(parent, {:error, ref, e, __STACKTRACE__})
    end
  end

  defp worker_idle_timeout(timeout) when is_integer(timeout) and timeout > 0, do: timeout
  defp worker_idle_timeout(_other), do: 30_000

  defp next_batch(%{pid: pid, ref: ref} = state) do
    receive do
      {:ready_for_next, ^ref} ->
        send(pid, {:request_batch, ref})

        receive do
          {:batch, ^ref, batch} ->
            {batch, state}

          {:done, ^ref} ->
            {:halt, state}

          {:error, ^ref, error, stack} ->
            reraise error, stack

          {:EXIT, ^pid, reason} ->
            exit(reason)
        end

      {:done, ^ref} ->
        {:halt, state}

      {:error, ^ref, error, stack} ->
        reraise error, stack

      {:EXIT, ^pid, reason} ->
        exit(reason)
    end
  end

  defp stop_worker(%{pid: pid, ref: ref}) do
    if Process.alive?(pid) do
      Process.unlink(pid)
      send(pid, {:halt, ref})

      receive do
        {:done, ^ref} -> :ok
      after
        500 ->
          Process.exit(pid, :kill)
      end
    end

    :ok
  end

  defp allow_sandbox(repo, parent, child) do
    if Code.ensure_loaded?(Ecto.Adapters.SQL.Sandbox) do
      try do
        Ecto.Adapters.SQL.Sandbox.allow(repo, parent, child)
      rescue
        _ -> :ok
      end
    end
  end
end
