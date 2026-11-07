if Code.ensure_loaded?(Ecto.Queryable) do
  defmodule BeamExport.Ecto.Streamer do
    @moduledoc false
    # Runs an Ecto cursor in a dedicated worker process so that a lazy stream can be
    # consumed outside of a transaction.
    #
    # Protocol (`ref` identifies one stream):
    #
    #   consumer -> worker   {:request_batch, ref}   (one outstanding request at most)
    #   consumer -> worker   {:halt, ref}            (stream halted early)
    #   worker -> consumer   {:batch, ref, rows}     (reply to a request)
    #   worker -> consumer   {:done, ref}            (cursor exhausted AND transaction committed)
    #   worker -> consumer   {:error, ref, kind, reason, stacktrace}
    #
    # Guarantees:
    #
    #   * `{:done, ref}` is sent only after the cursor is exhausted and the transaction
    #     committed. Every other way of stopping (consumer idle, halt, crash) is an
    #     error on the consumer side, so an incomplete export is never reported as a
    #     complete one.
    #   * The processes monitor (not link) each other: a crashing worker becomes an
    #     exception in the consumer, and a dying consumer makes the worker roll back.
    #   * When the stream stops for any reason, the consumer waits for the worker to
    #     exit and removes every protocol message from its mailbox.

    alias BeamExport.{ExportError, PreloadError, TimeoutError}

    @default_max_rows 500
    @default_timeout :infinity
    @default_consumer_timeout 30_000
    @default_batch_timeout 120_000
    @stop_timeout 5_000

    @doc """
    Builds an `Enumerable` that runs an Ecto query in a managed transaction.
    The query is validated eagerly.
    """
    @spec build(module(), Ecto.Queryable.t(), keyword()) :: Enumerable.t()
    def build(repo, queryable, opts) do
      validated_query = validate_query!(queryable)
      max_rows = Keyword.get(opts, :max_rows, @default_max_rows)
      timeout = Keyword.get(opts, :timeout, @default_timeout)
      consumer_timeout = Keyword.get(opts, :consumer_timeout) || @default_consumer_timeout
      batch_timeout = Keyword.get(opts, :batch_timeout, @default_batch_timeout)

      config = %{
        repo: repo,
        query: validated_query,
        max_rows: max_rows,
        timeout: timeout,
        consumer_timeout: consumer_timeout
      }

      Stream.resource(
        fn -> start_worker(config) end,
        &next_batch(&1, batch_timeout),
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

    ## Consumer side

    defp start_worker(config) do
      parent = self()
      ref = make_ref()
      {pid, mon} = spawn_monitor(fn -> worker_init(config, parent, ref) end)
      # The worker does not touch the database before the first request, which is
      # sent after this call, so the sandbox allowance cannot race the first query.
      allow_sandbox(config.repo, parent, pid)
      %{pid: pid, mon: mon, ref: ref, consumer_timeout: config.consumer_timeout}
    end

    defp next_batch(%{pid: pid, mon: mon, ref: ref} = state, batch_timeout) do
      send(pid, {:request_batch, ref})

      receive do
        {:batch, ^ref, rows} ->
          {rows, state}

        {:done, ^ref} ->
          {:halt, state}

        {:error, ^ref, kind, reason, stacktrace} ->
          :erlang.raise(kind, reason, stacktrace)

        {:DOWN, ^mon, :process, ^pid, {:shutdown, :consumer_timeout}} ->
          raise TimeoutError, reason: :consumer_timeout, timeout: state.consumer_timeout

        {:DOWN, ^mon, :process, ^pid, reason} ->
          raise ExportError,
            reason: reason,
            message: "the Ecto stream worker exited unexpectedly: #{inspect(reason)}"
      after
        batch_timeout ->
          Process.exit(pid, :kill)
          raise TimeoutError, reason: :batch_timeout, timeout: batch_timeout
      end
    end

    defp stop_worker(%{pid: pid, mon: mon, ref: ref}) do
      if Process.alive?(pid) do
        send(pid, {:halt, ref})

        receive do
          {:DOWN, ^mon, :process, ^pid, _reason} -> :ok
        after
          @stop_timeout ->
            Process.exit(pid, :kill)

            receive do
              {:DOWN, ^mon, :process, ^pid, _reason} -> :ok
            end
        end
      end

      Process.demonitor(mon, [:flush])
      flush_protocol_messages(ref)
    end

    defp flush_protocol_messages(ref) do
      receive do
        {:batch, ^ref, _rows} -> flush_protocol_messages(ref)
        {:done, ^ref} -> flush_protocol_messages(ref)
        {:error, ^ref, _kind, _reason, _stacktrace} -> flush_protocol_messages(ref)
      after
        0 -> :ok
      end
    end

    ## Worker side

    defp worker_init(config, parent, ref) do
      parent_mon = Process.monitor(parent)

      case await_request(ref, parent, parent_mon, config.consumer_timeout) do
        :ok -> run_cursor(config, parent, ref, parent_mon)
        :halt -> exit(:normal)
        :timeout -> exit({:shutdown, :consumer_timeout})
      end
    end

    defp run_cursor(config, parent, ref, parent_mon) do
      %{repo: repo, query: query, max_rows: max_rows} = config

      result =
        try do
          BeamExport.Ecto.transaction(
            repo,
            fn ->
              outcome =
                query
                |> repo.stream(max_rows: max_rows)
                |> Stream.chunk_every(max_rows)
                |> Enum.reduce_while(:requested, fn batch, pending ->
                  # A request is already pending for the first batch; afterwards the
                  # next batch is prefetched while the consumer works, then held
                  # until the consumer asks for it.
                  wait =
                    if pending == :requested,
                      do: :ok,
                      else: await_request(ref, parent, parent_mon, config.consumer_timeout)

                  case wait do
                    :ok ->
                      send(parent, {:batch, ref, batch})
                      {:cont, :idle}

                    stop ->
                      {:halt, stop}
                  end
                end)

              {:ok, outcome}
            end,
            timeout: config.timeout
          )
        catch
          kind, reason ->
            send(parent, {:error, ref, kind, reason, __STACKTRACE__})
            exit(:normal)
        end

      case result do
        {:ok, outcome} when outcome in [:requested, :idle] ->
          # Cursor exhausted and transaction committed: the only successful outcome.
          send(parent, {:done, ref})

        {:ok, :halt} ->
          :ok

        {:ok, :timeout} ->
          exit({:shutdown, :consumer_timeout})

        {:error, reason} ->
          error =
            ExportError.exception(
              reason: reason,
              message:
                "the export transaction did not commit (#{inspect(reason)}); the export is " <>
                  "INCOMPLETE. A common cause is the :timeout option elapsing, which " <>
                  "bounds how long the export may hold its database connection"
            )

          send(parent, {:error, ref, :error, error, []})
      end
    end

    defp await_request(ref, parent, parent_mon, consumer_timeout) do
      receive do
        {:request_batch, ^ref} -> :ok
        {:halt, ^ref} -> :halt
        {:DOWN, ^parent_mon, :process, ^parent, _reason} -> :halt
      after
        consumer_timeout -> :timeout
      end
    end

    if Code.ensure_loaded?(Ecto.Adapters.SQL.Sandbox) do
      # Only meaningful in tests: lets the worker use the consumer's sandboxed
      # connection. `allow/3` raises for non-sandbox pools, which is ignored.
      defp allow_sandbox(repo, parent, child) do
        Ecto.Adapters.SQL.Sandbox.allow(repo, parent, child)
      rescue
        _ -> :ok
      end
    else
      defp allow_sandbox(_repo, _parent, _child), do: :ok
    end
  end
end
