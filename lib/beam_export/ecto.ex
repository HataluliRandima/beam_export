defmodule BeamExport.Ecto do
  @moduledoc false
  # Ecto integration: source detection, row streaming and transaction handling.
  #
  # Two execution models exist, deliberately:
  #
  #   * Eager sinks (`to_file/3`, `to_iodata/2`) run the cursor inside a transaction
  #     owned by the *calling* process (`run/3`). No helper process is involved.
  #   * The lazy `to_stream/2` cannot own a transaction across the caller's
  #     enumeration, so outside a transaction it delegates to
  #     `BeamExport.Ecto.Streamer`, which runs the cursor in a monitored worker.

  @ecto_available Code.ensure_loaded?(Ecto.Queryable)

  @doc """
  Returns `true` when `source` is a `{repo, queryable}` tuple.

  Raises `ArgumentError` for sources that are neither enumerables nor valid
  Ecto sources, so mistakes such as `{:ok, rows}` fail fast with a clear message.
  """
  def ecto_source?({repo, _queryable} = source) when is_atom(repo) do
    cond do
      not Code.ensure_loaded?(repo) ->
        raise ArgumentError,
              "invalid export source #{inspect(source, limit: 3)}: #{inspect(repo)} is not " <>
                "an available module. Pass an Enumerable or a {Repo, queryable} tuple"

      not (function_exported?(repo, :stream, 2) and function_exported?(repo, :in_transaction?, 0)) ->
        raise ArgumentError,
              "invalid export source: #{inspect(repo)} is not an Ecto repository " <>
                "(it does not export stream/2 and in_transaction?/0)"

      true ->
        ecto_required!()
    end
  end

  def ecto_source?(source) do
    if Enumerable.impl_for(source) do
      false
    else
      raise ArgumentError,
            "invalid export source of type #{type_name(source)}: expected an Enumerable " <>
              "or a {Repo, queryable} tuple"
    end
  end

  @doc """
  Returns an enumerable of records for `source`.

  For Ecto sources, the query is validated eagerly (raising `BeamExport.PreloadError`
  before any side effect). Inside an existing transaction the repo cursor is used
  directly; otherwise a managed worker owns the transaction.
  """
  def rows(source, opts) do
    if ecto_source?(source), do: ecto_rows(source, opts), else: source
  end

  @doc """
  Runs `fun` inside a transaction owned by the caller when `source` is an Ecto source
  and the caller is not already in a transaction; otherwise runs `fun` directly.

  `fun` must return `{:ok, value}` or `{:error, exception, stacktrace}`. When it
  returns an error the transaction is rolled back. Returns the same shapes.
  """
  def run(source, opts, fun) do
    if ecto_source?(source) do
      {repo, _query} = source
      if repo.in_transaction?(), do: fun.(), else: run_in_transaction(repo, opts, fun)
    else
      fun.()
    end
  end

  defp run_in_transaction(repo, opts, fun) do
    timeout = Keyword.get(opts, :timeout, :infinity)

    wrapped = fn ->
      case fun.() do
        {:ok, value} -> {:ok, value}
        {:error, exception, stacktrace} -> {:error, {__MODULE__, exception, stacktrace}}
      end
    end

    case transaction(repo, wrapped, timeout: timeout) do
      {:ok, value} ->
        {:ok, value}

      {:error, {__MODULE__, exception, stacktrace}} ->
        {:error, exception, stacktrace}

      {:error, other} ->
        {:error,
         BeamExport.ExportError.exception(
           reason: other,
           message: "export transaction was rolled back: #{inspect(other)}"
         ), []}
    end
  end

  @doc """
  Runs `fun` (returning `{:ok, value}` or `{:error, reason}`) in a transaction,
  rolling back on `{:error, reason}`.

  Uses `c:Ecto.Repo.transact/2` when the repo provides it (Ecto >= 3.13, where
  `transaction/2` is soft-deprecated) and falls back to `transaction/2`.
  """
  def transaction(repo, fun, opts) do
    if function_exported?(repo, :transact, 2) do
      repo.transact(fun, opts)
    else
      repo.transaction(
        fn ->
          case fun.() do
            {:ok, value} -> value
            {:error, reason} -> repo.rollback(reason)
          end
        end,
        opts
      )
    end
  end

  if @ecto_available do
    defp ecto_required!, do: true
  else
    defp ecto_required!, do: raise(ArgumentError, "Ecto sources require the :ecto dependency")
  end

  if @ecto_available do
    defp ecto_rows({repo, query}, opts) do
      if repo.in_transaction?() do
        validated_query = BeamExport.Ecto.Streamer.validate_query!(query)
        repo.stream(validated_query, max_rows: Keyword.get(opts, :max_rows, 500))
      else
        BeamExport.Ecto.Streamer.build(repo, query, opts)
      end
    end
  else
    defp ecto_rows(_source, _opts), do: raise(ArgumentError, "Ecto sources require :ecto")
  end

  @doc false
  def type_name(nil), do: "nil"
  def type_name(value) when is_boolean(value), do: "boolean"
  def type_name(value) when is_list(value), do: "list"
  def type_name(%module{}), do: "struct #{inspect(module)}"
  def type_name(value) when is_map(value), do: "map"
  def type_name(value) when is_tuple(value), do: "tuple"
  def type_name(value) when is_binary(value), do: "string"
  def type_name(value) when is_bitstring(value), do: "bitstring"
  def type_name(value) when is_atom(value), do: "atom"
  def type_name(value) when is_integer(value), do: "integer"
  def type_name(value) when is_float(value), do: "float"
  def type_name(value) when is_function(value), do: "function"
  def type_name(value) when is_pid(value), do: "pid"
  def type_name(value) when is_reference(value), do: "reference"
  def type_name(value) when is_port(value), do: "port"
end
