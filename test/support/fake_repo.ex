defmodule BeamExport.Test.FakeRepo do
  @moduledoc false
  # Minimal stand-in for an Ecto repo, used to drive the stream worker into states
  # that a real database cannot produce on demand (hangs, crashes, exits).
  #
  # It deliberately does NOT define `transact/2`, so it also covers the
  # `transaction/2` fallback used for Ecto < 3.13.
  #
  #     defmodule HangingRepo do
  #       use BeamExport.Test.FakeRepo
  #       def stream(_query, _opts), do: Stream.repeatedly(fn -> Process.sleep(:infinity) end)
  #     end

  defmacro __using__(_opts) do
    quote do
      def in_transaction?, do: Process.get({__MODULE__, :in_transaction}, false)
      def config, do: []

      def transaction(fun, _opts) do
        Process.put({__MODULE__, :in_transaction}, true)
        BeamExport.Test.FakeRepo.notify({:worker, self()})

        try do
          {:ok, fun.()}
        catch
          :throw, {:fake_rollback, value} -> {:error, value}
        after
          Process.delete({__MODULE__, :in_transaction})
        end
      end

      def rollback(value), do: throw({:fake_rollback, value})

      def stream(_query, _opts), do: []
      defoverridable stream: 2, transaction: 2
    end
  end

  @doc "Registers the current process to receive worker notifications."
  def listen, do: :persistent_term.put({__MODULE__, :listener}, self())

  @doc false
  def notify(message) do
    case :persistent_term.get({__MODULE__, :listener}, nil) do
      pid when is_pid(pid) -> send(pid, message)
      nil -> :ok
    end
  end
end
