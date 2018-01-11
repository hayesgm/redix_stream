defmodule Redix.Stream do
  @moduledoc """
  Documentation for Redix.Stream.
  """

  @type redix :: pid() | atom()
  @type t :: String.t

  @doc """
  Produces a new single message in a Redis stream.

  ## Examples

      iex> Redix.Stream.produce(:redix, "topic", "temperature", 55)
  """
  @spec produce(redix, t, String.t, any()) :: {:ok, String.t} | {:error, any()}
  def produce(redix, stream, key, value) do
    case Redix.command(redix, ["XADD", stream, "*", key, value]) do
      {:ok, id} when is_binary(id) -> {:ok, id}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Provides a supervisable specification for a consumer which consumes
  from the given topic or topics.

  ## Examples

      iex> Redix.Stream.consumer(:redix, "topic", fn msg -> msg end)

      iex> Redix.Stream.consumer(:redix, "topic", {Module, :function, [:arg1, :arg2]})
  """
  @spec consumer(redix, t, function() | mfa()) :: Supervisor.Spec.spec
  def consumer(redix, stream, callback) do
    Supervisor.Spec.worker(Redix.Stream.Consumer, [redix, stream, callback])
  end
end
