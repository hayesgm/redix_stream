defmodule Redix.Stream.Consumer do
  @moduledoc """
  """

  @type state :: %{
          redix: Redix.Stream.redix(),
          stream: Redix.Stream.t(),
          handler: function() | mfa()
        }

  @default_timeout 0

  @doc """
  Starts a new GenServer of `Redix.Stream.Consumer`.
  """
  @spec start_link(Redix.Stream.redix(), Redix.Stream.t(), function() | mfa(), keyword()) ::
          Supervisor.Spec.spec()
  def start_link(redix, stream, handler, opts \\ []) do
    GenServer.start_link(__MODULE__, {redix, stream, handler, opts})
  end

  @doc """
  Initializes a new `Redix.Stream.Consumer`, establishing a long-term
  stream with the given `redis` server.
  """
  @spec init({Redix.Stream.redix(), Redix.Stream.t(), function() | mfa(), keyword}) ::
          {:ok, state}
  def init({redix, stream, handler, opts}) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    default_start_pos = Keyword.get(opts, :start_pos, "$")
    tracker = Keyword.get(opts, :tracker, nil)

    # If tracker specified, load our starting position
    start_pos =
      if tracker do
        case Redix.command!(redix, ["GET", "stream_tracker:#{tracker}"]) do
          nil -> default_start_pos
          pos -> pos
        end
      else
        default_start_pos
      end

    stream_more_data(timeout, start_pos)

    {:ok, %{redix: redix, stream: stream, tracker: tracker, handler: handler}}
  end

  @doc """
  Handles a new message from a stream, dispatching it to the given handler.
  """
  def handle_info(
        {:stream_more_data, timeout, start_pos},
        %{redix: redix, stream: stream, handler: handler, tracker: tracker} = state
      ) do
    # Wait for a number of messages to come in
    {:ok, stream_results} =
      Redix.command(
        redix,
        ["XREAD", "BLOCK", timeout, "STREAMS", stream, start_pos],
        timeout: :infinity
      )

    # Process the results and get the next positions to consume from
    for stream_result <- stream_results do
      [^stream, items] = stream_result

      {stream_items, next_pos} =
        Enum.reduce(items, {[], start_pos}, fn [id, kvs], {msgs, _next_pos} ->
          {[{id, kvs} | msgs], id}
        end)

      # Process the items
      for stream_item <- stream_items |> Enum.reverse() do
        call_handler(handler, stream, stream_item)
      end

      # If tracker specified, track our current position
      if tracker do
        Redix.command!(redix, ["SET", "stream_tracker:#{tracker}", next_pos])
      end

      # And stream more data...
      stream_more_data(timeout, next_pos)
    end

    {:noreply, state}
  end

  @spec call_handler(mfa(), Redix.Stream.t(), any()) :: any()
  defp call_handler({module, function, args}, stream, msg) do
    apply(module, function, args ++ [stream, msg])
  end

  @spec call_handler(function(), Redix.Stream.t(), any()) :: any()
  defp call_handler(fun, stream, msg) do
    fun.(stream, msg)
  end

  @spec stream_more_data(integer(), String.t()) :: :ok
  defp stream_more_data(timeout, next_pos) do
    Process.send_after(self(), {:stream_more_data, timeout, next_pos}, 0)

    :ok
  end
end
