defmodule Redix.Stream.Consumer do
  @moduledoc """
  """

  @type group_name :: String.t()
  @type consumer_name :: String.t()

  @type state :: %{
          redix: Redix.Stream.redix(),
          consumer_group_command_connection: Redix.Stream.redix(),
          stream: Redix.Stream.t(),
          group_name: group_name(),
          consumer_name: consumer_name(),
          handler: function() | mfa()
        }

  @default_timeout 0

  @doc """
  Starts a new GenServer of `Redix.Stream.Consumer`.
  """
  @spec start_link(Redix.Stream.redix(), Redix.Stream.t(), function() | mfa(), keyword()) ::
          GenServer.on_start()
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
    group_name = Keyword.get(opts, :group_name)
    consumer_name = Keyword.get(opts, :consumer_name)
    consumer_group_command_connection = Keyword.get(opts, :consumer_group_command_connection)

    default_start_pos =
      case group_name do
        nil -> "$"
        _ -> ">"
      end

    start_pos = Keyword.get(opts, :start_pos, default_start_pos)

    if consumer_name do
      try do
        start_pos =
          case start_pos do
            "$" -> "$"
            ">" -> "$"
            other -> other
          end

        Redix.command(redix, ["XGROUP", "CREATE", stream, group_name, start_pos])
      rescue
        e in Redix.Error ->
          case e.message do
            "BUSYGROUP Consumer Group name already exists" -> nil
            _ -> raise e
          end
      end
    end

    stream_more_data(timeout, start_pos)

    {:ok,
     %{
       redix: redix,
       consumer_group_command_connection: consumer_group_command_connection,
       stream: stream,
       group_name: group_name,
       consumer_name: consumer_name,
       handler: handler
     }}
  end

  @doc """
  Handles a new message from a stream, dispatching it to the given handler.
  """
  def handle_info(
        {:stream_more_data, timeout, start_pos},
        %{
          redix: redix,
          consumer_group_command_connection: consumer_group_command_connection,
          stream: stream,
          group_name: group_name,
          consumer_name: consumer_name,
          handler: handler
        } = state
      ) do
    # Wait for a number of messages to come in
    {:ok, stream_results} =
      case {group_name, consumer_name} do
        {nil, nil} ->
          Redix.command(
            redix,
            ["XREAD", "BLOCK", timeout, "STREAMS", stream, start_pos],
            timeout: :infinity
          )

        {group_name, consumer_name} ->
          Redix.command(
            redix,
            [
              "XREADGROUP",
              "GROUP",
              group_name,
              consumer_name,
              "BLOCK",
              timeout,
              "STREAMS",
              stream,
              start_pos
            ],
            timeout: :infinity
          )
      end

    # Process the results and get the next positions to consume from
    for stream_result <- stream_results do
      [^stream, items] = stream_result

      {stream_items, next_pos} =
        Enum.reduce(items, {[], start_pos}, fn [id, kvs], {msgs, _next_pos} ->
          {[{id, kvs} | msgs], id}
        end)

      # Process the items
      for stream_item = {id, _} <- stream_items |> Enum.reverse() do
        case call_handler(handler, stream, stream_item) do
          :ok ->
            {:ok, _} =
              Redix.command(
                consumer_group_command_connection,
                ~w(XACK #{stream} #{group_name} #{id})
              )

          _ ->
            nil
        end
      end

      next_pos =
        case start_pos do
          ">" -> ">"
          _ -> next_pos
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
