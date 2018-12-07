defmodule Redix.Stream.Consumer do
  @moduledoc """
  ABC
  """

  @type group_name :: String.t()
  @type consumer_name :: String.t()

  @type state :: %{
          redix: Redix.Stream.redix(),
          consumer_group_command_connection: Redix.Stream.redix(),
          stream: Redix.Stream.t(),
          group_name: group_name(),
          consumer_name: consumer_name(),
          handler: function() | Redix.Stream.handler(),
          process_pending: boolean(),
          raise_errors: boolean()
        }

  @default_timeout 0

  @doc """
  Returns child specification when used with a supervisor.
  """
  @spec child_spec(
          {Redix.Stream.redix(), Redix.Stream.t(), function() | Redix.Stream.handler(), keyword()}
        ) :: Supervisor.child_spec()
  def child_spec({redix, stream, handler, opts}) do
    {id, opts_without_id} = Keyword.pop(opts, :id, __MODULE__)

    %{
      id: id,
      start: {__MODULE__, :start_link, [redix, stream, handler, opts_without_id]}
    }
  end

  @doc """
  Starts a new GenServer of `Redix.Stream.Consumer`.
  """
  @spec start_link(
          Redix.Stream.redix(),
          Redix.Stream.t(),
          function() | Redix.Stream.handler(),
          keyword()
        ) :: GenServer.on_start()
  def start_link(redix, stream, handler, opts \\ []) do
    GenServer.start_link(__MODULE__, {redix, stream, handler, opts})
  end

  @doc """
  Initializes a new `Redix.Stream.Consumer`, establishing a long-term stream
  with the given `redis` server.
  """
  @spec init(
          {Redix.Stream.redix(), Redix.Stream.t(), function() | Redix.Stream.handler(), keyword}
        ) :: {:ok, state}
  def init({redix, stream, handler, opts}) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    group_name = Keyword.get(opts, :group_name)
    consumer_name = Keyword.get(opts, :consumer_name)
    consumer_group_command_connection = Keyword.get(opts, :consumer_group_command_connection)
    create_not_exists = Keyword.get(opts, :create_not_exists, true)
    process_pending = Keyword.get(opts, :process_pending, true)
    raise_errors = Keyword.get(opts, :raise_errors, true)

    default_start_pos =
      case group_name do
        nil -> :end_of_stream
        _ -> :last_known_message
      end

    start_pos_given = Keyword.get(opts, :start_pos, default_start_pos)

    start_pos =
      case {group_name, process_pending, start_pos_given} do
        {nil, _, :start_of_stream} -> "0"
        {nil, _, :end_of_stream} -> "$"
        {_, true, _} -> "0"
        {_, false, :start_of_stream} -> "0"
        {_, false, :end_of_stream} -> "$"
        {_, false, :last_known_message} -> "$"
        {_, false, other} -> other
      end

    if consumer_name do
      case Redix.command(redix, ["XGROUP", "CREATE", stream, group_name, start_pos]) do
        {:error, error = %Redix.Error{message: "ERR no such key"}} ->
          if create_not_exists do
            {:ok, _} = Redix.command(redix, ["XADD", stream, "*", "", ""])

            # Recurse without create_not_exists flag set
            init({redix, stream, handler, Keyword.put(opts, :create_not_exists, false)})
          else
            raise error
          end

        {:error, %Redix.Error{message: "BUSYGROUP Consumer Group name already exists"}} ->
          # This is fine, just means the group already exists
          :ok

        {:error, error} ->
          raise error

        {:ok, _} ->
          :ok
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
       handler: handler,
       process_pending: process_pending,
       raise_errors: raise_errors
     }}
  end

  @doc """
  Handles a new message from a stream, dispatching it to the given handler.
  """
  # When we have a consumer group
  def handle_info(
        {:stream_more_data, timeout, start_pos},
        %{
          redix: redix,
          consumer_group_command_connection: consumer_group_command_connection,
          stream: stream,
          group_name: group_name,
          consumer_name: consumer_name,
          handler: handler,
          process_pending: process_pending,
          raise_errors: raise_errors
        } = state
      )
      when not is_nil(group_name) and not is_nil(consumer_name) and
             not is_nil(consumer_group_command_connection) do
    # Wait for a number of messages to come in
    {:ok, stream_results} =
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

    if process_pending && stream_results == [[stream, []]] do
      # If we ran out of results, let's switch from processing
      # pending to most recent.
      stream_more_data(timeout, ">")

      {:noreply, %{state | process_pending: false}}
    else
      # Process the results and get the next positions to consume from
      for [^stream, items] <- stream_results do
        {stream_items, next_pos} = stream_items_to_tuples(items, start_pos)

        # Process the items
        for {id, map} <- Enum.reverse(stream_items) do
          case call_handler(handler, stream, id, map) do
            :ok ->
              # TODO: Should we allow asynchronous ack?

              {:ok, _} =
                Redix.command(consumer_group_command_connection, [
                  "XACK",
                  stream,
                  group_name,
                  id
                ])

            {:error, error} ->
              if raise_errors do
                raise "#{__MODULE__} Error processing #{id}: #{error}\n\nvalues:\n#{inspect(map)}"
              end
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
  end

  # Without a consumer group
  def handle_info(
        {:stream_more_data, timeout, start_pos},
        %{
          redix: redix,
          stream: stream,
          handler: handler
        } = state
      ) do
    # Wait for a number of messages to come in
    {:ok, stream_results} =
      Redix.command(
        redix,
        ["XREAD", "BLOCK", timeout, "STREAMS", stream, start_pos],
        timeout: :infinity
      )

    # Process the results and get the next positions to consume from
    for [^stream, items] <- stream_results do
      {stream_items, next_pos} = stream_items_to_tuples(items, start_pos)

      # Process the items
      for {id, map} <- Enum.reverse(stream_items) do
        call_handler(handler, stream, id, map)
      end

      # And stream more data...
      stream_more_data(timeout, next_pos)
    end

    {:noreply, state}
  end

  @spec call_handler(Redix.Stream.handler(), Redix.Stream.t(), String.t(), %{
          String.t() => String.t()
        }) :: any()
  # Handle sentinel
  defp call_handler(_handler, _stream, _id, %{"" => ""}), do: :ok

  defp call_handler({module, function, args}, stream, id, map) do
    apply(module, function, args ++ [stream, id, map])
  end

  @spec call_handler(function(), Redix.Stream.t(), String.t(), %{
          String.t() => String.t()
        }) :: any()
  defp call_handler(fun, stream, id, map) do
    fun.(stream, id, map)
  end

  @spec stream_items_to_tuples(list(list(String.t() | list(String.t()))), String.t()) ::
          {list({String.t(), list(String.t())}), String.t()}
  defp stream_items_to_tuples(items, start_pos) do
    Enum.reduce(items, {[], start_pos}, fn [id, key_values], {msgs, _next_pos} ->
      map =
        key_values
        |> Enum.chunk_every(2)
        |> Enum.map(fn [a, b] -> {a, b} end)
        |> Enum.into(%{})

      {[{id, map} | msgs], id}
    end)
  end

  @spec stream_more_data(integer(), String.t()) :: :ok
  defp stream_more_data(timeout, next_pos) do
    Process.send_after(self(), {:stream_more_data, timeout, next_pos}, 0)

    :ok
  end
end
