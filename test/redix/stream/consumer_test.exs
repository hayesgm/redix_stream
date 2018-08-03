defmodule Redix.Stream.ConsumerTest do
  use ExUnit.Case

  doctest Redix.Stream.Consumer

  setup_all do
    {:ok, cmd_connection} = Redix.start_link()

    %{
      cmd_connection: cmd_connection
    }
  end

  setup %{cmd_connection: cmd_connection} do
    stream_name = random_string(25)

    on_exit(fn ->
      {:ok, 1} = Redix.command(cmd_connection, ["DEL", stream_name])
    end)

    %{
      stream_name: stream_name,
      cmd_connection: cmd_connection
    }
  end

  @tag :integration
  test "it should connect and stream a single message", %{
    cmd_connection: cmd_connection,
    stream_name: stream_name
  } do
    {:ok, redix_1} = Redix.start_link()
    pid = self()

    {:ok, _pid} =
      Redix.Stream.Consumer.start_link(
        redix_1,
        stream_name,
        fn stream, {id, values} -> send(pid, {:streamed, stream, id, values}) end
      )

    # allow consumer time to connect
    :timer.sleep(500)

    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, "key_1", "value_1")

    assert_receive {:streamed, ^stream_name, _id, ["key_1", "value_1"]}, 5_000
  end

  @tag :integration
  test "it should connect and stream multiple messages", %{
    cmd_connection: cmd_connection,
    stream_name: stream_name
  } do
    {:ok, redix_1} = Redix.start_link()
    pid = self()

    {:ok, _pid} =
      Redix.Stream.Consumer.start_link(
        redix_1,
        stream_name,
        fn stream, {id, values} -> send(pid, {:streamed, stream, id, values}) end
      )

    # allow consumer time to connect
    :timer.sleep(500)

    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, "key_1", "value_1")
    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, "key_2", "value_2")
    :timer.sleep(500)
    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, "key_3", "value_3")

    assert_receive {:streamed, ^stream_name, _id, ["key_1", "value_1"]}, 5_000
    assert_receive {:streamed, ^stream_name, _id, ["key_2", "value_2"]}
    assert_receive {:streamed, ^stream_name, _id, ["key_3", "value_3"]}
  end

  @tag :integration
  test "it stream many messages at once", %{
    cmd_connection: cmd_connection,
    stream_name: stream_name
  } do
    {:ok, redix_1} = Redix.start_link()
    pid = self()

    {:ok, _pid} =
      Redix.Stream.Consumer.start_link(
        redix_1,
        stream_name,
        fn stream, {id, values} ->
          # this runs in the consumer and blocks
          :timer.sleep(100)
          # further processing.

          send(pid, {:streamed, stream, id, values})
        end
      )

    # allow consumer time to connect
    :timer.sleep(500)

    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, "key_1", "value_1")
    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, "key_2", "value_2")
    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, "key_3", "value_3")

    assert_receive {:streamed, ^stream_name, _id, ["key_1", "value_1"]}, 5_000
    assert_receive {:streamed, ^stream_name, _id, ["key_2", "value_2"]}, 5_000
    assert_receive {:streamed, ^stream_name, _id, ["key_3", "value_3"]}, 5_000
  end

  @tag :integration
  test "it streams messages to consumer groups but not the same message to multiple groups", %{
    cmd_connection: cmd_connection,
    stream_name: stream_name
  } do
    # A stream must exist before a consumer group can be created
    # https://github.com/antirez/redis/issues/4824
    {:ok, _} = Redix.Stream.produce(cmd_connection, stream_name, "initial", "initial_value")
    {:ok, redix_1} = Redix.start_link()
    {:ok, redix_2} = Redix.start_link()
    {:ok, redix_3} = Redix.start_link()
    pid = self()

    {:ok, _pid} =
      Redix.Stream.Consumer.start_link(
        redix_1,
        stream_name,
        fn stream, {id, values} ->
          # this runs in the consumer and blocks
          :timer.sleep(100)
          # further processing.

          send(pid, {:streamed_consumer1, stream, id, values})

          :ok
        end,
        group_name: "test",
        consumer_name: "consumer1",
        consumer_group_command_connection: redix_3
      )

    {:ok, _pid} =
      Redix.Stream.Consumer.start_link(
        redix_2,
        stream_name,
        fn stream, {id, values} ->
          # this runs in the consumer and blocks
          :timer.sleep(100)
          # further processing.

          send(pid, {:streamed_consumer2, stream, id, values})

          :ok
        end,
        group_name: "test",
        consumer_name: "consumer2",
        consumer_group_command_connection: redix_3
      )

    {:ok, msg_id1} = Redix.Stream.produce(cmd_connection, stream_name, "key_1", "value_1")
    :timer.sleep(100)
    {:ok, msg_id2} = Redix.Stream.produce(cmd_connection, stream_name, "key_2", "value_2")
    :timer.sleep(100)
    {:ok, msg_id3} = Redix.Stream.produce(cmd_connection, stream_name, "key_3", "value_3")

    assert_receive {consumer1, ^stream_name, ^msg_id1, ["key_1", "value_1"]}, 5_000
    assert_receive {consumer2, ^stream_name, ^msg_id2, ["key_2", "value_2"]}, 5_000
    assert_receive {consumer3, ^stream_name, ^msg_id3, ["key_3", "value_3"]}, 5_000

    # Make sure each consumer handles at least one message
    assert Enum.member?([consumer1, consumer2, consumer3], :streamed_consumer1)
    assert Enum.member?([consumer1, consumer2, consumer3], :streamed_consumer2)
  end

  def random_string(length) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64() |> binary_part(0, length)
  end
end
