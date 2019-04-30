defmodule Redix.Stream.ConsumerTest do
  use ExUnit.Case
  doctest Redix.Stream.Consumer

  alias Redix.Stream.{Consumer, ConsumerSup}

  setup_all do
    {:ok, cmd_connection} = Redix.start_link()

    %{
      cmd_connection: cmd_connection
    }
  end

  setup %{cmd_connection: cmd_connection} do
    stream_name = random_string(25)

    on_exit(fn ->
      {:ok, _} = Redix.command(cmd_connection, ["DEL", stream_name])
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
      Consumer.start_link(
        redix_1,
        stream_name,
        fn stream, id, values -> send(pid, {:streamed, stream, id, values}) end
      )

    # allow consumer time to connect
    :timer.sleep(500)

    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_1" => "value_1"})

    assert_receive {:streamed, ^stream_name, _id, %{"key_1" => "value_1"}}, 5_000
  end

  @tag :integration
  test "it should connect and stream multiple messages", %{
    cmd_connection: cmd_connection,
    stream_name: stream_name
  } do
    {:ok, redix_1} = Redix.start_link()
    pid = self()

    {:ok, _pid} =
      Consumer.start_link(
        redix_1,
        stream_name,
        fn stream, id, values ->
          send(pid, {:streamed, stream, id, values})

          :ok
        end
      )

    # allow consumer time to connect
    :timer.sleep(500)

    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_1" => "value_1"})
    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_2" => "value_2"})

    :timer.sleep(500)

    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_3" => "value_3"})

    assert_receive {:streamed, ^stream_name, _id, %{"key_1" => "value_1"}}, 5_000
    assert_receive {:streamed, ^stream_name, _id, %{"key_2" => "value_2"}}
    assert_receive {:streamed, ^stream_name, _id, %{"key_3" => "value_3"}}
  end

  @tag :integration
  test "it stream many messages at once", %{
    cmd_connection: cmd_connection,
    stream_name: stream_name
  } do
    {:ok, redix_1} = Redix.start_link()
    pid = self()

    {:ok, _pid} =
      Consumer.start_link(
        redix_1,
        stream_name,
        fn stream, id, values ->
          # this runs in the consumer and blocks
          # further processing.
          :timer.sleep(100)

          send(pid, {:streamed, stream, id, values})

          :ok
        end
      )

    # allow consumer time to connect
    :timer.sleep(500)

    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_1" => "value_1"})
    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_2" => "value_2"})
    {:ok, _msg_id} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_3" => "value_3"})

    assert_receive {:streamed, ^stream_name, _id, %{"key_1" => "value_1"}}, 5_000
    assert_receive {:streamed, ^stream_name, _id, %{"key_2" => "value_2"}}, 5_000
    assert_receive {:streamed, ^stream_name, _id, %{"key_3" => "value_3"}}, 5_000
  end

  @tag :integration
  test "it streams messages to consumer groups but not the same message to multiple groups", %{
    cmd_connection: cmd_connection,
    stream_name: stream_name
  } do
    {:ok, redix_1} = Redix.start_link()
    {:ok, redix_2} = Redix.start_link()
    {:ok, redix_3} = Redix.start_link()
    pid = self()

    {:ok, _pid} =
      Consumer.start_link(
        redix_1,
        stream_name,
        fn stream, id, values ->
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
      Consumer.start_link(
        redix_2,
        stream_name,
        fn stream, id, values ->
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

    {:ok, msg_id1} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_1" => "value_1"})
    :timer.sleep(100)
    {:ok, msg_id2} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_2" => "value_2"})
    :timer.sleep(100)
    {:ok, msg_id3} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_3" => "value_3"})

    assert_receive {consumer1, ^stream_name, ^msg_id1, %{"key_1" => "value_1"}}, 5_000
    assert_receive {consumer2, ^stream_name, ^msg_id2, %{"key_2" => "value_2"}}, 5_000
    assert_receive {consumer3, ^stream_name, ^msg_id3, %{"key_3" => "value_3"}}, 5_000

    # Make sure each consumer handles at least one message
    assert Enum.member?([consumer1, consumer2, consumer3], :streamed_consumer1)
    assert Enum.member?([consumer1, consumer2, consumer3], :streamed_consumer2)
  end

  @tag :integration
  test "a consumer group should pick up where it left off in a stream", %{
    cmd_connection: cmd_connection,
    stream_name: stream_name
  } do
    {:ok, redix_1} = Redix.start_link()
    pid = self()

    {:ok, agent} = Agent.start_link(fn -> 1 end)

    handler = fn stream, id, values ->
      ctr = Agent.get_and_update(agent, fn state -> {state, state + 1} end)

      # Fail on second and third message processing
      case ctr do
        2 ->
          # Fail hard processing second value
          throw("Error processing id=#{id}, values=#{inspect(values)}")

        3 ->
          # Fail soft processing third value
          {:error, "Error processing id=#{id}, values=#{inspect(values)}"}

        _ ->
          # Otherwise, good
          send(pid, {:streamed, stream, id, values})

          :ok
      end
    end

    {:ok, _sup} =
      ConsumerSup.start_link(
        redix_1,
        stream_name,
        handler,
        group_name: "test",
        consumer_name: "consumer1",
        consumer_group_command_connection: redix_1
      )

    {:ok, _msg_id1} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_1" => "value_1"})
    {:ok, _msg_id2} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_2" => "value_2"})
    {:ok, _msg_id3} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_3" => "value_3"})
    {:ok, _msg_id4} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_4" => "value_4"})
    {:ok, _msg_id5} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_5" => "value_5"})

    assert_receive {:streamed, ^stream_name, _id, %{"key_1" => "value_1"}}, 500
    assert_receive {:streamed, ^stream_name, _id, %{"key_2" => "value_2"}}, 500
    assert_receive {:streamed, ^stream_name, _id, %{"key_3" => "value_3"}}
    assert_receive {:streamed, ^stream_name, _id, %{"key_4" => "value_4"}}
    assert_receive {:streamed, ^stream_name, _id, %{"key_5" => "value_5"}}

    # Make sure we didn't restream any data
    refute_receive {:streamed, ^stream_name, _id, %{"key_1" => "value_1"}}
    refute_receive {:streamed, ^stream_name, _id, %{"key_2" => "value_2"}}
    refute_receive {:streamed, ^stream_name, _id, %{"key_3" => "value_3"}}
    refute_receive {:streamed, ^stream_name, _id, %{"key_4" => "value_4"}}
    refute_receive {:streamed, ^stream_name, _id, %{"key_5" => "value_5"}}
  end

  @tag :integration
  test "a consumer group can read starting from end of stream", %{
    cmd_connection: cmd_connection,
    stream_name: stream_name
  } do
    {:ok, redix_1} = Redix.start_link()
    pid = self()

    {:ok, _msg_id1} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_1" => "value_1"})
    {:ok, _msg_id2} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_2" => "value_2"})

    {:ok, _sup} =
      ConsumerSup.start_link(
        redix_1,
        stream_name,
        fn stream, id, values ->
          # this runs in the consumer and blocks
          # further processing.
          :timer.sleep(100)

          send(pid, {:streamed, stream, id, values})

          :ok
        end,
        group_name: "test",
        consumer_name: "consumer1",
        consumer_group_command_connection: redix_1,
        process_pending: true,
        start_pos: :end_of_stream
      )

    {:ok, _msg_id3} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_3" => "value_3"})
    {:ok, _msg_id4} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_4" => "value_4"})
    {:ok, _msg_id5} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_5" => "value_5"})

    assert_receive {:streamed, ^stream_name, _id, %{"key_3" => "value_3"}}, 500
    assert_receive {:streamed, ^stream_name, _id, %{"key_4" => "value_4"}}, 500
    assert_receive {:streamed, ^stream_name, _id, %{"key_5" => "value_5"}}, 500

    # Make sure we didn't restream any data
    refute_receive {:streamed, ^stream_name, _id, %{"key_3" => "value_3"}}
    refute_receive {:streamed, ^stream_name, _id, %{"key_4" => "value_4"}}
    refute_receive {:streamed, ^stream_name, _id, %{"key_5" => "value_5"}}
  end

  @tag :integration
  test "it allows processing to finish before shutdown", %{
    cmd_connection: cmd_connection,
    stream_name: stream_name
  } do
    {:ok, redix_1} = Redix.start_link()
    pid = self()

    handler = fn stream, id, values ->
      Process.sleep(1000)

      send(pid, {:streamed, stream, id, values})

      :ok
    end

    {:ok, sup} =
      ConsumerSup.start_link(
        redix_1,
        stream_name,
        handler,
        group_name: "test",
        consumer_name: "consumer1",
        consumer_group_command_connection: redix_1,
        sup_timeout: 1100
      )

    {:ok, _msg_id1} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_1" => "value_1"})
    {:ok, _msg_id2} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_2" => "value_2"})
    {:ok, _msg_id3} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_3" => "value_3"})
    {:ok, _msg_id4} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_4" => "value_4"})
    {:ok, _msg_id5} = Redix.Stream.produce(cmd_connection, stream_name, %{"key_5" => "value_5"})

    assert_receive {:streamed, ^stream_name, _id, %{"key_1" => "value_1"}}, 1100

    # Let the second one start processing
    Process.sleep(500)

    # Kill our supervisor before it can finish processing the next message
    Process.unlink(sup)
    Process.exit(sup, :normal)

    refute_receive {:streamed, ^stream_name, _id, %{"key_2" => "value_2"}}, 0
    refute_receive {:streamed, ^stream_name, _id, %{"key_3" => "value_3"}}, 0
    refute_receive {:streamed, ^stream_name, _id, %{"key_4" => "value_4"}}, 0
    refute_receive {:streamed, ^stream_name, _id, %{"key_5" => "value_5"}}, 0

    # Give our supervisor time to kill the consumer
    Process.sleep(2000)

    assert_receive {:streamed, ^stream_name, _id, %{"key_2" => "value_2"}}, 0
    refute_receive {:streamed, ^stream_name, _id, %{"key_3" => "value_3"}}, 0
    refute_receive {:streamed, ^stream_name, _id, %{"key_4" => "value_4"}}, 0
    refute_receive {:streamed, ^stream_name, _id, %{"key_5" => "value_5"}}, 0

    Process.sleep(2000)

    # Key 3 may or may not have been processed
    refute_receive {:streamed, ^stream_name, _id, %{"key_3" => "value_3"}}, 0
    refute_receive {:streamed, ^stream_name, _id, %{"key_4" => "value_4"}}, 0
    refute_receive {:streamed, ^stream_name, _id, %{"key_5" => "value_5"}}, 0

    # Now, finish processing

    {:ok, _sup} =
      ConsumerSup.start_link(
        redix_1,
        stream_name,
        handler,
        group_name: "test",
        consumer_name: "consumer1",
        consumer_group_command_connection: redix_1
      )

    Process.sleep(5000)

    assert_receive {:streamed, ^stream_name, _id, %{"key_3" => "value_3"}}, 0
    assert_receive {:streamed, ^stream_name, _id, %{"key_4" => "value_4"}}, 0
    assert_receive {:streamed, ^stream_name, _id, %{"key_5" => "value_5"}}, 0
  end

  def random_string(length) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64() |> binary_part(0, length)
  end
end
