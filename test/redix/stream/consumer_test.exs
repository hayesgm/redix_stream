defmodule Redix.Stream.ConsumerTest do
  use ExUnit.Case
  doctest Redix.Stream.Consumer

  @test_stream "test_stream"

  @tag :integration
  test "it should connect and stream a single message" do
    {:ok, redix_1} = Redix.start_link()
    {:ok, redix_2} = Redix.start_link()
    pid = self()

    {:ok, _pid} =
      Redix.Stream.Consumer.start_link(
        redix_1,
        @test_stream,
        fn stream, {id, values} -> send(pid, {:streamed, stream, id, values}) end
      )

    # allow consumer time to connect
    :timer.sleep(500)

    {:ok, _msg_id} = Redix.Stream.produce(redix_2, @test_stream, "key_1", "value_1")

    assert_receive {:streamed, @test_stream, _id, ["key_1", "value_1"]}, 5_000
  end

  @tag :integration
  test "it should connect and stream multiple messages" do
    {:ok, redix_1} = Redix.start_link()
    {:ok, redix_2} = Redix.start_link()
    pid = self()

    {:ok, _pid} =
      Redix.Stream.Consumer.start_link(
        redix_1,
        @test_stream,
        fn stream, {id, values} -> send(pid, {:streamed, stream, id, values}) end
      )

    # allow consumer time to connect
    :timer.sleep(500)

    {:ok, _msg_id} = Redix.Stream.produce(redix_2, @test_stream, "key_1", "value_1")
    {:ok, _msg_id} = Redix.Stream.produce(redix_2, @test_stream, "key_2", "value_2")
    :timer.sleep(500)
    {:ok, _msg_id} = Redix.Stream.produce(redix_2, @test_stream, "key_3", "value_3")

    assert_receive {:streamed, @test_stream, _id, ["key_1", "value_1"]}, 5_000
    assert_receive {:streamed, @test_stream, _id, ["key_2", "value_2"]}
    assert_receive {:streamed, @test_stream, _id, ["key_3", "value_3"]}
  end

  @tag :integration
  test "it stream many messages at once" do
    {:ok, redix_1} = Redix.start_link()
    {:ok, redix_2} = Redix.start_link()
    pid = self()

    {:ok, _pid} =
      Redix.Stream.Consumer.start_link(
        redix_1,
        @test_stream,
        fn stream, {id, values} ->
          # this runs in the consumer and blocks
          :timer.sleep(100)
          # further processing.

          send(pid, {:streamed, stream, id, values})
        end
      )

    # allow consumer time to connect
    :timer.sleep(500)

    {:ok, _msg_id} = Redix.Stream.produce(redix_2, @test_stream, "key_1", "value_1")
    {:ok, _msg_id} = Redix.Stream.produce(redix_2, @test_stream, "key_2", "value_2")
    {:ok, _msg_id} = Redix.Stream.produce(redix_2, @test_stream, "key_3", "value_3")

    assert_receive {:streamed, @test_stream, _id, ["key_1", "value_1"]}, 5_000
    assert_receive {:streamed, @test_stream, _id, ["key_2", "value_2"]}, 5_000
    assert_receive {:streamed, @test_stream, _id, ["key_3", "value_3"]}, 5_000
  end

  @tag :integration
  test "it streams messages to consumer groups but not the same message to multiple groups" do
    {:ok, redix_1} = Redix.start_link()
    {:ok, redix_2} = Redix.start_link()
    {:ok, redix_3} = Redix.start_link()
    {:ok, redix_4} = Redix.start_link()
    pid = self()

    {:ok, _pid} =
      Redix.Stream.Consumer.start_link(
        redix_1,
        @test_stream,
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
        @test_stream,
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

    # allow consumer time to connect
    :timer.sleep(500)

    {:ok, msg_id1} = Redix.Stream.produce(redix_4, @test_stream, "key_1", "value_1")
    {:ok, msg_id2} = Redix.Stream.produce(redix_4, @test_stream, "key_2", "value_2")
    {:ok, msg_id3} = Redix.Stream.produce(redix_4, @test_stream, "key_3", "value_3")
    {:ok, msg_id4} = Redix.Stream.produce(redix_4, @test_stream, "key_4", "value_4")
    {:ok, msg_id5} = Redix.Stream.produce(redix_4, @test_stream, "key_5", "value_5")
    {:ok, msg_id6} = Redix.Stream.produce(redix_4, @test_stream, "key_6", "value_6")

    assert_receive {consumer1, @test_stream, ^msg_id1, ["key_1", "value_1"]}, 5_000
    assert_receive {consumer2, @test_stream, ^msg_id2, ["key_2", "value_2"]}, 5_000
    assert_receive {consumer3, @test_stream, ^msg_id3, ["key_3", "value_3"]}, 5_000
    assert_receive {consumer4, @test_stream, ^msg_id4, ["key_4", "value_4"]}, 5_000
    assert_receive {consumer5, @test_stream, ^msg_id5, ["key_5", "value_5"]}, 5_000
    assert_receive {consumer6, @test_stream, ^msg_id6, ["key_6", "value_6"]}, 5_000

    assert Enum.member?(
             [consumer1, consumer2, consumer3, consumer4, consumer5, consumer6],
             :streamed_consumer1
           )

    assert Enum.member?(
             [consumer1, consumer2, consumer3, consumer4, consumer5, consumer6],
             :streamed_consumer2
           )
  end
end
