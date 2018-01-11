defmodule Redix.Stream.ConsumerTest do
  use ExUnit.Case
  doctest Redix.Stream.Consumer

  @test_stream "test_stream"

  @tag :integration
  test "it should connect and stream a single message" do
    {:ok, redix_1} = Redix.start_link()
    {:ok, redix_2} = Redix.start_link()
    pid = self()

    {:ok, _pid} = Redix.Stream.Consumer.start_link(
      redix_1,
      @test_stream,
      fn stream, {id, values} -> send(pid, {:streamed, stream, id, values}) end
    )

    :timer.sleep(500) # allow consumer time to connect

    {:ok, _msg_id} = Redix.Stream.produce(redix_2, @test_stream, "key_1", "value_1")

    assert_receive {:streamed, @test_stream, _id, ["key_1", "value_1"]}, 5_000
  end

  @tag :integration
  test "it should connect and stream multiple messages" do
    {:ok, redix_1} = Redix.start_link()
    {:ok, redix_2} = Redix.start_link()
    pid = self()

    {:ok, _pid} = Redix.Stream.Consumer.start_link(
      redix_1,
      @test_stream,
      fn stream, {id, values} -> send(pid, {:streamed, stream, id, values}) end
    )

    :timer.sleep(500) # allow consumer time to connect

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

    {:ok, _pid} = Redix.Stream.Consumer.start_link(
      redix_1,
      @test_stream,
      fn stream, {id, values} ->
        :timer.sleep(100) # this runs in the consumer and blocks
                          # further processing.

        send(pid, {:streamed, stream, id, values})
      end
    )

    :timer.sleep(500) # allow consumer time to connect

    {:ok, _msg_id} = Redix.Stream.produce(redix_2, @test_stream, "key_1", "value_1")
    {:ok, _msg_id} = Redix.Stream.produce(redix_2, @test_stream, "key_2", "value_2")
    {:ok, _msg_id} = Redix.Stream.produce(redix_2, @test_stream, "key_3", "value_3")

    assert_receive {:streamed, @test_stream, _id, ["key_1", "value_1"]}, 5_000
    assert_receive {:streamed, @test_stream, _id, ["key_2", "value_2"]}, 5_000
    assert_receive {:streamed, @test_stream, _id, ["key_3", "value_3"]}, 5_000
  end
end