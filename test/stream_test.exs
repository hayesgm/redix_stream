defmodule Redix.StreamTest do
  use ExUnit.Case

  describe "produce/4" do
    test "it produces a new id" do
      {:ok, redix} = Redix.start_link()
      {:ok, msg_id} = Redix.Stream.produce(redix, "topic", "temperature", 55)

      assert Regex.match?(~r/\d+-\d+$/, msg_id)
    end
  end

  describe "consumer/3" do
    test "it should produce a spec with given a single stream and simple function" do
      spec = Redix.Stream.consumer(:redix, "topic", fn msg -> msg end)

      assert {
        Redix.Stream.Consumer,
        {Redix.Stream.Consumer, :start_link, [:redix, "topic", _fn, []]},
        :permanent, 5000, :worker, [Redix.Stream.Consumer]
      } = spec
    end

    test "it should produce a spec with given multiple streams and an MFA" do
      spec = Redix.Stream.consumer(:redix, "topic", {Module, :function, [:arg1, :arg2]})

      assert {
        Redix.Stream.Consumer,
        {
          Redix.Stream.Consumer, :start_link, [:redix, "topic", {Module, :function, [:arg1, :arg2]}, []]
        },
        :permanent, 5000, :worker, [Redix.Stream.Consumer]
      } = spec
    end

    test "it should produce a spec with given multiple streams and an MFA and opts" do
      spec = Redix.Stream.consumer(:redix, "topic", {Module, :function, [:arg1, :arg2]}, group: "my_consumer_group")

      assert {
        Redix.Stream.Consumer,
        {
          Redix.Stream.Consumer, :start_link, [:redix, "topic", {Module, :function, [:arg1, :arg2]}, [group: "my_consumer_group"]]
        },
        :permanent, 5000, :worker, [Redix.Stream.Consumer]
      } = spec
    end
  end
end
