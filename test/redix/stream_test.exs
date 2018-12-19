defmodule Redix.StreamTest do
  use ExUnit.Case
  doctest Redix.Stream

  setup_all do
    {:ok, pid} = Redix.start_link()
    Process.register(pid, :redix)

    :ok
  end

  describe "produce/4" do
    test "it produces a new id" do
      {:ok, redix} = Redix.start_link()
      {:ok, msg_id} = Redix.Stream.produce(redix, "topic", %{"temperature" => 55})

      assert Regex.match?(~r/\d+-\d+$/, msg_id)
    end
  end

  describe "consumer/3" do
    test "it should produce a spec with given a single stream and simple function" do
      spec = Redix.Stream.consumer_spec(:redix, "topic", fn msg -> msg end)

      assert %{
               id: Redix.Stream.ConsumerSup,
               start:
                 {Redix.Stream.ConsumerSup, :start_link,
                  [:redix, "topic", _fn, [sup_name: Redix.Stream.ConsumerSup]]},
               type: :supervisor,
               restart: :permanent
             } = spec
    end

    test "it should produce a spec with given multiple streams and an MFA" do
      spec = Redix.Stream.consumer_spec(:redix, "topic", {Module, :function, [:arg1, :arg2]})

      assert %{
               id: Redix.Stream.ConsumerSup,
               start: {
                 Redix.Stream.ConsumerSup,
                 :start_link,
                 [
                   :redix,
                   "topic",
                   {Module, :function, [:arg1, :arg2]},
                   [sup_name: Redix.Stream.ConsumerSup]
                 ]
               },
               type: :supervisor,
               restart: :permanent
             } == spec
    end

    test "it should produce a spec with given multiple streams and an MFA and opts" do
      spec =
        Redix.Stream.consumer_spec(
          :redix,
          "topic",
          {Module, :function, [:arg1, :arg2]},
          group: "my_consumer_group"
        )

      assert %{
               id: Redix.Stream.ConsumerSup,
               start: {
                 Redix.Stream.ConsumerSup,
                 :start_link,
                 [
                   :redix,
                   "topic",
                   {Module, :function, [:arg1, :arg2]},
                   [
                     sup_name: Redix.Stream.ConsumerSup,
                     group: "my_consumer_group"
                   ]
                 ]
               },
               type: :supervisor,
               restart: :permanent
             } = spec
    end
  end
end
