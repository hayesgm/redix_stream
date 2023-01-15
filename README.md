# Redix.Stream [![CircleCI](https://circleci.com/gh/hayesgm/redix_stream.svg?style=svg)](https://circleci.com/gh/hayesgm/redix_stream)

`Redix.Stream` is an extension to [redix](https://github.com/whatyouhide/redix) supporting Redis streams. This project allows you to stream and consume data from redis streams.

[Redis streams](https://redis.io/topics/streams-intro) are similar to Kafka, nats.io and other "distributed commit log" software. The core idea is that the stream is an append-only log and any number of consumers can read from that stream, each keeping track of its position in that log. This allows for high-troughput processing of messages in the log. Streams can be used for analytics, queues, etc. based on how they are consumed.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `redix_stream` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:redix_stream, "~> 0.2.2"}
  ]
end
```

Note: to use streams, you must be using redis 5.0 or greater.

## Usage

First, you will need to start `redix`, e.g.

```elixir
{:ok, redix} = Redix.start_link("redis://localhost:6379")
```

Redix can also be started in the supervision tree as a named process.

Next, you should start a consumer to a stream specifying a callback function to run for each message:

```elixir
Redix.Stream.Consumer.start_link(redix, "my_topic", fn stream, id, values -> Logger.info("Got message #{inspect values} from stream #{stream}") end)
```

The callback function can be in `{module, function, args}` format as well. When called, your handler will receive your args, followed by the stream name, the message id, and a map with the key-value pairs for the stream message.

```elixir
Redix.Stream.Consumer.start_link(redix, "my_topic", {MyModule, :my_func, [100]})

# Elsewhere
defmodule MyModule do
  @spec my_func(integer(), String.t(), String.t(), %{String.t() => String.t()}) :: :ok | {:error, String.t()}
  def my_func(my_arg, stream, id, values) do
    :ok
  end
end
```

Your handler must return `:ok`, otherwise the consumer will raise an error. For consumer groups, this will crash the consumer and means the message will need to be reprocessed by that consumer.

Consumers can also be started as part of the Supervision tree:

```elixir
def MyApp.Application do
  use Application

  def start(_type, _args) do
    # List all child processes to be supervised
    children = [
      worker(Redix, [[], [name: :redix]]),
      Redix.Stream.consumer_spec(:redix, "my_topic", {MyModule, :my_func, []})
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Blocks.Supervisor]
    Supervisor.start_link(children, opts)
  end
```

From there, you will be able to effectively stream messages.

## Consumer Groups

Redis Streams have the concept of [consumer groups](https://redis.io/topics/streams-intro#consumer-groups). Consumer groups allow multiple consumers to work on the same stream, guaranteeing that messages are only processed by one consumer.

Starting a Consumer as part of a group is similar to starting a normal stream. You need to provide the additional `group_name` and `consumer_name` options:

```elixir
def MyApp.Application do
  use Application

  def start(_type, _args) do
    # List all child processes to be supervised
    children = [
      worker(Redix, [[], [name: :redix]]),
      Redix.Stream.consumer_spec(:redix, "my_topic", {MyModule, :my_func, []}, group_name: "my_group", consumer_name: "consumer1")
    ]
    # start_pos: Stream starting position
    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Blocks.Supervisor]
    Supervisor.start_link(children, opts)
  end
```

Each consumer in the consumer group will process messages in parallel with other consumers, but each group will only consume each message one time. You can have multiple consumer groups for a given stream.

## Consumer Spec Options

Each consumer will have a single supervisor to monitor that consumer. This allows the supervisor to restart the consumer for temporary issues.

The following options can be passed in to `consumer_spec` when defining a consumer and supervisor:

* `sup_id` - The id to register the supervisor process as (default: `Redix.Stream.ConsumerSup`)
* `sup_restart` - The restart-type for the supervisor (`:transient`, `:temporary` or `:permanent`) (default: `:permanent`)
* `sup_name` - The name to register the supervisor as (default: same as sup_id)
* `sup_timeout` - The shutdown strategy (or timeout) for the supervisor.
* `id` - The id to register the consumer process as (default: `Redix.Stream.Consumer`)
* `timeout` - Timeout to wait for new messages in the stream before failing, 0 implies block forever (default: `0`)
* `group_name` - Consumer group for this consumer. We will create the group if it does not already exist (default: `nil`)
* `consumer_name` - Unique name for this consumer. These names should be persistent per work since each consumer will claim messages that need to be processed. (default: `nil`)
* `create_not_exists` - We will create the stream if it does not already exist (default: `true`)
* `process_pending` - For a consumer in a consumer group, should we process pending messages (ones we claimed but did not successfully process) before processing new messages? (default: `true`)
* `raise_errors` - If we fail to process a message because a handler returns an `{:error, error}` tuple, should we raise an error to fail the processor versus continue with an unacknowledged message? (default: `true`)

## Contributing

To contribute, please feel free to open an issue or pull request. Here are a few topics which we know need to be addressed:

1.  Callbacks are run in the stream consumer process. If the callback fails, it will crash the consumer process. The callbacks also block all processing until each finishes.

## Futher Reading

- [Redis Streams](https://redis.io/topics/streams-intro)
- [Redix.Streams ExDocs](https://hexdocs.pm/redix_stream)
