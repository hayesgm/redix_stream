# Redix.Stream [![CircleCI](https://circleci.com/gh/hayesgm/redix_stream.svg?style=svg)](https://circleci.com/gh/hayesgm/redix_stream)

`Redix.Stream` is an extension to [redix](https://github.com/whatyouhide/redix) supporting Redis streams. This project allows you to stream and consume data from redis streams.

[Redis streams](https://redis.io/topics/streams-intro) are similar to Kafka, nats.io and other "distributed commit log" software. The core idea is that the stream is an append-only log and any number of consumers can read from that stream, each keeping track of its position in that log. This allows for high-troughput processing of messages in the log. Streams can be used for analytics, queues, etc. based on how they are consumed.

** Note: redis streams are currently in the 5.0 release candidate. See `Installation` below for details. **

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `redix_stream` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:redix_stream, "~> 0.1.3"}
  ]
end
```

### Installing "unstable" Redis

As of writing, redis streams are currently available in the 5.0 release candidates. You can install from the official [downloads page](https://redis.io/download) (or directly from the [unstable.tar.gz](https://github.com/antirez/redis/archive/unstable.tar.gz)), use the 5.0-rc [docker image](https://hub.docker.com/_/redis/) or install from source.

If you are using Homebrew on macOS, you can simply run `run install redis --head`.

## Usage

First, you will need to start `redix`, e.g.

```elixir
{:ok, redix} = Redix.start_link("redis://localhost:6379")
```

Redix can also be started in the supervision tree as a named process.

Next, you should start a consumer to a stream specifying a callback function to run for each message:

```elixir
Redix.Stream.Consumer.start_link(redix, "my_topic", fn stream, msg -> Logger.info("Got message #{inspect msg} from stream #{stream}") end)
```

The callback function can be in `{module, function, args}` format as well:

```elixir
Redix.Stream.Consumer.start_link(redix, "my_topic", {MyModule, :my_func, []})
```

Consumers can also be started as part of the Supervision tree:

```elixir
def MyApp.Application do
  use Application

  def start(_type, _args) do
    # List all child processes to be supervised
    children = [
      worker(Redix, [[], [name: :redix]]),
      Redix.Stream.consumer(:redix, "my_topic", {MyModule, :my_func, []})
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
      Redix.Stream.consumer(:redix, "my_topic", {MyModule, :my_func, [group_name: "my_group", consumer_name: "consumer1"]})
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Blocks.Supervisor]
    Supervisor.start_link(children, opts)
  end
```

## Contributing

To contribute, please feel free to open an issue or pull request. Here are a few topics which we know need to be addressed:

1.  Callbacks are run in the stream consumer process. If the callback fails, it will crash the consumer process. The callbacks also block all processing until each finishes.

## Futher Reading

- [Redis Streams](https://redis.io/topics/streams-intro)
- [Redix.Streams ExDocs](https://hexdocs.pm/redix_stream)
