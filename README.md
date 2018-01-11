# Redix.Stream [![CircleCI](https://circleci.com/gh/hayesgm/redix_stream.svg?style=svg)](https://circleci.com/gh/hayesgm/redix_stream)

`Redix.Stream` is an extension to [redix](https://github.com/whatyouhide/redix) supporting Redis streams. This project allows you to stream and consume data from redis streams.

[Redis streams](http://antirez.com/news/114) are similar to Kafka, nats.io and other "distributed commit log" software. The core idea is that the stream is an append-only log and any number of consumers can read from that stream, each keeping track of its position in that log. This allows for high-troughput processing of messages in the log. Streams can be used for analytics, queues, etc. based on how they are consumed.

** Note: redis streams are currently only available on the unstable (head) branch of redis. See `Installation` below for details. **

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `redix_stream` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:redix_stream, "~> 0.1.1"}
  ]
end
```

### Installing "unstable" Redis

As of writing, redis streams are currently only available on the unstable (head) branch of redis. You can install from the official [downloads page](https://redis.io/download) (or directly from the [unstable.tar.gz](https://github.com/antirez/redis/archive/unstable.tar.gz)), or install from source.

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

## Rank Tracker

Until redis streams officially support consumer groups, we add an option that allows a given consumer to specify a rank (position) tracker per consumer. This tracks the last processed offset in a stream by a consumer, such that we can continue to consume a stream after the consumer restarts.

** Trackers do not support sharding of stream items. Trackers are only used to track your position in the stream across restarts. Trackers do not guarantee exactly-once message processing. **

```elixir
Redix.Stream.Consumer.start_link(redix, "my_topic", {MyModule, :my_func, []}, tracker: "my_stream_tracker")
```

This consumer can be restarted and will safely pick up processing the next message. This is done by tracking the highest id of stream in a redis key `stream_tracker:<tracker name>`. ** If processing fails, messages will not be re-streamed. **

## Contributing

To contribute, please feel free to open an issue or pull request. Here are a few topics which we know need to be addressed:

 1. Callbacks are run in the stream consumer process. If the callback fails, it will crash the consumer process. The callbacks also block all processing until each finishes.

 2. Redis streams currently do not support consumer groups. This means they we cannot run several consumers and know that only one will process a given message. This could be acheived through other redis commands, however.

## Futher Reading

* [Redis Streams](http://antirez.com/news/114)
* [Redix.Streams ExDocs](https://hexdocs.pm/redix_stream)
