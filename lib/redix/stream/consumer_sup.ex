defmodule Redix.Stream.ConsumerSup do
  @moduledoc """
  Supervisor which attempts to restart a failed consumer.

  Generally, you should start this supervisor as transient,
  since a failure will be unrecoverable beyond this point.
  """
  use Supervisor

  alias Redix.Stream.Consumer

  def child_spec([redix, stream, handler, opts]) do
    {sup_id, opts_2} = Keyword.pop(opts, :sup_id, __MODULE__)
    {restart, opts_3} = Keyword.pop(opts_2, :sup_restart, :permanent)
    opts_4 = Keyword.put(opts_3, :sup_name, sup_id)

    %{
      id: sup_id,
      start: {__MODULE__, :start_link, [redix, stream, handler, opts_4]},
      type: :supervisor,
      restart: restart
    }
  end

  def start_link(redix, stream, handler, opts) do
    {sup_name, rest_opts} = Keyword.pop(opts, :sup_name, __MODULE__)

    Supervisor.start_link(__MODULE__, {redix, stream, handler, rest_opts}, name: sup_name)
  end

  @impl true
  def init({redix, stream, handler, opts}) do
    {shutdown, consumer_opts} = Keyword.pop(opts, :sup_timeout, 5000)

    children = [
      {Consumer, {redix, stream, handler, consumer_opts}}
    ]

    Supervisor.init(children, strategy: :one_for_one, shutdown: shutdown)
  end
end
