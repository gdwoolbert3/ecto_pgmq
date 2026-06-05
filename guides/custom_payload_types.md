# Custom Payload Types

This guide covers the usage of custom payload types.

TODO(Gordon) - add warning about single type per queue

## When to Use

One of the many benefits of `Ecto` is the strict separation of database data and
application data. While PGMQ requires `JSONB` payloads for messages, `EctoPGMQ`
allows applications to use any `t:term/0` as a message payload as long as there
is a corresponding `Ecto.Type` or `Ecto.ParameterizedType` implementation that
dumps to and loads from a `t:map/0`.

Custom payload types are supported by all `EctoPGMQ` functions that read or
write PGMQ messages.

## Motivating Example

Assume that an application supports webhooks and processes them with a queue.
The following `Ecto.Type` implementation can be used to read and write messages
with a `Webhook` struct as a payload:

```elixir
defmodule MyApp.Webhook do
  @moduledoc false

  use Ecto.Type

  defstruct [:url, :body]

  @doc false
  @impl Ecto.Type
  def cast(%__MODULE__{} = webhook), do: {:ok, webhook}

  @doc false
  @impl Ecto.Type
  def dump(%__MODULE__{url: url, body: body}) do
    {:ok, %{"body" => body, "url" => URI.to_string(url)}}
  end

  @doc false
  @impl Ecto.Type
  def load(%{"body" => body, "url" => url}) do
    {:ok, %__MODULE__{body: body, url: URI.new!(url)}}
  end

  @doc false
  @impl Ecto.Type
  def type, do: :map
end
```

The examples below illustrate some of the ways that the above implementation can
be used in practice:

```elixir
# Send a webhook to a queue
webhook = %MyApp.Webhook{
  body: %{"data" => 123_456},
  url: URI.new!("https://host:443/path?foo=bar")
}

message = EctoPGMQ.Message.build(webhook)
EctoPGMQ.send_messages(MyApp.Repo, "webhook_queue", [message], payload_type: MyApp.Webhook)

# Read a webhook from a queue
messages = EctoPGMQ.read_messages(MyApp.Repo, "webhook_queue", 300, 1, payload_type: MyApp.Webhook)
[%EctoPGMQ.Message{payload: %MyApp.Webhook{}} | _] = messages

# Query for webhooks directly
messages =
  "webhook_queue"
  |> EctoPGMQ.Message.queue_query(payload_type: MyApp.Webhook)
  |> MyApp.Repo.all()

[%EctoPGMQ.Message{payload: %MyApp.Webhook{}} | _] = messages
```

## Payload Filtering

Due to how custom payload types are applied, non-map payloads can't be directly
interpolated in an `Ecto.Query`. Instead, payloads must be cast with
`Ecto.Query.API.type/2`:

```elixir
"webhook_queue"
|> EctoPGMQ.Message.queue_query(payload_type: MyApp.Webhook)
|> where([m], m.payload == type(^webhook, MyApp.Webhook))
```

> #### Ecto Type {: .warning}
> Note that the second argument to the aforementioned function is **NOT** a
> `t:EctoPGMQ.Message.payload_type/0` but rather an `t:Ecto.Type.t/0`.

Alternatively, PGMQ has experimental support for payload filtering during reads.
For more information, see `t:EctoPGMQ.PGMQ.conditional/0`.
