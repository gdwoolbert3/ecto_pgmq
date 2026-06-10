# Message Routing

This guide covers the usage of topic-based routing.

## When to Use

Message routing is primarily used to send messages to a dynamic set of queues
based on a `t:EctoPGMQ.PGMQ.routing_key/0`. This model is heavily inspired by
[RabbitMQ Topic Exchanges](https://www.rabbitmq.com/docs/exchanges#topic).

## Motivating Example

Assume that an application executes
[webhooks](fifo_message_groups.md#motivating-example) and uses queues to handle
the execution results. The example below illustrates how webhook results can be
sent to multiple queues simultaneously:

```elixir
# Bind webhook queue to webhooks that need to be executed/retried
EctoPGMQ.update_queue(MyApp.Repo, "webhook_queue", %{bindings: ["webhook.created", "webhook.failed"]})

# Bind result queue to webhooks that have been executed
EctoPGMQ.create_queue(MyApp.Repo, "webhook_results", %{bindings: ["webhook.succeeded", "webhook.failed"]})

# Send a new webhook to the corresponding topic
routing_key = {:routing_key, "webhook.created"}

webhook = %MyApp.Webhook{
  code: nil,
  body: %{"data" => 123_456},
  url: URI.new!("https://host:443/path?foo=bar")
}

message = EctoPGMQ.Message.build(webhook, "transaction-123")
result = EctoPGMQ.send_messages(MyApp.Repo, routing_key, [message], payload_type: MyApp.Webhook)
%{"webhook_queue" => [_]} = result

# Send a failed webhook to the corresponding topic
routing_key = {:routing_key, "webhook.failed"}

webhook = %MyApp.Webhook{
  code: 500,
  body: %{"data" => 123_456},
  url: URI.new!("https://host:443/path?foo=bar")
}

message = EctoPGMQ.Message.build(webhook, "transaction-123")
result = EctoPGMQ.send_messages(MyApp.Repo, routing_key, [message], payload_type: MyApp.Webhook)
%{"webhook_queue" => [_], "webhook_results" => [_]} = result

# Send a succeeded webhook to the corresponding topic
routing_key = {:routing_key, "webhook.succeeded"}

webhook = %MyApp.Webhook{
  code: 200,
  body: %{"data" => 123_456},
  url: URI.new!("https://host:443/path?foo=bar")
}

message = EctoPGMQ.Message.build(webhook, "transaction-123")
result = EctoPGMQ.send_messages(MyApp.Repo, routing_key, [message], payload_type: MyApp.Webhook)
%{"webhook_results" => [_]} = result
```

## Additional Information

For more information about topics and message routing, see the
[PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/topics.md).
