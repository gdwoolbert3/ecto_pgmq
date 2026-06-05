# FIFO Message Groups

This guide covers the usage of FIFO message groups.

## When to Use

While PGMQ queues are inherently FIFO data structures, messages can be processed
in non-deterministic order when message processing is parallelized within a
single consumer and/or across multiple consumers. In many cases, this isn't an
issue but there is sometimes a need to consume messages strictly in order within
a group. In order to support this, PGMQ exposes a number of functions that read
messages while guaranteeing FIFO ordering for messages with the same
`x-pgmq-group` header.

## Reading Methods

There are three slightly different methodologies for reading messages while
respecting FIFO message groups:

### Head

This method will read **ONLY** the oldest visible message (the "head") for each
group:

```elixir
messages = [
  Message.build(%{"foo" => 1}, "A"),
  Message.build(%{"foo" => 2}, "A"),
  Message.build(%{"foo" => 3}, "B"),
  Message.build(%{"foo" => 4}, "B"),
  Message.build(%{"foo" => 5}, "C")
]

[id_1, _, id_2, _, id_3] = EctoPGMQ.send_messages(MyApp.Repo, "my_queue", messages)
messages = EctoPGMQ.read_messages(Myapp.Repo, "my_queue", 300, 4, message_grouping: :head)
[^id_1, ^id_2, ^id_3] = Enum.map(messages, & &1.id)
```

### Round-Robin

This method will fairly interleave messages from all available groups:

```elixir
messages = [
  Message.build(%{"foo" => 1}, "A"),
  Message.build(%{"foo" => 2}, "A"),
  Message.build(%{"foo" => 3}, "B"),
  Message.build(%{"foo" => 4}, "B"),
  Message.build(%{"foo" => 5}, "C")
]

[id_1, id_2, id_3, _, id_4] = EctoPGMQ.send_messages(MyApp.Repo, "my_queue", messages)
messages = EctoPGMQ.read_messages(Myapp.Repo, "my_queue", 300, 4, message_grouping: :round_robin)
[^id_1, ^id_3, ^id_4, ^id_2] = Enum.map(messages, & &1.id)
```

### Throughput-Optimized

This method will prioritize messages from the same group in order to maximize
throughput:

```elixir
messages = [
  Message.build(%{"foo" => 1}, "A"),
  Message.build(%{"foo" => 2}, "A"),
  Message.build(%{"foo" => 3}, "B"),
  Message.build(%{"foo" => 4}, "B"),
  Message.build(%{"foo" => 5}, "C")
]

[id_1, id_2, id_3, id_4, _] = EctoPGMQ.send_messages(MyApp.Repo, "my_queue", messages)
messages = EctoPGMQ.read_messages(Myapp.Repo, "my_queue", 300, 4, message_grouping: :throughput_optimized)
[^id_1, ^id_2, ^id_3, ^id_4] = Enum.map(messages, & &1.id)
```

> #### Long-Lived Message Groups {: .warning}
>
> If message groups are long-lived and high-volume, this method of reading can
> effectively starve later groups. For more information, see
> [Performance Considerations](#performance-considerations).

## Performance Considerations

In general, FIFO message groups are more performant when the following
conditions are met:

  * There are many low-volume groups.

  * Messages are removed from the queue relatively quickly.

  * The queue is optimized for FIFO message group reads (see
    `EctoPGMQ.create_queue/4`, `EctoPGMQ.update_queue/4` and the corresponding
    functions in the `EctoPGMQ.Migrations` module).

> #### Messages Without a Group {: .warning}
>
> Messages with no group specified are treated as if they belong to a single,
> default group.

## Additional Information

For more information about FIFO message groups, see the
[PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/fifo-queues.md).
