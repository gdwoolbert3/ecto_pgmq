defmodule EctoPGMQ.PGMQTest do
  use EctoPGMQ.TestCase, async: true

  import Ecto.Query

  alias EctoPGMQ.Binding
  alias EctoPGMQ.DurationType
  alias EctoPGMQ.Metrics
  alias EctoPGMQ.PGMQ
  alias EctoPGMQ.Queue
  alias EctoPGMQ.Throttle

  doctest PGMQ, import: true

  describe "archive/4" do
    test "will archive messages", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)

      # Validate that messages are in the queue
      refute queue_empty?(Repo, ctx.queue.name)

      PGMQ.archive(Repo, ctx.queue.name, message_ids)

      # Validate that messages have been removed from the queue
      assert queue_empty?(Repo, ctx.queue.name)

      # Validate that all of the messages have been archived
      assert Repo
             |> all_archive_messages(ctx.queue.name)
             |> same_messages?(message_ids, message_specs)
    end
  end

  describe "bind_topic/4" do
    test "will bind a queue to a routing key pattern", ctx do
      queue = ctx.queue.name

      # Validate that queue has bindings
      assert %Queue{bindings: []} = EctoPGMQ.get_queue(Repo, queue)

      PGMQ.bind_topic(Repo, "#", queue)

      # Validate that queue has the expected bindings
      assert %Queue{
               bindings: [
                 %Binding{
                   queue: ^queue,
                   pattern: "#",
                   regex: %Regex{},
                   bound_at: %DateTime{}
                 }
               ]
             } = EctoPGMQ.get_queue(Repo, queue)
    end
  end

  describe "convert_archive_partitioned/6" do
    test "will partition a non-partitioned archive based on message ID", ctx do
      # Validate that archive is unpartitioned
      refute archive_partitioned?(Repo, ctx.queue.name)

      PGMQ.convert_archive_partitioned(Repo, ctx.queue.name)

      # Validate that archive is partitioned
      assert archive_partitioned?(Repo, ctx.queue.name)
    end

    test "will partition a non-partitioned archive based on a time interval", ctx do
      # Validate that archive is unpartitioned
      refute archive_partitioned?(Repo, ctx.queue.name)

      partition = Duration.new!(minute: 5)
      retention = Duration.new!(hour: 1)
      PGMQ.convert_archive_partitioned(Repo, ctx.queue.name, partition, retention)

      # Validate that archive is partitioned
      assert archive_partitioned?(Repo, ctx.queue.name)
    end
  end

  describe "create_fifo_index/3" do
    test "will create an index to optimize FIFO group reads", ctx do
      # Validate that index does not exist
      refute fifo_index?(Repo, ctx.queue.name)

      PGMQ.create_fifo_index(Repo, ctx.queue.name)

      # Validate that index has been created
      assert fifo_index?(Repo, ctx.queue.name)
    end
  end

  describe "create_non_partitioned/3" do
    @describetag queue: false

    test "will create a non-partitioned queue" do
      # Validate that queue does not exist
      assert EctoPGMQ.get_queue(Repo, "my_unpartitioned_queue") == nil

      PGMQ.create_non_partitioned(Repo, "my_unpartitioned_queue")

      # Validate that queue has been created
      assert %Queue{
               name: "my_unpartitioned_queue",
               created_at: %DateTime{},
               partitioned?: false,
               unlogged?: false,
               metrics: %Metrics{},
               notifications: nil
             } = EctoPGMQ.get_queue(Repo, "my_unpartitioned_queue")
    end
  end

  describe "create_partitioned/5" do
    @describetag queue: false

    test "will create a partitioned queue based on message ID" do
      # Validate that queue does not exist
      assert EctoPGMQ.get_queue(Repo, "my_partitioned_queue") == nil

      PGMQ.create_partitioned(Repo, "my_partitioned_queue")

      # Validate that queue has been created
      assert %Queue{
               name: "my_partitioned_queue",
               created_at: %DateTime{},
               partitioned?: true,
               unlogged?: false,
               metrics: %Metrics{},
               notifications: nil
             } = EctoPGMQ.get_queue(Repo, "my_partitioned_queue")
    end

    test "will create a partitioned queue based on a time interval" do
      # Validate that queue does not exist
      assert EctoPGMQ.get_queue(Repo, "my_partitioned_queue") == nil

      partition = Duration.new!(minute: 5)
      retention = Duration.new!(hour: 1)
      PGMQ.create_partitioned(Repo, "my_partitioned_queue", partition, retention)

      # Validate that queue has been created
      assert %Queue{
               name: "my_partitioned_queue",
               created_at: %DateTime{},
               partitioned?: true,
               unlogged?: false,
               metrics: %Metrics{},
               notifications: nil
             } = EctoPGMQ.get_queue(Repo, "my_partitioned_queue")
    end
  end

  describe "create_unlogged/3" do
    @describetag queue: false

    test "will create an unlogged queue" do
      # Validate that queue does not exist
      assert EctoPGMQ.get_queue(Repo, "my_unlogged_queue") == nil

      PGMQ.create_unlogged(Repo, "my_unlogged_queue")

      # Validate that queue has been created
      assert %Queue{
               name: "my_unlogged_queue",
               created_at: %DateTime{},
               partitioned?: false,
               unlogged?: true,
               metrics: %Metrics{},
               notifications: nil
             } = EctoPGMQ.get_queue(Repo, "my_unlogged_queue")
    end
  end

  describe "delete/4" do
    test "will delete messages", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)

      # Validate that messages are in the queue
      refute queue_empty?(Repo, ctx.queue.name)

      PGMQ.delete(Repo, ctx.queue.name, message_ids)

      # Validate that messages have been deleted
      assert queue_empty?(Repo, ctx.queue.name)
    end
  end

  describe "disable_notify_insert/3" do
    @describetag queue_attributes: %{notifications: 250}

    test "will disable insert notifications", ctx do
      # Validate that notifications are enabled
      assert %Queue{notifications: %Throttle{}} = EctoPGMQ.get_queue(Repo, ctx.queue.name)

      PGMQ.disable_notify_insert(Repo, ctx.queue.name)

      # Validate that notifications have been disabled
      assert %Queue{notifications: nil} = EctoPGMQ.get_queue(Repo, ctx.queue.name)
    end
  end

  describe "drop_queue/3" do
    test "will drop a queue", ctx do
      # Validate that queue exists
      assert %Queue{} = EctoPGMQ.get_queue(Repo, ctx.queue.name)

      PGMQ.drop_queue(Repo, ctx.queue.name)

      # Validate that queue has been dropped
      assert EctoPGMQ.get_queue(Repo, ctx.queue.name) == nil
    end
  end

  describe "enable_notify_insert/4" do
    test "will enable insert notifications", ctx do
      # Validate that notifications are disabled
      assert %Queue{notifications: nil} = EctoPGMQ.get_queue(Repo, ctx.queue.name)

      PGMQ.enable_notify_insert(Repo, ctx.queue.name)

      # Validate that notifications have been enabled
      assert %Queue{notifications: %Throttle{}} = EctoPGMQ.get_queue(Repo, ctx.queue.name)
    end
  end

  describe "list_notify_insert_throttles/2" do
    @describetag queue: false

    test "will list all notification throttles" do
      queue_1 = EctoPGMQ.create_queue(Repo, "my_queue_1", %{notifications: 250})
      queue_2 = EctoPGMQ.create_queue(Repo, "my_queue_2", %{notifications: 250})
      response = PGMQ.list_notify_insert_throttles(Repo)

      # Validate that the response contains the expected records
      assert [queue_1, queue_2]
             |> Enum.map(fn queue -> queue.notifications end)
             |> same_elements?(response)
    end
  end

  describe "list_queues/2" do
    @describetag queue: false

    test "will list all queues" do
      queue_1 = EctoPGMQ.create_queue(Repo, "my_queue_1")
      queue_2 = EctoPGMQ.create_queue(Repo, "my_queue_2")
      response = PGMQ.list_queues(Repo)

      # Validate that the response contains the expected records
      assert [queue_1, queue_2]
             |> Enum.map(fn queue -> %{queue | metrics: nil, notifications: nil} end)
             |> same_elements?(response)
    end
  end

  describe "list_topic_bindings/2" do
    @describetag queue: false

    test "will list all queue bindings" do
      queue_1 = EctoPGMQ.create_queue(Repo, "my_queue_1", %{bindings: ["foo.*", "bar.*"]})
      queue_2 = EctoPGMQ.create_queue(Repo, "my_queue_2", %{bindings: ["bar.*", "baz.*"]})
      response = PGMQ.list_topic_bindings(Repo)

      # Validate that the response contains the expected records
      # Note that direct regex comparison will often fail so we instead compare
      # the regex sources. For more information, see
      # https://hexdocs.pm/elixir/Regex.html.
      assert [queue_1, queue_2]
             |> Enum.flat_map(fn queue -> queue.bindings end)
             |> Enum.map(fn b -> Map.update!(b, :regex, &Regex.source/1) end)
             |> same_elements?(Enum.map(response, fn b -> Map.update!(b, :regex, &Regex.source/1) end))
    end
  end

  describe "metrics_all/2" do
    @describetag queue: false

    test "will return metrics for all queues" do
      EctoPGMQ.create_queue(Repo, "my_queue_1")
      EctoPGMQ.create_queue(Repo, "my_queue_2")

      # Validate that the response contains the expected records
      assert Repo
             |> PGMQ.metrics_all()
             |> Enum.map(fn %Metrics{queue: queue} -> queue end)
             |> same_elements?(["my_queue_1", "my_queue_2"])
    end
  end

  describe "pop/4" do
    test "will pop messages", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      messages = all_queue_messages(Repo, ctx.queue.name)

      # Validate that messages are in the queue
      refute queue_empty?(Repo, ctx.queue.name)

      response = PGMQ.pop(Repo, ctx.queue.name, 2)

      # Validate that the queue is empty
      assert queue_empty?(Repo, ctx.queue.name)

      # Validate that the response contains the expected records
      # Note that visibility timeout and read count don't change
      assert messages == response
    end
  end

  describe "purge_queue/3" do
    test "will purge messages from a queue", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)

      # Validate that messages are in the queue
      refute queue_empty?(Repo, ctx.queue.name)

      # Validate that the purged message count is correct
      assert PGMQ.purge_queue(Repo, ctx.queue.name) == 2

      # Validate that messages have been removed from the queue
      assert queue_empty?(Repo, ctx.queue.name)
    end
  end

  describe "read/6" do
    test "will read messages from a queue", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      messages = all_queue_messages(Repo, ctx.queue.name)
      response = PGMQ.read(Repo, ctx.queue.name, 300, 2)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end

    test "will read messages from a queue when too many are requested", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      messages = all_queue_messages(Repo, ctx.queue.name)
      response = PGMQ.read(Repo, ctx.queue.name, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end
  end

  describe "read_grouped/5" do
    @describetag queue_attributes: %{message_groups?: true}

    test "will read messages from a queue", ctx do
      message_specs = [
        Message.build(%{"id" => 1}, "foo"),
        Message.build(%{"id" => 2}, "foo"),
        Message.build(%{"id" => 3}, "bar")
      ]

      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      messages = all_queue_messages(Repo, ctx.queue.name)
      response = PGMQ.read_grouped(Repo, ctx.queue.name, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 3)
    end

    test "will read messages from a queue when too many are requested", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      messages = all_queue_messages(Repo, ctx.queue.name)
      response = PGMQ.read_grouped(Repo, ctx.queue.name, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end
  end

  describe "read_grouped_rr/5" do
    @describetag queue_attributes: %{message_groups?: true}

    test "will read messages from a queue", ctx do
      message_specs = [
        Message.build(%{"id" => 1}, "foo"),
        Message.build(%{"id" => 2}, "foo"),
        Message.build(%{"id" => 3}, "bar")
      ]

      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      [foo_1, foo_2, bar_1] = all_queue_messages(Repo, ctx.queue.name)
      response = PGMQ.read_grouped_rr(Repo, ctx.queue.name, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?([foo_1, bar_1, foo_2], response, 300, 3)
    end

    test "will read messages from a queue when too many are requested", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      messages = all_queue_messages(Repo, ctx.queue.name)
      response = PGMQ.read_grouped_rr(Repo, ctx.queue.name, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end
  end

  describe "read_grouped_rr_with_poll/7" do
    @describetag queue_attributes: %{message_groups?: true}

    test "will read messages from a queue", ctx do
      message_specs = [
        Message.build(%{"id" => 1}, "foo"),
        Message.build(%{"id" => 2}, "foo"),
        Message.build(%{"id" => 3}, "bar")
      ]

      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      [foo_1, foo_2, bar_1] = all_queue_messages(Repo, ctx.queue.name)
      response = PGMQ.read_grouped_rr_with_poll(Repo, ctx.queue.name, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?([foo_1, bar_1, foo_2], response, 300, 3)
    end

    test "will read messages from a queue when too many are requested", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      messages = all_queue_messages(Repo, ctx.queue.name)
      response = PGMQ.read_grouped_rr_with_poll(Repo, ctx.queue.name, 300, 3, 1, 500)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end
  end

  describe "read_grouped_with_poll/7" do
    @describetag queue_attributes: %{message_groups?: true}

    test "will read messages from a queue", ctx do
      message_specs = [
        Message.build(%{"id" => 1}, "foo"),
        Message.build(%{"id" => 2}, "foo"),
        Message.build(%{"id" => 3}, "bar")
      ]

      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      messages = all_queue_messages(Repo, ctx.queue.name)
      response = PGMQ.read_grouped_with_poll(Repo, ctx.queue.name, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 3)
    end

    test "will read messages from a queue when too many are requested", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      messages = all_queue_messages(Repo, ctx.queue.name)
      response = PGMQ.read_grouped_with_poll(Repo, ctx.queue.name, 300, 3, 1, 500)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end
  end

  describe "read_with_poll/8" do
    test "will read messages from a queue", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      messages = all_queue_messages(Repo, ctx.queue.name)
      response = PGMQ.read_with_poll(Repo, ctx.queue.name, 300, 2)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end

    test "will read messages from a queue when too many are requested", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      messages = all_queue_messages(Repo, ctx.queue.name)
      response = PGMQ.read_with_poll(Repo, ctx.queue.name, 300, 3, 1, 500)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end
  end

  describe "send_batch/6" do
    test "will send messages to a queue with an integer delay", ctx do
      payloads = [%{"id" => 1}, %{"id" => 2}]
      message_ids = PGMQ.send_batch(Repo, ctx.queue.name, payloads)

      # Validate that all of the messages are in the queue
      assert Repo
             |> all_queue_messages(ctx.queue.name)
             |> same_messages?(message_ids, Enum.map(payloads, &Message.build/1))
    end

    test "will send messages to a queue with a timestamp delay", ctx do
      payloads = [%{"id" => 1}, %{"id" => 2}]
      message_ids = PGMQ.send_batch(Repo, ctx.queue.name, payloads, nil, DateTime.utc_now())

      # Validate that all of the messages are in the queue
      assert Repo
             |> all_queue_messages(ctx.queue.name)
             |> same_messages?(message_ids, Enum.map(payloads, &Message.build/1))
    end
  end

  describe "send_batch_topic/6" do
    @describetag queue_attributes: %{bindings: ["#"]}

    test "will send messages with an integer delay", ctx do
      queue = ctx.queue.name
      payloads = [%{"id" => 1}, %{"id" => 2}]

      assert %{^queue => message_ids} = PGMQ.send_batch_topic(Repo, "my.key", payloads)

      # Validate that all of the messages are in the queue
      assert Repo
             |> all_queue_messages(queue)
             |> same_messages?(message_ids, Enum.map(payloads, &Message.build/1))
    end

    test "will send messages with a timestamp delay", ctx do
      queue = ctx.queue.name
      payloads = [%{"id" => 1}, %{"id" => 2}]

      assert %{^queue => message_ids} = PGMQ.send_batch_topic(Repo, "my.key", payloads, nil, DateTime.utc_now())

      # Validate that all of the messages are in the queue
      assert Repo
             |> all_queue_messages(queue)
             |> same_messages?(message_ids, Enum.map(payloads, &Message.build/1))
    end
  end

  describe "set_vt/5" do
    test "will update message visibility timeouts with an integer delay", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      messages = all_queue_messages(Repo, ctx.queue.name)
      response = PGMQ.set_vt(Repo, ctx.queue.name, message_ids, 300)

      # Validate that invisible messages will not be read
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert updated_messages?(response, messages, 300, 2)
    end

    test "will update message visibility timeouts with a timestamp delay", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      [message | _] = messages = all_queue_messages(Repo, ctx.queue.name)
      delay = DateTime.shift(message.visible_at, second: 300)
      response = PGMQ.set_vt(Repo, ctx.queue.name, message_ids, delay)

      # Validate that invisible messages will not be read
      assert PGMQ.read(Repo, ctx.queue.name, 300, 1) == []

      # Validate that the response contains the expected records
      assert updated_messages?(response, messages, 300, 2)
    end
  end

  describe "test_routing/3" do
    @describetag queue_attributes: %{bindings: ["#"]}

    test "will return all bindings for a routing key", ctx do
      queue = ctx.queue.name

      # Validate that result contains expected bindings
      assert [%Binding{queue: ^queue}] = PGMQ.test_routing(Repo, "my.key")
    end
  end

  describe "unbind_topic/4" do
    @describetag queue_attributes: %{bindings: ["#"]}

    test "will unbind a queue from a routing key pattern", ctx do
      # Validate that queue has bindings
      assert %Queue{bindings: [_]} = EctoPGMQ.get_queue(Repo, ctx.queue.name)

      PGMQ.unbind_topic(Repo, "#", ctx.queue.name)

      # Validate that queue no longer has bindings
      assert %Queue{bindings: []} = EctoPGMQ.get_queue(Repo, ctx.queue.name)
    end
  end

  describe "update_notify_insert/4" do
    @describetag queue_attributes: %{notifications: 250}

    test "will update a notification throttle", ctx do
      # Validate that notification throttle is set
      assert %Queue{notifications: %Throttle{interval: interval}} = EctoPGMQ.get_queue(Repo, ctx.queue.name)
      assert DurationType.to_time(interval, :millisecond) == 250

      PGMQ.update_notify_insert(Repo, ctx.queue.name, 500)

      # Validate that notification throttle has been updated
      assert %Queue{notifications: %Throttle{interval: interval}} = EctoPGMQ.get_queue(Repo, ctx.queue.name)
      assert DurationType.to_time(interval, :millisecond) == 500
    end
  end

  describe "validate_routing_key/3" do
    @describetag queue: false

    test "will validate a routing key" do
      assert PGMQ.validate_routing_key(Repo, "my.key") == :ok
    end

    test "will raise an error with an invalid routing key" do
      assert_raise Postgrex.Error, fn ->
        PGMQ.validate_routing_key(Repo, "my.*.invalid..key")
      end
    end
  end

  describe "validate_topic_pattern" do
    @describetag queue: false

    test "will validate a pattern" do
      assert PGMQ.validate_topic_pattern(Repo, "my.*.pattern")
    end

    test "will raise an error with an invalid pattern" do
      assert_raise Postgrex.Error, fn ->
        PGMQ.validate_topic_pattern(Repo, "my..invalid*.pattern")
      end
    end
  end

  ################################
  # Private API
  ################################

  defp archive_partitioned?(repo, queue) do
    "part_config"
    |> put_query_prefix("partman")
    |> where([p], p.parent_table == ^"#{PGMQ.schema()}.#{PGMQ.archive_table_name(queue)}")
    |> repo.exists?()
  end
end
