defmodule EctoPGMQTest do
  use EctoPGMQ.TestCase, async: true

  alias EctoPGMQ.Metrics
  alias EctoPGMQ.Queue
  alias EctoPGMQ.TestType
  alias EctoPGMQ.Throttle

  doctest EctoPGMQ, import: true

  describe "all_queues/2" do
    @describetag :no_default_queue

    test "will list all queues" do
      queue_1 = EctoPGMQ.create_queue(Repo, "my_queue_1")
      queue_2 = EctoPGMQ.create_queue(Repo, "my_queue_2")

      # Validate that the response contains the expected records
      assert Repo
             |> EctoPGMQ.all_queues()
             |> same_elements?([queue_1, queue_2])
    end
  end

  describe "create_queue/3" do
    @describetag :no_default_queue

    test "will create an unpartitioned queue" do
      # Validate that queue does not exist
      assert EctoPGMQ.get_queue(Repo, "my_unpartitioned_queue") == nil

      EctoPGMQ.create_queue(Repo, "my_unpartitioned_queue")

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

    test "will create a partitioned queue" do
      # Validate that queue does not exist
      assert EctoPGMQ.get_queue(Repo, "my_partitioned_queue") == nil

      EctoPGMQ.create_queue(Repo, "my_partitioned_queue", %{partitions: {10_000, 100_000}})

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

    test "will create an unlogged queue" do
      # Validate that queue does not exist
      assert EctoPGMQ.get_queue(Repo, "my_unlogged_queue") == nil

      EctoPGMQ.create_queue(Repo, "my_unlogged_queue", %{unlogged?: true})

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

    test "will create a queue with notifications enabled" do
      # Validate that queue does not exist
      assert EctoPGMQ.get_queue(Repo, "my_queue") == nil

      throttle = Duration.new!(second: 5)
      EctoPGMQ.create_queue(Repo, "my_queue", %{notifications: throttle})

      # Validate that queue has been created
      assert %Queue{
               name: "my_queue",
               created_at: %DateTime{},
               partitioned?: false,
               unlogged?: false,
               metrics: %Metrics{},
               notifications: %Throttle{throttle: ^throttle}
             } = EctoPGMQ.get_queue(Repo, "my_queue")
    end

    test "will create a queue with an index to optimize message group reads" do
      # Validate that queue does not exist
      assert EctoPGMQ.get_queue(Repo, "my_queue") == nil

      EctoPGMQ.create_queue(Repo, "my_queue", %{message_groups?: true})

      # Validate that queue has been created
      assert %Queue{} = EctoPGMQ.get_queue(Repo, "my_queue")

      # Validate that index has been created
      assert fifo_index?(Repo, "my_queue")
    end
  end

  describe "drop_queue/3" do
    test "will drop a queue", ctx do
      # Validate that queue exists
      assert %Queue{} = EctoPGMQ.get_queue(Repo, ctx.queue)

      EctoPGMQ.drop_queue(Repo, ctx.queue)

      # Validate that queue has been dropped
      assert EctoPGMQ.get_queue(Repo, ctx.queue) == nil
    end
  end

  describe "get_queue/3" do
    test "will get a queue", ctx do
      queue = ctx.queue
      throttle = Duration.new!(second: 5)
      EctoPGMQ.update_queue(Repo, queue, %{notifications: throttle})

      assert %Queue{
               name: ^queue,
               created_at: %DateTime{},
               partitioned?: false,
               unlogged?: false,
               metrics: %Metrics{},
               notifications: %Throttle{throttle: ^throttle}
             } = EctoPGMQ.get_queue(Repo, queue)
    end

    @tag :no_default_queue
    test "will not get a missing queue" do
      assert EctoPGMQ.get_queue(Repo, "my_fake_queue") == nil
    end
  end

  describe "purge_queue/3" do
    test "will purge messages from a queue", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Validate that messages are in the queue
      refute queue_empty?(Repo, ctx.queue)

      # Validate that the purged message count is correct
      assert EctoPGMQ.purge_queue(Repo, ctx.queue) == 2

      # Validate that messages have been removed from the queue
      assert queue_empty?(Repo, ctx.queue)
    end
  end

  describe "update_queue/4" do
    test "will update notifications for a queue", ctx do
      # Validate that notifications are disabled
      assert %Queue{notifications: nil} = EctoPGMQ.get_queue(Repo, ctx.queue)

      throttle = Duration.new!(second: 5)
      EctoPGMQ.update_queue(Repo, ctx.queue, %{notifications: throttle})

      # Validate that notifications have been enabled
      assert %Queue{notifications: %Throttle{throttle: ^throttle}} = EctoPGMQ.get_queue(Repo, ctx.queue)

      throttle = Duration.new!(second: 10)
      EctoPGMQ.update_queue(Repo, ctx.queue, %{notifications: throttle})

      # Validate that notification throttle has been updated
      assert %Queue{notifications: %Throttle{throttle: ^throttle}} = EctoPGMQ.get_queue(Repo, ctx.queue)

      EctoPGMQ.update_queue(Repo, ctx.queue, %{notifications: nil})

      # Validate that notifications have been disabled
      assert %Queue{notifications: nil} = EctoPGMQ.get_queue(Repo, ctx.queue)
    end

    test "will create an index to optimize message group reads", ctx do
      # Validate that index does not exist
      refute fifo_index?(Repo, ctx.queue)

      EctoPGMQ.update_queue(Repo, ctx.queue, %{message_groups?: true})

      # Validate that index has been created
      assert fifo_index?(Repo, ctx.queue)

      # Validate that attempting to recreate index succeeds
      EctoPGMQ.update_queue(Repo, ctx.queue, %{message_groups?: true})

      # Validate that index still exists
      assert fifo_index?(Repo, ctx.queue)
    end
  end

  describe "archive_messages/4" do
    test "will archive messages from a queue", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Validate that messages are in the queue
      refute queue_empty?(Repo, ctx.queue)

      EctoPGMQ.archive_messages(Repo, ctx.queue, message_ids)

      # Validate that messages have been removed from the queue
      assert queue_empty?(Repo, ctx.queue)

      # Validate that all of the messages have been archived
      assert Repo
             |> all_archive_messages(ctx.queue)
             |> same_messages?(message_ids, message_specs)
    end
  end

  describe "delete_messages/4" do
    test "will delete messages from a queue", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Validate that messages are in the queue
      refute queue_empty?(Repo, ctx.queue)

      EctoPGMQ.delete_messages(Repo, ctx.queue, message_ids)

      # Validate that messages have been deleted
      assert queue_empty?(Repo, ctx.queue)
    end
  end

  describe "read_messages/5" do
    test "will read messages from a queue without grouping", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = EctoPGMQ.read_messages(Repo, ctx.queue, 300, 2)

      # Validate that messages are no longer visible
      assert EctoPGMQ.read_messages(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end

    @tag default_queue_attributes: %{message_groups?: true}
    test "will read messages from a queue with throughput-optimized grouping", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = EctoPGMQ.read_messages(Repo, ctx.queue, 300, 2, message_grouping: :throughput_optimized)

      # Validate that messages are no longer visible
      assert EctoPGMQ.read_messages(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end

    @tag default_queue_attributes: %{message_groups?: true}
    test "will read messages from a queue with round-robin grouping", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = EctoPGMQ.read_messages(Repo, ctx.queue, 300, 2, message_grouping: :round_robin)

      # Validate that messages are no longer visible
      assert EctoPGMQ.read_messages(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end

    test "will read messages with a poll from a queue without grouping", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      poll_config = {Duration.new!(microsecond: {500_000, 3}), Duration.new!(second: 1)}
      response = EctoPGMQ.read_messages(Repo, ctx.queue, 300, 2, polling: poll_config)

      # Validate that messages are no longer visible
      assert EctoPGMQ.read_messages(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end

    @tag default_queue_attributes: %{message_groups?: true}
    test "will read messages with a poll from a queue with throughput-optimized grouping", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      poll_config = {Duration.new!(microsecond: {500_000, 3}), Duration.new!(second: 1)}
      opts = [polling: poll_config, message_grouping: :throughput_optimized]
      response = EctoPGMQ.read_messages(Repo, ctx.queue, 300, 2, opts)

      # Validate that messages are no longer visible
      assert EctoPGMQ.read_messages(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end

    @tag default_queue_attributes: %{message_groups?: true}
    test "will read messages with a poll from a queue with round-robin grouping", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      poll_config = {Duration.new!(microsecond: {500_000, 3}), Duration.new!(second: 1)}
      opts = [polling: poll_config, message_grouping: :round_robin]
      response = EctoPGMQ.read_messages(Repo, ctx.queue, 300, 2, opts)

      # Validate that messages are no longer visible
      assert EctoPGMQ.read_messages(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end

    test "will read messages from a queue without grouping and immediately delete them", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)

      # Validate that messages are in the queue
      refute queue_empty?(Repo, ctx.queue)

      response = EctoPGMQ.read_messages(Repo, ctx.queue, 300, 2, delete?: true)

      # Validate that the queue is empty
      assert queue_empty?(Repo, ctx.queue)

      # Validate that the response contains the expected records
      # Note that visibility timeout and read count don't change
      assert messages == response
    end

    test "will read messages with a Duration visibility timeout", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      visibility_timeout = Duration.new!(second: 300)
      response = EctoPGMQ.read_messages(Repo, ctx.queue, visibility_timeout, 2)

      # Validate that messages are no longer visible
      assert EctoPGMQ.read_messages(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end

    test "will read messages with a custom payload type", ctx do
      today = Date.utc_today()
      range = Date.range(today, Date.shift(today, day: 25), 5)
      message_specs = [Message.build(range), Message.build(range)]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs, payload_type: TestType)
      messages = all_queue_messages(Repo, ctx.queue, TestType)
      response = EctoPGMQ.read_messages(Repo, ctx.queue, 300, 2, payload_type: TestType)

      # Validate that messages are no longer visible
      assert EctoPGMQ.read_messages(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end
  end

  describe "send_messages/4" do
    test "will send messages to a queue", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Validate that all of the messages are in the queue
      assert Repo
             |> all_queue_messages(ctx.queue)
             |> same_messages?(message_ids, message_specs)
    end

    test "will send messages with groups to a queue", ctx do
      message_specs = [
        Message.build(%{"id" => 1}, "foo"),
        Message.build(%{"id" => 2})
      ]

      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Validate that all of the messages are in the queue
      assert Repo
             |> all_queue_messages(ctx.queue)
             |> same_messages?(message_ids, message_specs)
    end

    test "will send messages with groups and headers to a queue", ctx do
      message_specs = [
        Message.build(%{"id" => 1}, "foo", %{EctoPGMQ.PGMQ.group_header() => "bar"}),
        Message.build(%{"id" => 2}, nil, %{EctoPGMQ.PGMQ.group_header() => "bar"}),
        Message.build(%{"id" => 3}, "foo", %{"header" => "baz"}),
        Message.build(%{"id" => 4}, nil, %{"header" => "baz"}),
        Message.build(%{"id" => 5}, "foo"),
        Message.build(%{"id" => 6})
      ]

      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Validate that all of the messages are in the queue
      assert Repo
             |> all_queue_messages(ctx.queue)
             |> same_messages?(message_ids, message_specs)
    end

    test "will send messages to a queue with a duration delay", ctx do
      delay = Duration.new!(second: 300)
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      now = DateTime.utc_now()
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs, delay: delay)
      messages = all_queue_messages(Repo, ctx.queue)

      # Validate that all of the messages are in the queue
      assert same_messages?(messages, message_ids, message_specs)

      # Validate that the delay is applied correctly
      assert Enum.all?(messages, fn message ->
               diff = DateTime.diff(message.visible_at, now)
               diff > 0 and diff <= 300
             end)
    end

    test "will send messages with a custom payload type to a queue", ctx do
      today = Date.utc_today()
      range = Date.range(today, Date.shift(today, day: 25), 5)
      message_specs = [Message.build(range), Message.build(range)]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs, payload_type: TestType)

      # Validate that all of the messages are in the queue
      assert Repo
             |> all_queue_messages(ctx.queue, TestType)
             |> same_messages?(message_ids, message_specs)
    end
  end

  describe "update_messages/5" do
    test "will update message visibility timeouts", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      EctoPGMQ.update_messages(Repo, ctx.queue, message_ids, %{visibility_timeout: 300})

      # Validate that invisible messages will not be read
      assert EctoPGMQ.read_messages(Repo, ctx.queue, 300, 1) == []

      # Validate that messages have been updated correctly
      assert Repo
             |> all_queue_messages(ctx.queue)
             |> updated_messages?(messages, 300, 2)
    end
  end
end
