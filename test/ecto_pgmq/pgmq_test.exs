defmodule EctoPGMQ.PGMQTest do
  use EctoPGMQ.TestCase, async: true

  import Ecto.Query

  alias EctoPGMQ.Metrics
  alias EctoPGMQ.PGMQ
  alias EctoPGMQ.Queue
  alias EctoPGMQ.Throttle

  doctest PGMQ, import: true

  describe "archive/4" do
    test "will archive messages", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Validate that messages are in the queue
      refute queue_empty?(Repo, ctx.queue)

      PGMQ.archive(Repo, ctx.queue, message_ids)

      # Validate that messages have been removed from the queue
      assert queue_empty?(Repo, ctx.queue)

      # Validate that all of the messages have been archived
      assert Repo
             |> all_archive_messages(ctx.queue)
             |> same_messages?(message_ids, message_specs)
    end
  end

  describe "convert_archive_partitioned/6" do
    test "will partition a non-partitioned archive based on message ID", ctx do
      # Validate that archive is unpartitioned
      refute archive_partitioned?(Repo, ctx.queue)

      PGMQ.convert_archive_partitioned(Repo, ctx.queue)

      # Validate that archive is partitioned
      assert archive_partitioned?(Repo, ctx.queue)
    end

    test "will partition a non-partitioned archive based on a time interval", ctx do
      # Validate that archive is unpartitioned
      refute archive_partitioned?(Repo, ctx.queue)

      partition = Duration.new!(minute: 5)
      retention = Duration.new!(hour: 1)
      PGMQ.convert_archive_partitioned(Repo, ctx.queue, partition, retention)

      # Validate that archive is partitioned
      assert archive_partitioned?(Repo, ctx.queue)
    end
  end

  describe "create_fifo_index/3" do
    test "will create an index to optimize FIFO group reads", ctx do
      # Validate that index does not exist
      refute fifo_index?(Repo, ctx.queue)

      PGMQ.create_fifo_index(Repo, ctx.queue)

      # Validate that index has been created
      assert fifo_index?(Repo, ctx.queue)
    end
  end

  describe "create_non_partitioned/3" do
    @describetag :no_default_queue

    test "will create a non-partitioned queue" do
      # Validate that queue does not exist
      assert EctoPGMQ.get_queue(Repo, "my_unpartitioned_queue") == nil

      PGMQ.create_non_partitioned(Repo, "my_unpartitioned_queue")

      # Validate that queue has been created
      assert %Queue{
               name: "my_unpartitioned_queue",
               created_at: %DateTime{},
               is_partitioned: false,
               is_unlogged: false,
               metrics: %Metrics{},
               notifications: nil
             } = EctoPGMQ.get_queue(Repo, "my_unpartitioned_queue")
    end
  end

  describe "create_partitioned/5" do
    @describetag :no_default_queue

    test "will create a partitioned queue based on message ID" do
      # Validate that queue does not exist
      assert EctoPGMQ.get_queue(Repo, "my_partitioned_queue") == nil

      PGMQ.create_partitioned(Repo, "my_partitioned_queue")

      # Validate that queue has been created
      assert %Queue{
               name: "my_partitioned_queue",
               created_at: %DateTime{},
               is_partitioned: true,
               is_unlogged: false,
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
               is_partitioned: true,
               is_unlogged: false,
               metrics: %Metrics{},
               notifications: nil
             } = EctoPGMQ.get_queue(Repo, "my_partitioned_queue")
    end
  end

  describe "create_unlogged/3" do
    @describetag :no_default_queue

    test "will create an unlogged queue" do
      # Validate that queue does not exist
      assert EctoPGMQ.get_queue(Repo, "my_unlogged_queue") == nil

      PGMQ.create_unlogged(Repo, "my_unlogged_queue")

      # Validate that queue has been created
      assert %Queue{
               name: "my_unlogged_queue",
               created_at: %DateTime{},
               is_partitioned: false,
               is_unlogged: true,
               metrics: %Metrics{},
               notifications: nil
             } = EctoPGMQ.get_queue(Repo, "my_unlogged_queue")
    end
  end

  describe "delete/4" do
    test "will delete messages", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Validate that messages are in the queue
      refute queue_empty?(Repo, ctx.queue)

      PGMQ.delete(Repo, ctx.queue, message_ids)

      # Validate that messages have been deleted
      assert queue_empty?(Repo, ctx.queue)
    end
  end

  describe "disable_notify_insert/3" do
    @describetag default_queue_attributes: %{notifications: 250}

    test "will disable insert notifications", ctx do
      # Validate that notifications are enabled
      assert %Queue{notifications: %Throttle{}} = EctoPGMQ.get_queue(Repo, ctx.queue)

      PGMQ.disable_notify_insert(Repo, ctx.queue)

      # Validate that notifications have been disabled
      assert %Queue{notifications: nil} = EctoPGMQ.get_queue(Repo, ctx.queue)
    end
  end

  describe "drop_queue/3" do
    test "will drop a queue", ctx do
      # Validate that queue exists
      assert %Queue{} = EctoPGMQ.get_queue(Repo, ctx.queue)

      PGMQ.drop_queue(Repo, ctx.queue)

      # Validate that queue has been dropped
      assert EctoPGMQ.get_queue(Repo, ctx.queue) == nil
    end
  end

  describe "enable_notify_insert/4" do
    test "will enable insert notifications", ctx do
      # Validate that notifications are disabled
      assert %Queue{notifications: nil} = EctoPGMQ.get_queue(Repo, ctx.queue)

      PGMQ.enable_notify_insert(Repo, ctx.queue)

      # Validate that notifications have been enabled
      assert %Queue{notifications: %Throttle{}} = EctoPGMQ.get_queue(Repo, ctx.queue)
    end
  end

  describe "list_queues/2" do
    @describetag :no_default_queue

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

  describe "metrics_all/2" do
    @describetag :no_default_queue

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
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)

      # Validate that messages are in the queue
      refute queue_empty?(Repo, ctx.queue)

      response = PGMQ.pop(Repo, ctx.queue, 2)

      # Validate that the queue is empty
      assert queue_empty?(Repo, ctx.queue)

      # Validate that the response contains the expected records
      # Note that visibility timeout and read count don't change
      assert messages == response
    end
  end

  describe "purge_queue/3" do
    test "will purge messages from a queue", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Validate that messages are in the queue
      refute queue_empty?(Repo, ctx.queue)

      # Validate that the purged message count is correct
      assert PGMQ.purge_queue(Repo, ctx.queue) == 2

      # Validate that messages have been removed from the queue
      assert queue_empty?(Repo, ctx.queue)
    end
  end

  describe "read/6" do
    test "will read messages from a queue", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = PGMQ.read(Repo, ctx.queue, 300, 2)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end

    test "will read messages from a queue when too many are requested", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = PGMQ.read(Repo, ctx.queue, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end
  end

  describe "read_grouped/5" do
    @describetag default_queue_attributes: %{message_groups?: true}

    test "will read messages from a queue", ctx do
      message_specs = [{%{"id" => 1}, "foo"}, {%{"id" => 2}, "foo"}, {%{"id" => 3}, "bar"}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = PGMQ.read_grouped(Repo, ctx.queue, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 3)
    end

    test "will read messages from a queue when too many are requested", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = PGMQ.read_grouped(Repo, ctx.queue, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end
  end

  describe "read_grouped_rr/5" do
    @describetag default_queue_attributes: %{message_groups?: true}

    test "will read messages from a queue", ctx do
      message_specs = [{%{"id" => 1}, "foo"}, {%{"id" => 2}, "foo"}, {%{"id" => 3}, "bar"}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      [foo_1, foo_2, bar_1] = all_queue_messages(Repo, ctx.queue)
      response = PGMQ.read_grouped_rr(Repo, ctx.queue, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?([foo_1, bar_1, foo_2], response, 300, 3)
    end

    test "will read messages from a queue when too many are requested", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = PGMQ.read_grouped_rr(Repo, ctx.queue, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end
  end

  describe "read_grouped_rr_with_poll/7" do
    @describetag default_queue_attributes: %{message_groups?: true}

    test "will read messages from a queue", ctx do
      message_specs = [{%{"id" => 1}, "foo"}, {%{"id" => 2}, "foo"}, {%{"id" => 3}, "bar"}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      [foo_1, foo_2, bar_1] = all_queue_messages(Repo, ctx.queue)
      response = PGMQ.read_grouped_rr_with_poll(Repo, ctx.queue, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?([foo_1, bar_1, foo_2], response, 300, 3)
    end

    test "will read messages from a queue when too many are requested", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = PGMQ.read_grouped_rr_with_poll(Repo, ctx.queue, 300, 3, 1, 500)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end
  end

  describe "read_grouped_with_poll/7" do
    @describetag default_queue_attributes: %{message_groups?: true}

    test "will read messages from a queue", ctx do
      message_specs = [{%{"id" => 1}, "foo"}, {%{"id" => 2}, "foo"}, {%{"id" => 3}, "bar"}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = PGMQ.read_grouped_with_poll(Repo, ctx.queue, 300, 3)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 3)
    end

    test "will read messages from a queue when too many are requested", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = PGMQ.read_grouped_with_poll(Repo, ctx.queue, 300, 3, 1, 500)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end
  end

  describe "read_with_poll/8" do
    test "will read messages from a queue", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = PGMQ.read_with_poll(Repo, ctx.queue, 300, 2)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end

    test "will read messages from a queue when too many are requested", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = PGMQ.read_with_poll(Repo, ctx.queue, 300, 3, 1, 500)

      # Validate that messages are no longer visible
      assert PGMQ.read(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert read_messages?(messages, response, 300, 2)
    end
  end

  describe "send_batch/6" do
    test "will send messages to a queue with an integer delay", ctx do
      payloads = [%{"id" => 1}, %{"id" => 2}]
      message_ids = PGMQ.send_batch(Repo, ctx.queue, payloads)

      # Validate that all of the messages are in the queue
      assert Repo
             |> all_queue_messages(ctx.queue)
             |> same_messages?(message_ids, payloads)
    end

    test "will send messages to a queue with a timestamp delay", ctx do
      payloads = [%{"id" => 1}, %{"id" => 2}, %{"id" => 3}]
      message_ids = PGMQ.send_batch(Repo, ctx.queue, payloads, nil, DateTime.utc_now())

      # Validate that all of the messages are in the queue
      assert Repo
             |> all_queue_messages(ctx.queue)
             |> same_messages?(message_ids, payloads)
    end
  end

  describe "set_vt/5" do
    test "will update message visibility timeouts", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      messages = all_queue_messages(Repo, ctx.queue)
      response = PGMQ.set_vt(Repo, ctx.queue, message_ids, 300)

      # Validate that invisible messages will not be read
      assert PGMQ.read(Repo, ctx.queue, 300, 1) == []

      # Validate that the response contains the expected records
      assert updated_messages?(response, messages, 300, 2)
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
