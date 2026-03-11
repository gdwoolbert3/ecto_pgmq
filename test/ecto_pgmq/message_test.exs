defmodule EctoPGMQ.MessageTest do
  use EctoPGMQ.TestCase, async: true

  import Ecto.Query

  alias EctoPGMQ.PGMQ
  alias EctoPGMQ.TestType

  doctest Message, import: true

  describe "archive_query/1" do
    test "will return a query for archived messages", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      EctoPGMQ.archive_messages(Repo, ctx.queue.name, message_ids)

      # Validate that all of the messages are returned by the query
      assert ctx.queue.name
             |> Message.archive_query()
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end

    test "will return a query for archived messages with a custom payload type", ctx do
      today = Date.utc_today()
      range = Date.range(today, Date.shift(today, day: 25), 5)
      message_specs = [Message.build(range), Message.build(range)]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs, payload_type: TestType)
      EctoPGMQ.archive_messages(Repo, ctx.queue.name, message_ids)

      # Validate that all of the messages are returned by the query
      assert ctx.queue.name
             |> Message.archive_query(payload_type: TestType)
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end

    test "will allow filtering on payload field with a custom payload type", ctx do
      today = Date.utc_today()
      range = Date.range(today, Date.shift(today, day: 25), 5)
      message_specs = [Message.build(range), Message.build(range)]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs, payload_type: TestType)
      EctoPGMQ.archive_messages(Repo, ctx.queue.name, message_ids)

      # Validate that custom payload can be used in query
      assert ctx.queue.name
             |> Message.archive_query(payload_type: TestType)
             |> where([m], m.payload == type(^range, TestType))
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end

    test "will allow filtering on archive timestamp field", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      EctoPGMQ.archive_messages(Repo, ctx.queue.name, message_ids)
      cutoff = DateTime.utc_now()

      # Validate that archive timestamp filter can be used in query
      assert ctx.queue.name
             |> Message.archive_query()
             |> where([m], m.archived_at <= ^cutoff)
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end
  end

  describe "queue_query/2" do
    test "will return a query for queue messages", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)

      # Validate that all of the messages are returned by the query
      assert ctx.queue.name
             |> Message.queue_query()
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end

    test "will return a query with the same structure as an archive query", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      ([_ | archive_ids] = message_ids) = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)
      EctoPGMQ.archive_messages(Repo, ctx.queue.name, archive_ids)

      # Validate that queue query can be unioned with archive query
      # Note that we manually specify sorting due to an Ecto Query quirk and
      # that the sorting column name does NOT match the schema field name
      assert ctx.queue.name
             |> Message.queue_query(archived_at?: true)
             |> union_all(^Message.archive_query(ctx.queue.name))
             |> order_by(fragment("msg_id"))
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end

    test "will return a query for queue messages with a custom payload type", ctx do
      today = Date.utc_today()
      range = Date.range(today, Date.shift(today, day: 25), 5)
      message_specs = [Message.build(range), Message.build(range)]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs, payload_type: TestType)

      # Validate that all of the messages are returned by the query
      assert ctx.queue.name
             |> Message.queue_query(payload_type: TestType)
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end

    test "will allow filtering on payload field with a custom payload type", ctx do
      today = Date.utc_today()
      range = Date.range(today, Date.shift(today, day: 25), 5)
      message_specs = [Message.build(range), Message.build(range)]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs, payload_type: TestType)

      # Validate that custom payload can be used in query
      assert ctx.queue.name
             |> Message.queue_query(payload_type: TestType)
             |> where([m], m.payload == type(^range, TestType))
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end
  end

  describe "build/3" do
    @describetag queue: false

    test "will override group in headers with given group" do
      headers = %{PGMQ.group_header() => "bar"}
      spec = Message.build(%{"id" => 1}, "foo", headers)

      assert {:spec, %{"id" => 1}, "foo", headers} = spec
      assert Map.fetch!(headers, PGMQ.group_header()) == "foo"
    end

    test "will extract group from headers if not specified" do
      headers = %{PGMQ.group_header() => "foo"}
      spec = Message.build(%{"id" => 1}, nil, headers)

      assert spec == {:spec, %{"id" => 1}, "foo", headers}
    end
  end

  describe "group/1" do
    test "will extract the group from a message", ctx do
      message_specs = [Message.build(%{"id" => 1}, "foo")]
      [message_id] = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)

      assert ctx.queue.name
             |> Message.queue_query()
             |> Repo.get(message_id)
             |> Message.group() == "foo"
    end

    test "will not extract the group from a message without a group", ctx do
      message_specs = [Message.build(%{"id" => 1}, nil, %{"my_header" => "bar"})]
      [message_id] = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)

      assert ctx.queue.name
             |> Message.queue_query()
             |> Repo.get(message_id)
             |> Message.group()
             |> is_nil()
    end

    test "will not extract the group from a message without headers", ctx do
      message_specs = [Message.build(%{"id" => 1})]
      [message_id] = EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)

      assert ctx.queue.name
             |> Message.queue_query()
             |> Repo.get(message_id)
             |> Message.group()
             |> is_nil()
    end

    test "will extract the group from a message spec" do
      message_spec = Message.build(%{"id" => 1}, "foo")

      assert Message.group(message_spec) == "foo"
    end

    test "will not extract the group from a message spec without a group" do
      message_spec = Message.build(%{"id" => 1}, nil, %{"my_header" => "bar"})

      assert Message.group(message_spec) == nil
    end

    test "will not extract the group from a message spec without headers" do
      message_spec = Message.build(%{"id" => 1})

      assert Message.group(message_spec) == nil
    end
  end
end
