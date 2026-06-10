defmodule EctoPGMQ.MessageTest do
  use EctoPGMQ.TestCase, async: true

  import Ecto.Query

  alias EctoPGMQ.PGMQ
  alias EctoPGMQ.TestType

  doctest Message

  describe "archive_query/1" do
    test "will return a query for archived messages", ctx do
      messages = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages)

      assert {:ok, message_ids} = Map.fetch(response, ctx.queue.name)

      EctoPGMQ.archive_messages(Repo, ctx.queue.name, message_ids)

      # Validate that all of the messages are returned by the query
      assert ctx.queue.name
             |> Message.archive_query()
             |> Repo.all()
             |> same_messages?(message_ids, messages)
    end

    test "will return a query for archived messages with a custom payload type", ctx do
      today = Date.utc_today()
      range = Date.range(today, Date.shift(today, day: 25), 5)
      messages = [Message.build(range), Message.build(range)]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages, payload_type: TestType)

      assert {:ok, message_ids} = Map.fetch(response, ctx.queue.name)

      EctoPGMQ.archive_messages(Repo, ctx.queue.name, message_ids)

      # Validate that all of the messages are returned by the query
      assert ctx.queue.name
             |> Message.archive_query(payload_type: TestType)
             |> Repo.all()
             |> same_messages?(message_ids, messages)
    end

    test "will allow filtering on payload field with a custom payload type", ctx do
      today = Date.utc_today()
      range = Date.range(today, Date.shift(today, day: 25), 5)
      messages = [Message.build(range), Message.build(range)]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages, payload_type: TestType)

      assert {:ok, message_ids} = Map.fetch(response, ctx.queue.name)

      EctoPGMQ.archive_messages(Repo, ctx.queue.name, message_ids)

      # Validate that custom payload can be used in query
      assert ctx.queue.name
             |> Message.archive_query(payload_type: TestType)
             |> where([m], m.payload == type(^range, TestType))
             |> Repo.all()
             |> same_messages?(message_ids, messages)
    end

    test "will allow filtering on archive timestamp field", ctx do
      messages = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages)

      assert {:ok, message_ids} = Map.fetch(response, ctx.queue.name)

      EctoPGMQ.archive_messages(Repo, ctx.queue.name, message_ids)
      cutoff = DateTime.shift(DateTime.utc_now(), second: 5)

      # Validate that archive timestamp filter can be used in query
      assert ctx.queue.name
             |> Message.archive_query()
             |> where([m], m.archived_at <= ^cutoff)
             |> Repo.all()
             |> same_messages?(message_ids, messages)
    end
  end

  describe "queue_query/2" do
    test "will return a query for queue messages", ctx do
      messages = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages)

      assert {:ok, message_ids} = Map.fetch(response, ctx.queue.name)

      # Validate that all of the messages are returned by the query
      assert ctx.queue.name
             |> Message.queue_query()
             |> Repo.all()
             |> same_messages?(message_ids, messages)
    end

    test "will return a query with the same structure as an archive query", ctx do
      messages = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages)

      assert {:ok, [_ | archive_ids] = message_ids} = Map.fetch(response, ctx.queue.name)

      EctoPGMQ.archive_messages(Repo, ctx.queue.name, archive_ids)

      # Validate that queue query can be unioned with archive query
      # Note that we manually specify sorting due to an Ecto Query quirk and
      # that the sorting column name does NOT match the schema field name
      assert ctx.queue.name
             |> Message.queue_query(archived_at?: true)
             |> union_all(^Message.archive_query(ctx.queue.name))
             |> order_by(fragment("msg_id"))
             |> Repo.all()
             |> same_messages?(message_ids, messages)
    end

    test "will return a query for queue messages with a custom payload type", ctx do
      today = Date.utc_today()
      range = Date.range(today, Date.shift(today, day: 25), 5)
      messages = [Message.build(range), Message.build(range)]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages, payload_type: TestType)

      assert {:ok, message_ids} = Map.fetch(response, ctx.queue.name)

      # Validate that all of the messages are returned by the query
      assert ctx.queue.name
             |> Message.queue_query(payload_type: TestType)
             |> Repo.all()
             |> same_messages?(message_ids, messages)
    end

    test "will allow filtering on payload field with a custom payload type", ctx do
      today = Date.utc_today()
      range = Date.range(today, Date.shift(today, day: 25), 5)
      messages = [Message.build(range), Message.build(range)]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages, payload_type: TestType)

      assert {:ok, message_ids} = Map.fetch(response, ctx.queue.name)

      # Validate that custom payload can be used in query
      assert ctx.queue.name
             |> Message.queue_query(payload_type: TestType)
             |> where([m], m.payload == type(^range, TestType))
             |> Repo.all()
             |> same_messages?(message_ids, messages)
    end
  end

  describe "build/2" do
    @describetag queue: false

    test "will create a specification from a payload" do
      assert Message.build(%{"id" => 1}) == {:message, %{"id" => 1}, nil}
    end

    test "will create a specification from a payload and a group" do
      spec = Message.build(%{"id" => 1}, "A")

      assert spec == {:message, %{"id" => 1}, %{PGMQ.group_header() => "A"}}
    end

    test "will create a specification from a payload and headers" do
      spec = Message.build(%{"id" => 1}, %{"header" => "foo"})

      assert spec == {:message, %{"id" => 1}, %{"header" => "foo"}}
    end

    test "will create a specification from a paylod, a group, and headers" do
      spec = Message.build(%{"id" => 1}, "B", %{PGMQ.group_header() => "A", "header" => "foo"})

      assert spec == {:message, %{"id" => 1}, %{PGMQ.group_header() => "B", "header" => "foo"}}
    end
  end

  describe "group/1" do
    @describetag queue: false

    @tag queue: true
    test "will extract the group from a message", ctx do
      messages = [Message.build(%{"id" => 1}, "A")]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages)

      assert {:ok, [message_id]} = Map.fetch(response, ctx.queue.name)

      assert ctx.queue.name
             |> Message.queue_query()
             |> Repo.get(message_id)
             |> Message.group() == "A"
    end

    @tag queue: true
    test "will not extract the group from a message without a group", ctx do
      messages = [Message.build(%{"id" => 1}, %{"header" => "foo"})]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages)

      assert {:ok, [message_id]} = Map.fetch(response, ctx.queue.name)

      assert ctx.queue.name
             |> Message.queue_query()
             |> Repo.get(message_id)
             |> Message.group()
             |> is_nil()
    end

    @tag queue: true
    test "will not extract the group from a message without headers", ctx do
      messages = [Message.build(%{"id" => 1})]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages)

      assert {:ok, [message_id]} = Map.fetch(response, ctx.queue.name)

      assert ctx.queue.name
             |> Message.queue_query()
             |> Repo.get(message_id)
             |> Message.group()
             |> is_nil()
    end

    test "will extract the group from a message spec" do
      message_spec = Message.build(%{"id" => 1}, "A")

      assert Message.group(message_spec) == "A"
    end

    test "will not extract the group from a message spec without a group" do
      message_spec = Message.build(%{"id" => 1}, %{"header" => "foo"})

      assert Message.group(message_spec) == nil
    end

    test "will not extract the group from a message spec without headers" do
      message_spec = Message.build(%{"id" => 1})

      assert Message.group(message_spec) == nil
    end

    test "will extract the group from headers" do
      headers = %{PGMQ.group_header() => "A"}

      assert Message.group(headers) == "A"
    end

    test "will not extract the group from headers without a group" do
      headers = %{"header" => "foo"}

      assert Message.group(headers) == nil
    end

    test "will not extract a group from nothing" do
      assert Message.group(nil) == nil
    end
  end
end
