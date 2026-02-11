defmodule EctoPGMQ.MessageTest do
  use EctoPGMQ.TestCase, async: true

  alias EctoPGMQ.TestType

  require Ecto.Query

  doctest Message, import: true

  describe "archive_query/1" do
    test "will return a query for archived messages", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      EctoPGMQ.archive_messages(Repo, ctx.queue, message_ids)

      # Validate that all of the messages are returned by the query
      assert ctx.queue
             |> Message.archive_query()
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end

    test "will return a query for archived messages with a custom payload type", ctx do
      today = Date.utc_today()
      range = Date.range(today, Date.shift(today, day: 25), 5)
      message_specs = [Message.build(range), Message.build(range)]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs, payload_type: TestType)
      EctoPGMQ.archive_messages(Repo, ctx.queue, message_ids)

      # Validate that all of the messages are returned by the query
      assert ctx.queue
             |> Message.archive_query(payload_type: TestType)
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end
  end

  describe "queue_query/2" do
    test "will return a query for queue messages", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Validate that all of the messages are returned by the query
      assert ctx.queue
             |> Message.queue_query()
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end

    test "will return a query with the same structure as an archive query", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      ([_ | archive_ids] = message_ids) = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      EctoPGMQ.archive_messages(Repo, ctx.queue, archive_ids)

      # Validate that queue query can be unioned with archive query
      # Note that we manually specify sorting due to an Ecto Query quirk and
      # that the sorting column name does NOT match the schema field name
      assert ctx.queue
             |> Message.queue_query(archived_at?: true)
             |> Ecto.Query.union_all(^Message.archive_query(ctx.queue))
             |> Ecto.Query.order_by(fragment("msg_id"))
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end

    test "will return a query for queue messages with a custom payload type", ctx do
      today = Date.utc_today()
      range = Date.range(today, Date.shift(today, day: 25), 5)
      message_specs = [Message.build(range), Message.build(range)]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs, payload_type: TestType)

      # Validate that all of the messages are returned by the query
      assert ctx.queue
             |> Message.queue_query(payload_type: TestType)
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end
  end
end
