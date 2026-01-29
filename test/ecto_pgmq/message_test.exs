defmodule EctoPGMQ.MessageTest do
  use EctoPGMQ.TestCase, async: true

  alias EctoPGMQ.Message

  require Ecto.Query

  doctest Message, import: true

  describe "archive_query/1" do
    test "will return a query for archived messages", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)
      EctoPGMQ.archive_messages(Repo, ctx.queue, message_ids)

      # Validate that all of the messages are returned by the query
      assert ctx.queue
             |> Message.archive_query()
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end
  end

  describe "queue_query/2" do
    test "will return a query for queue messages", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Validate that all of the messages are returned by the query
      assert ctx.queue
             |> Message.queue_query()
             |> Repo.all()
             |> same_messages?(message_ids, message_specs)
    end

    test "will return a query with the same structrure as an archive query", ctx do
      message_specs = [%{"id" => 1}, %{"id" => 2}]
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
  end
end
