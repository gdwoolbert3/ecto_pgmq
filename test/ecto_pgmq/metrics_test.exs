defmodule EctoPGMQ.MetricsTest do
  use EctoPGMQ.TestCase, async: true

  import Ecto.Query

  alias EctoPGMQ.Metrics

  doctest Metrics, import: true

  describe "query/0" do
    @tag queue: false
    test "will return a query for queue metrics" do
      EctoPGMQ.create_queue(Repo, "my_queue_1")
      EctoPGMQ.create_queue(Repo, "my_queue_2")

      # Validate that all queue metrics are returned by the query
      assert Metrics.query()
             |> Repo.all()
             |> Enum.map(fn %Metrics{queue: queue} -> queue end)
             |> same_elements?(["my_queue_1", "my_queue_2"])
    end

    test "will allow filtering on message age field with a Duration struct", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)

      # Validate that Duration struct can be used in query
      assert Metrics.query()
             |> where([m], m.oldest_message_age >= ^Duration.new!(second: 0))
             |> Repo.exists?()
    end

    test "will allow filtering on message age field with an integer time", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)

      # Validate that integer time can be used in query
      assert Metrics.query()
             |> where([m], m.oldest_message_age >= 0)
             |> Repo.exists?()
    end
  end
end
