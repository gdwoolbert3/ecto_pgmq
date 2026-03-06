defmodule EctoPGMQ.MetricsTest do
  use EctoPGMQ.TestCase, async: true

  import Ecto.Query

  alias EctoPGMQ.Metrics

  doctest Metrics, import: true

  describe "query/0" do
    @tag queue: false
    test "will return a query for queue metrics" do
      %{metrics: metrics_1} = EctoPGMQ.create_queue(Repo, "my_queue_1")
      %{metrics: metrics_2} = EctoPGMQ.create_queue(Repo, "my_queue_2")
      metrics = Repo.all(Metrics.query())

      # Validate that all queue metrics are returned by the query
      assert [metrics_1, metrics_2]
             |> Enum.map(fn m -> %{m | requested_at: nil} end)
             |> same_elements?(Enum.map(metrics, fn m -> %{m | requested_at: nil} end))
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
