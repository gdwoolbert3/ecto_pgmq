defmodule EctoPGMQ.MetricsTest do
  use EctoPGMQ.TestCase, async: true

  alias EctoPGMQ.Metrics

  doctest Metrics, import: true

  @moduletag :no_default_queue

  describe "query/0" do
    test "will return a query for queue metrics" do
      EctoPGMQ.create_queue(Repo, "my_queue_1")
      EctoPGMQ.create_queue(Repo, "my_queue_2")

      # Validate that all queue metrics are returned by the query
      assert Metrics.query()
             |> Repo.all()
             |> Enum.map(fn %Metrics{queue: queue} -> queue end)
             |> same_elements?(["my_queue_1", "my_queue_2"])
    end
  end
end
