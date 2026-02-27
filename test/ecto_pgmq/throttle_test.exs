defmodule EctoPGMQ.ThrottleTest do
  use EctoPGMQ.TestCase, async: true

  alias EctoPGMQ.Throttle

  doctest Throttle, import: true

  @moduletag default_queue_attributes: %{notifications: 250}

  # TODO(Gordon) - interval filtering tests after field name change

  describe "query/0" do
    @tag :no_default_queue
    test "will return a query for notification throttles" do
      queue_1 = EctoPGMQ.create_queue(Repo, "my_queue_1", %{notifications: 250})
      queue_2 = EctoPGMQ.create_queue(Repo, "my_queue_2", %{notifications: 250})

      # Validate that all notification throttles are returned by the query
      assert [queue_1, queue_2]
             |> Enum.map(fn queue -> queue.notifications end)
             |> same_elements?(Repo.all(Throttle.query()))
    end
  end
end
