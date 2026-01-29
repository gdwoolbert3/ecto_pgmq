defmodule EctoPGMQ.QueueTest do
  use EctoPGMQ.TestCase, async: true

  alias EctoPGMQ.Queue

  doctest Queue, import: true

  @moduletag :no_default_queue

  describe "query/0" do
    test "will return a query for queues" do
      queue_1 = EctoPGMQ.create_queue(Repo, "my_queue_1")
      queue_2 = EctoPGMQ.create_queue(Repo, "my_queue_2")

      # Validate that the response contains the expected records
      assert Queue.query()
             |> Repo.all()
             |> same_elements?([queue_1, queue_2])
    end
  end
end
