defmodule EctoPGMQ.ThrottleTest do
  use EctoPGMQ.TestCase, async: true

  import Ecto.Query

  alias EctoPGMQ.Throttle

  doctest Throttle, import: true

  @moduletag queue_attributes: %{notifications: 250}

  describe "query/0" do
    @tag queue: false
    test "will return a query for notification throttles" do
      queue_1 = EctoPGMQ.create_queue(Repo, "my_queue_1", %{notifications: 250})
      queue_2 = EctoPGMQ.create_queue(Repo, "my_queue_2", %{notifications: 250})

      # Validate that all notification throttles are returned by the query
      assert [queue_1, queue_2]
             |> Enum.map(fn queue -> queue.notifications end)
             |> same_elements?(Repo.all(Throttle.query()))
    end

    test "will allow filtering on interval field with a Duration struct", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)

      # Validate that Duration struct can be used in query
      assert Throttle.query()
             |> where([t], t.interval >= ^Duration.new!(second: 0))
             |> Repo.exists?()
    end

    test "will allow filtering on interval field with an integer time", ctx do
      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, message_specs)

      # Validate that integer time can be used in query
      assert Throttle.query()
             |> where([t], t.interval >= 0)
             |> Repo.exists?()
    end
  end
end
