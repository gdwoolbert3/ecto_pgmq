defmodule EctoPGMQ.ThrottleTest do
  use EctoPGMQ.TestCase, async: true

  import Ecto.Query

  alias EctoPGMQ.Throttle

  doctest Throttle, import: true

  @moduletag queue_attributes: %{notifications: 250}

  describe "query/0" do
    @tag queue: false
    test "will return a query for notification throttles" do
      %{notifications: throttle_1} = EctoPGMQ.create_queue(Repo, "my_queue_1", %{notifications: 250})
      %{notifications: throttle_2} = EctoPGMQ.create_queue(Repo, "my_queue_2", %{notifications: 250})

      # Validate that all notification throttles are returned by the query
      assert Throttle.query()
             |> Repo.all()
             |> same_elements?([throttle_1, throttle_2])
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
