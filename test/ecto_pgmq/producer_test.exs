defmodule EctoPGMQ.ProducerTest do
  use EctoPGMQ.TestCase

  import Ecto.Query, only: [where: 3]

  alias EctoPGMQ.Message

  test "will consume messages from a queue", ctx do
    start_link_supervised!({EctoPGMQ.TestPipeline, [queue: ctx.queue]})
    pid = :erlang.pid_to_list(self())

    # Validate that producer can read and acknowledge messages
    payloads = [Message.build(%{"pid" => pid})]
    [message_id] = EctoPGMQ.send_messages(Repo, ctx.queue, payloads)

    assert_receive {:success, %Message{id: ^message_id}}
    assert with_poll(fn -> queue_empty?(Repo, ctx.queue) end)

    # Validate that producer can configure acknowledgements
    payloads = [Message.build(%{"pid" => pid, "archive" => true})]
    [message_id] = EctoPGMQ.send_messages(Repo, ctx.queue, payloads)

    assert_receive {:success, %Message{id: ^message_id}}
    assert with_poll(fn -> queue_empty?(Repo, ctx.queue) end)
    assert with_poll(fn -> archived_message_exists?(ctx.queue, message_id) end)

    # Validate that producer can acknowledge failed messages
    payloads = [Message.build(%{"pid" => pid, "fail" => true})]
    [message_id] = EctoPGMQ.send_messages(Repo, ctx.queue, payloads)

    assert_receive {:failure, %Message{id: ^message_id}}
    assert with_poll(fn -> queue_empty?(Repo, ctx.queue) end)
    assert with_poll(fn -> archived_message_exists?(ctx.queue, message_id) end)
  end

  ################################
  # Private API
  ################################

  defp with_poll(fun, interval \\ 500, retries \\ 5)
  defp with_poll(fun, _, 0), do: fun.()

  defp with_poll(fun, interval, retries) do
    # https://hexdocs.pm/elixir/1.10.4/Kernel.html#module-truthy-and-falsy-values
    with result when result in [false, nil] <- fun.() do
      :timer.sleep(interval)
      with_poll(fun, interval, retries - 1)
    end
  end

  defp archived_message_exists?(queue, message_id) do
    queue
    |> Message.archive_query()
    |> where([m], m.id == ^message_id)
    |> Repo.exists?()
  end
end
