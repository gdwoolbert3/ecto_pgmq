defmodule EctoPGMQ.ProducerTest do
  use EctoPGMQ.TestCase

  import Ecto.Query, only: [where: 3]

  alias EctoPGMQ.Message
  alias EctoPGMQ.TestPipeline

  @tag queue_attributes: %{notifications: 0}
  test "will consume messages from a queue", ctx do
    start_link_supervised!({TestPipeline, [queue: ctx.queue.name]})
    encoded_pid = TestPipeline.get_encoded_pid()

    # Validate that producer can read and acknowledge messages
    payloads = [Message.build(%{"encoded_pid" => encoded_pid})]
    [message_id] = EctoPGMQ.send_messages(Repo, ctx.queue.name, payloads)

    assert_receive {:success, ^message_id}
    assert poll_queue_empty?(ctx.queue.name)
    refute archived_message_exists?(ctx.queue.name, message_id)

    # Validate that producer can configure acknowledgements
    payloads = [Message.build(%{"encoded_pid" => encoded_pid, "archive" => true})]
    [message_id] = EctoPGMQ.send_messages(Repo, ctx.queue.name, payloads)

    assert_receive {:success, ^message_id}
    assert poll_queue_empty?(ctx.queue.name)
    assert archived_message_exists?(ctx.queue.name, message_id)

    # Validate that producer can acknowledge failed messages
    payloads = [Message.build(%{"encoded_pid" => encoded_pid, "fail" => true})]
    [message_id] = EctoPGMQ.send_messages(Repo, ctx.queue.name, payloads)

    assert_receive {:failure, ^message_id}
    assert poll_queue_empty?(ctx.queue.name)
    assert archived_message_exists?(ctx.queue.name, message_id)
  end

  ################################
  # Private API
  ################################

  defp poll_queue_empty?(queue) do
    with_poll(fn -> queue_empty?(Repo, queue) end)
  end

  defp archived_message_exists?(queue, message_id) do
    queue
    |> Message.archive_query()
    |> where([m], m.id == ^message_id)
    |> Repo.exists?()
  end
end
