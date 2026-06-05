defmodule EctoPGMQ.PGMQPollTest do
  @moduledoc """
  A test module for PGMQ functions that involve Postgres server-side polling.

  The tests in this module use a slightly weaker assertion than other read
  operation tests because attempting to fetch messages before they're read by
  the server side-poll leads to an unpredictable race condition. This is not an
  issue since, as already mentioned, the stronger assertion is used in the other
  read operation tests.
  """

  use EctoPGMQ.TestCase

  alias EctoPGMQ.PGMQ

  describe "read_grouped_head_with_poll/7" do
    test "will poll until messages are available", ctx do
      # Start a 30 second poll operation
      task =
        Task.async(fn ->
          PGMQ.read_grouped_head_with_poll(Repo, ctx.queue.name, 300, 2, 30)
        end)

      messages = [Message.build(%{"id" => 1}, "foo"), Message.build(%{"id" => 2}, "bar")]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages)

      assert {:ok, message_ids} = Map.fetch(response, ctx.queue.name)

      # Wait up to 5 seconds for a response
      response = Task.await(task)

      # Validate that the response contains the expected records
      assert same_messages?(response, message_ids, messages)
    end
  end

  describe "read_grouped_rr_with_poll/7" do
    test "will poll until messages are available", ctx do
      # Start a 30 second poll operation
      task =
        Task.async(fn ->
          PGMQ.read_grouped_rr_with_poll(Repo, ctx.queue.name, 300, 2, 30)
        end)

      messages = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages)

      assert {:ok, message_ids} = Map.fetch(response, ctx.queue.name)

      # Wait up to 5 seconds for a response
      response = Task.await(task)

      # Validate that the response contains the expected records
      assert same_messages?(response, message_ids, messages)
    end
  end

  describe "read_grouped_with_poll/7" do
    test "will poll until messages are available", ctx do
      # Start a 30 second poll operation
      task =
        Task.async(fn ->
          PGMQ.read_grouped_with_poll(Repo, ctx.queue.name, 300, 3, 30)
        end)

      messages = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages)

      assert {:ok, message_ids} = Map.fetch(response, ctx.queue.name)

      # Wait up to 5 seconds for a response
      response = Task.await(task)

      # Validate that the response contains the expected records
      assert same_messages?(response, message_ids, messages)
    end
  end

  describe "read_with_poll/8" do
    test "will poll until messages are available", ctx do
      # Start a 30 second poll operation
      task =
        Task.async(fn ->
          PGMQ.read_with_poll(Repo, ctx.queue.name, 300, 3, 30)
        end)

      messages = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      response = EctoPGMQ.send_messages(Repo, ctx.queue.name, messages)

      assert {:ok, message_ids} = Map.fetch(response, ctx.queue.name)

      # Wait up to 5 seconds for a response
      response = Task.await(task)

      # Validate that the response contains the expected records
      assert same_messages?(response, message_ids, messages)
    end
  end
end
