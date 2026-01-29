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

  describe "read_grouped_rr_with_poll/7" do
    test "will poll until messages are available", ctx do
      # Start a 30 second poll operation
      task =
        Task.async(fn ->
          PGMQ.read_grouped_rr_with_poll(Repo, ctx.queue, 300, 2, 30)
        end)

      message_specs = [%{"id" => 1}, %{"id" => 2}]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Wait up to 5 seconds for a response
      response = Task.await(task)

      # Validate that the response contains the expected records
      assert same_messages?(response, message_ids, message_specs)
    end
  end

  describe "read_grouped_with_poll/7" do
    test "will poll until messages are available", ctx do
      # Start a 30 second poll operation
      task =
        Task.async(fn ->
          PGMQ.read_grouped_with_poll(Repo, ctx.queue, 300, 3, 30)
        end)

      message_specs = [%{"id" => 1}, %{"id" => 2}]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Wait up to 5 seconds for a response
      response = Task.await(task)

      # Validate that the response contains the expected records
      assert same_messages?(response, message_ids, message_specs)
    end
  end

  describe "read_with_poll/8" do
    test "will poll until messages are available", ctx do
      # Start a 30 second poll operation
      task =
        Task.async(fn ->
          PGMQ.read_with_poll(Repo, ctx.queue, 300, 3, 30)
        end)

      message_specs = [%{"id" => 1}, %{"id" => 2}]
      message_ids = EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Wait up to 5 seconds for a response
      response = Task.await(task)

      # Validate that the response contains the expected records
      assert same_messages?(response, message_ids, message_specs)
    end
  end
end
