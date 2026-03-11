defmodule EctoPGMQ.TestCase do
  @moduledoc """
  The entrypoint for defining `EctoPGMQ` unit tests.

  By default, `EctoPGMQ` unit tests create a default queue for each test.
  Asynchronous tests are wrapped in a transaction while synchronous tests are
  not. This means that, aside from the default queue, synchronous tests are
  responsible for their own cleanup.

  In addition to standard test tags, `EctoPGMQ` unit tests also support the
  following tags:

    * `:queue` - An optional `t:boolean/0` denoting whether or not to create a
      queue for the test. If `true`, the queue will be included in the test
      context under the `:queue` key. Defaults to `true`.

    * `:queue_attributes` - An optional `t:EctoPGMQ.queue_create_attributes/0`
      for the default queue. Defaults to `%{}`.
  """

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox
  alias EctoPGMQ.TestRepo

  @queue_name "my_queue"

  ################################
  # CaseTemplate Callbacks
  ################################

  using do
    quote do
      import EctoPGMQ.TestHelpers

      alias EctoPGMQ.Message
      alias EctoPGMQ.TestRepo, as: Repo
    end
  end

  ################################
  # ExUnit Callbacks
  ################################

  setup ctx do
    # Only use sandbox for async tests
    sandbox? = Map.fetch!(ctx, :async)
    queue_attrs = Map.get(ctx, :queue_attributes, %{})
    :ok = Sandbox.checkout(TestRepo, sandbox: sandbox?)

    cond do
      # Skip queue creation when specified
      not Map.get(ctx, :queue, true) ->
        :ok

      # Create default queue without teardown when specified
      sandbox? ->
        queue = EctoPGMQ.create_queue(TestRepo, @queue_name, queue_attrs)
        %{queue: queue}

      # Create default queue with teardown
      true ->
        queue = EctoPGMQ.create_queue(TestRepo, @queue_name, queue_attrs)
        on_exit(fn -> EctoPGMQ.drop_queue(TestRepo, @queue_name) end)
        %{queue: queue}
    end
  end
end
