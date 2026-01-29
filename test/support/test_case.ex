defmodule EctoPGMQ.TestCase do
  @moduledoc """
  The entrypoint for defining `EctoPGMQ` unit tests.

  By default, `EctoPGMQ` unit tests create a default queue for each test.
  Asynchronous tests are wrapped in a transaction while synchronous tests are
  not. This means that, aside from the default queue, synchronous tests are
  responsible for their own cleanup.

  In addition to standard test tags, `EctoPGMQ` unit tests also support the
  following tags:

    * `:no_default_queue` - An optional `t:boolean/0` denoting whether or not to
      skip default queue creation.

    * `:default_queue_attributes` - An optional
      `t:EctoPGMQ.queue_create_attributes/0` for the default queue. Defaults to
      `%{}`.
  """

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox
  alias EctoPGMQ.TestRepo

  @default_queue "my_queue"

  ################################
  # CaseTemplate Callbacks
  ################################

  using do
    quote do
      import EctoPGMQ.TestHelpers

      alias EctoPGMQ.TestRepo, as: Repo
    end
  end

  ################################
  # ExUnit Callbacks
  ################################

  setup tags do
    # Only use sandbox for async tests
    sandbox? = tags[:async]
    queue_attrs = Map.get(tags, :default_queue_attributes, %{})
    :ok = Sandbox.checkout(TestRepo, sandbox: sandbox?)

    cond do
      # Skip queue creations when specified
      tags[:no_default_queue] ->
        :ok

      # Create default queue without teardown when specified
      sandbox? ->
        EctoPGMQ.create_queue(TestRepo, @default_queue, queue_attrs)
        %{queue: @default_queue}

      # Create default queue with teardown
      true ->
        EctoPGMQ.create_queue(TestRepo, @default_queue, queue_attrs)
        on_exit(fn -> EctoPGMQ.drop_queue(TestRepo, @default_queue) end)
        %{queue: @default_queue}
    end
  end
end
