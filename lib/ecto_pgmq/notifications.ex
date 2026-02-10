defmodule EctoPGMQ.Notifications do
  @moduledoc """
  The entrypoint for managing PGMQ notification subscriptions.

  For general information about notification subscriptions, see
  `Postgrex.Notifications`.

  ## When to Use Notifications

  In general, notifications are most useful for queues with sporadic message
  bursts. If a queue is expected to be empty for long periods of time,
  subscribing to notifications can reduce polling overhead for consumers. If a
  queue has a fairly steady flow of messages, then notifications may not be as
  useful.

  ## Enabling Notifications

  In order to receive insert notifications for a queue, they must be explicitly
  enabled. This can be done during queue creation with
  `EctoPGMQ.create_queue/4`. Alternatively, notifications can be enabled for an
  existing queue with `EctoPGMQ.update_queue/4` or
  `EctoPGMQ.PGMQ.enable_notify_insert/4`.

  ## Throttling

  PGMQ supports per-queue notification throttling with millisecond granularity
  (see `t:EctoPGMQ.PGMQ.throttle_interval/0`) in order to avoid flooding
  notification subscribers during periods of high insert volume.

  For more information about configuring notification throttling, see
  `EctoPGMQ.update_queue/4`.

  For more information about per-queue notification throttling metrics, see
  `EctoPGMQ.Throttle`.

  ## Disabling Notifications

  Notifications can be disabled for an existing queue with
  `EctoPGMQ.update_queue/4` or `EctoPGMQ.PGMQ.disable_notify_insert/3`
  """

  alias EctoPGMQ.PGMQ
  alias EctoPGMQ.Queue

  ################################
  # Types
  ################################

  @typedoc "A notification channel name."
  @type channel :: String.t()

  @typedoc "A listener process."
  @type listener :: GenServer.server()

  @typedoc "A subscription reference."
  @type subscription :: reference()

  ################################
  # Public API
  ################################

  @doc """
  Starts a PGMQ notification listener linked to the current process.

  > #### Warning {: .warning}
  >
  > Each notification listener uses its own Postgres connection outside of any
  > `Ecto.Repo` connection pools. Therefore, in most cases, it's preferable to
  > start a single listener that subscribes to multiple channels instead of
  > starting a single listener per channel.

  ## Options

  For information about supported options, see
  `Postgrex.Notifications.start_link/1`.

  ## Examples

  ```elixir
  start_link([name: MyApp.Notifications | Repo.config()])
  ```
  """
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, Postgrex.Error.t() | term()}
  defdelegate start_link(opts), to: Postgrex.Notifications

  @doc """
  Subscribes the current process to notifications for the given queue.

  Notifications will manifest as messages with the following shape where
  `listener_pid` is the `t:pid/0` of the `t:listener/0`:

  ```elixir
  {:notification, listener_pid, subscription, channel, ""}
  ```

  ## Options

  For information about supported options, see
  `Postgrex.Notifications.listen/3`.

  ## Examples

  ```elixir
  subscribe(MyApp.Notifications, "my_queue")
  ```
  """
  @spec subscribe(listener(), Queue.name()) :: {:ok | :eventually, subscription(), channel()}
  @spec subscribe(listener(), Queue.name(), keyword()) :: {:ok | :eventually, subscription(), channel()}
  def subscribe(listener, queue, opts \\ []) do
    channel = queue_channel(queue)
    {status, subscription} = Postgrex.Notifications.listen(listener, channel, opts)
    {status, subscription, channel}
  end

  @doc """
  Unsubscribes the current process from notifications associated with the given
  listener reference.

  ## Options

  For information about supported options, see
  `Postgrex.Notifications.unlisten/3`.

  ## Examples

  ```elixir
  unsubscribe(MyApp.Notifications, my_subscription)
  ```
  """
  @spec unsubscribe(listener(), subscription()) :: :ok | :error
  @spec unsubscribe(listener(), subscription(), keyword()) :: :ok | :error
  defdelegate unsubscribe(listener, subscription, opts \\ []), to: Postgrex.Notifications, as: :unlisten

  ################################
  # Private API
  ################################

  defp queue_channel(queue) do
    "#{PGMQ.schema()}.#{PGMQ.queue_table_name(queue)}.INSERT"
  end
end
