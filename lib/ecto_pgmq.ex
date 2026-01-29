defmodule EctoPGMQ do
  @moduledoc """
  An opinionated PGMQ client for Elixir that builds on top of `Ecto` and the
  `Ecto.Adapters.Postgres` adapter.

  ## PGMQ Installation

  Because PGMQ is entirely made up of SQL objects, there are two available
  installation methods:

    * Extension Installation - This method installs PGMQ as a traditional
      Postgres extension. This is the preferred installation method but it
      requires access to the Postgres server file system and, therefore, may not
      always be feasible.

    * SQL Installation - This method installs PGMQ by manually creating all of
      the necessary SQL objects and works entirely within the database.

  `EctoPGMQ.Migrations` contains helper functions for managing both installation
  methods.

  For more information about managing the PGMQ extension, see the PGMQ
  [Installation](https://github.com/pgmq/pgmq/blob/main/INSTALLATION.md) and
  [Updating](https://github.com/pgmq/pgmq/blob/main/pgmq-extension/UPDATING.md)
  guides.

  ## Partitioning

  PGMQ supports partitioning both queues and archives.

  The [pg_partman extension](https://github.com/pgpartman/pg_partman) must be
  available in order to use partitioning.

  For more information about partitioning, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/tree/main?tab=readme-ov-file#partitioned-queues).

  ## Polling

  PGMQ supports Postgres server-side polling during read operations. Reading
  with a poll can be used to reduce network round trips if there is a good
  chance that demand can be satisfied in a short time **BUT** doing so utilizes
  a connection for the duration of the read operation. As such, polling should
  be avoided in situations where the DB connection pool is a bottleneck.

  ## FIFO Message Groups

  While PGMQ queues are FIFO data structures, the order of message processing
  can be non-deterministic when there are multiple consumers. This is usually
  fine but there is sometimes a need to consume messages strictly in order
  within a group. In order to support this, PGMQ exposes a number of functions
  that read messages while guaranteeing FIFO ordering for messages with the same
  `x-pgmq-group` header.

  There are two slightly different methodologies for reading messages while
  respecting FIFO message groups: round-robin reading and throughput-optimized
  reading.

  ### Round-Robin Reading

  This method will fairly interleave messages from all available groups.

      iex> specs = [{%{}, "A"}, {%{}, "A"}, {%{}, "B"}, {%{}, "B"}, {%{}, "C"}]
      iex> [id_1, id_2, id_3, id_4, id_5] = send_messages(Repo, "my_queue", specs)
      iex> messages = read_messages(Repo, "my_queue", 300, 5, message_grouping: :round_robin)
      iex> Enum.map(messages, & &1.id) == [id_1, id_3, id_5, id_2, id_4]
      true

  ### Throughput-Optimized Reading

  This method will prioritize messages from the same group. As the name implies,
  this method will often be more efficient than round-robin reading.

      iex> specs = [{%{}, "A"}, {%{}, "A"}, {%{}, "B"}, {%{}, "B"}, {%{}, "C"}]
      iex> message_ids = send_messages(Repo, "my_queue", specs)
      iex> messages = read_messages(Repo, "my_queue", 300, 5, message_grouping: :throughput_optimized)
      iex> Enum.map(messages, & &1.id) == message_ids
      true

  > #### Warning {: .warning}
  >
  > If message groups are long-lived and high-volume, this method of reading can
  > effectively starve later groups. For more information, see
  > [Performance Considerations](#performance-considerations).

  ### Performance Considerations

  In general, FIFO message groups are more performant when the following
  conditions are met:

    * There are many low-volume groups

    * Messages are removed from the queue relatively quickly

    * The queue is optimized for FIFO message group reads (see
      `EctoPGMQ.create_queue/4` and `EctoPGMQ.update_queue/4`).

  ### Further Information

  For more information about FIFO message groups, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/fifo-queues.md).
  """

  alias Ecto.Repo
  alias EctoPGMQ.DurationType
  alias EctoPGMQ.Message
  alias EctoPGMQ.PGMQ
  alias EctoPGMQ.Queue
  alias EctoPGMQ.Throttle

  ################################
  # Types
  ################################

  @typedoc """
  A delay before a message becomes visible.

  This can take any of the following forms:

    * A `t:Duration.t/0` denoting the time to wait before a message becomes
      visible.

    * An `t:integer/0` denoting the time (in seconds) to wait before a message
      becomes visible.

    * A `t:DateTime.t/0` denoting when a message should become visible.

  For more information about this type, see `t:EctoPGMQ.PGMQ.delay/0`.
  """
  @type delay :: Duration.t() | PGMQ.delay()

  @typedoc """
  Message update attributes.

  The following attributes are supported:

    * `:visibility_timeout` - A required `t:visibility_timeout/0` for the
      messages.
  """
  @type message_update_attributes :: %{visibility_timeout: visibility_timeout()}

  @typedoc """
  The minimum time between notifications.

  This can take either of the following forms:

    * A `t:Duration.t/0` denoting the minimum time between notifications.

    * A `t:non_neg_integer/0` denoting the minimum time (in milliseconds)
      between notifications.

  For more information about notifications, see `EctoPGMQ.Notifications`.

  For more information about this type, see
  `t:EctoPGMQ.PGMQ.throttle_interval/0`.
  """
  @type notification_throttle :: Duration.t() | PGMQ.throttle_interval()

  @typedoc """
  A queue partition configuration.

  A partition configuration is a tuple containing two elements: the partition
  interval and the retention interval.

  Both elements can take either of the following forms:

    * A `t:Duration.t/0` denoting a time-based interval.

    * A `t:pos_integer/0` denoting a message-based interval.

  For more information about partitioning, see [Partitioning](#partitioning).

  For more information about this type, see
  `t:EctoPGMQ.PGMQ.partition_interval/0` and
  `t:EctoPGMQ.PGMQ.retention_interval/0`.
  """
  @type partition_config :: {PGMQ.partition_interval(), PGMQ.retention_interval()}

  @typedoc """
  A message polling configuration.

  A polling configuration is a tuple containing two elements: the poll interval
  and the poll timeout.

  Both elements can take either of the following forms:

    * A `t:Duration.t/0` denoting a length of time.

    * A `t:pos_integer/0` denoting a length of time. The unit for the poll
      interval is milliseconds and the unit for the timeout is seconds.

  For more information about polling, see [Polling](#polling).

  For more information about this type, see `t:EctoPGMQ.PGMQ.poll_interval/0`
  and `t:EctoPGMQ.PGMQ.poll_timeout/0`.
  """
  @type poll_config :: {Duration.t() | PGMQ.poll_interval(), Duration.t() | PGMQ.poll_timeout()}

  @typedoc """
  Queue creation attributes.

  The following attributes are supported:

    * `:message_groups?` - An optional `t:boolean/0` denoting whether or not the
      queue should be optimized for FIFO message group reads. Defaults to
      `false`. For more information about FIFO message groups, see
      [FIFO Message Groups](#fifo-message-groups).

    * `:notifications` - An optional `t:notification_throttle/0` for the queue
      or `nil` to leave notifications disabled. Defaults to `nil`. For more
      information about notifications, see `EctoPGMQ.Notifications`.

    * `:partitions` - An optional `t:partition_config/0` for the queue or
      `nil` to disable partitioning. This option is ignored for unlogged queues.
      Defaults to `nil`. For more information about partitioning, see
      [Partitioning](#partitioning).

    * `:unlogged?` - An optional `t:boolean/0` denoting whether or not the queue
      should be unlogged. Defaults to `false`.
  """
  @type queue_create_attributes :: %{
          optional(:message_groups?) => boolean(),
          optional(:notifications) => notification_throttle() | nil,
          optional(:partitions) => partition_config() | nil,
          optional(:unlogged?) => boolean()
        }

  @typedoc """
  Queue update attributes.

  The following attributes are supported:

    * `:message_groups?` - `true` to optimize the queue for FIFO message group
      reads. Note that `true` is the only valid value because this operation
      cannot be undone. For more information about FIFO message groups, see
      [FIFO Message Groups](#fifo-message-groups).

    * `:notifications` - An optional `t:notification_throttle/0` for the queue or
      `nil` to disable notifications. For more information about notifications,
      see `EctoPGMQ.Notifications`.
  """
  @type queue_update_attributes :: %{
          optional(:message_groups?) => true,
          optional(:notifications) => notification_throttle() | nil
        }

  @typedoc """
  Options for reading messages.

  In addition to the standard [query options](`m:EctoPGMQ.PGMQ#query-options`),
  messages can be read with the following options:

    * `:delete?` - An optional `t:boolean/0` denoting whether or not to delete
      messages immediately after reading them. Defaults to `false`. For more
      information, see `EctoPGMQ.PGMQ.pop/4`.

    * `:message_grouping` - An optional value specifying how to handle message
      groups when reading messages. Possible values are `:round_robin`,
      `:throughput_optimized`, or `nil` to ignore message groups when reading.
      This option is ignored when deleting on read. Defaults to `nil`. For more
      information about FIFO message groups, see
      [FIFO Message Groups](#fifo-message-groups).

    * `:polling` - An optional `t:poll_config/0` for the read operation or nil
      to disable polling. This option is ignored when deleting on read. Defaults
      to `nil`. For more information about polling, see [Polling](#polling).
  """
  @type read_messages_opts :: [
          {:delete?, boolean()}
          | {:polling, poll_config() | nil}
          | {:message_grouping, :round_robin | :throughput_optimized | nil}
          | PGMQ.query_opt()
        ]

  @typedoc """
  The time from now that a message is invisible.

  This can take either of the following forms:

    * A `t:Duration.t/0` denoting how long a message is invisible.

    * An `t:integer/0` denoting how long (in seconds) a message is invisible.

  For more information about this type, see
  `t:EctoPGMQ.PGMQ.visibility_timeout/0`.
  """
  @type visibility_timeout :: Duration.t() | PGMQ.visibility_timeout()

  ################################
  # Queue API
  ################################

  @doc """
  Lists all queues.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> queues = all_queues(Repo)
      iex> Enum.all?(queues, &is_struct(&1, EctoPGMQ.Queue))
      true
  """
  @doc group: "Queue API"
  @spec all_queues(Repo.t()) :: [Queue.t()]
  @spec all_queues(Repo.t(), [PGMQ.query_opt()]) :: [Queue.t()]
  def all_queues(repo, opts \\ []), do: repo.all(Queue.query(), opts)

  @doc """
  Creates a queue with the given name.

  To create a queue in an `Ecto.Migration`, see
  `EctoPGMQ.Migrations.create_queue/2`.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> queue = create_queue(Repo, "my_unpartitioned_queue", %{notifications: 1_000})
      iex> match?(%EctoPGMQ.Queue{notifications: %EctoPGMQ.Throttle{}}, queue)
      true

      iex> queue = create_queue(Repo, "my_partitioned_queue", %{partitions: {10_000, 100_000}})
      iex> match?(%EctoPGMQ.Queue{is_partitioned: true}, queue)
      true

      iex> partitions = {Duration.new!(hour: 1), Duration.new!(day: 1)}
      iex> queue = create_queue(Repo, "my_partitioned_queue", %{partitions: partitions})
      iex> match?(%EctoPGMQ.Queue{is_partitioned: true}, queue)
      true

      iex> queue = create_queue(Repo, "my_unlogged_queue", %{unlogged?: true})
      iex> match?(%EctoPGMQ.Queue{is_unlogged: true}, queue)
      true
  """
  @doc group: "Queue API"
  @spec create_queue(Repo.t(), Queue.name()) :: Queue.t()
  @spec create_queue(Repo.t(), Queue.name(), queue_create_attributes()) :: Queue.t()
  @spec create_queue(Repo.t(), Queue.name(), queue_create_attributes(), [PGMQ.query_opt()]) :: Queue.t()
  def create_queue(repo, queue, attributes \\ %{}, opts \\ []) do
    notification_throttle = attributes[:notifications]
    partition_config = attributes[:partitions]

    transaction(
      repo,
      fn ->
        cond do
          # Create unlogged queue when specified
          Map.get(attributes, :unlogged?, false) ->
            PGMQ.create_unlogged(repo, queue, opts)

          # Create non-partitioned queue when specified
          is_nil(partition_config) ->
            PGMQ.create_non_partitioned(repo, queue, opts)

          # Create partitioned queue
          true ->
            {partition, retention} = partition_config
            PGMQ.create_partitioned(repo, queue, partition, retention, opts)
        end

        # Enable notifications when specified
        if notification_throttle do
          enable_notifications(repo, queue, notification_throttle, opts)
        end

        # Create FIFO index when specified
        if Map.get(attributes, :message_groups?, false) do
          PGMQ.create_fifo_index(repo, queue, opts)
        end

        # Fetch created queue record
        repo.get!(Queue.query(), queue, opts)
      end,
      opts
    )
  end

  @doc """
  Drops the given queue.

  To drop a queue in an `Ecto.Migration`, see
  `EctoPGMQ.Migrations.drop_queue/2`.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> drop_queue(Repo, "my_queue")
      :ok
  """
  @doc group: "Queue API"
  @spec drop_queue(Repo.t(), Queue.name()) :: :ok
  @spec drop_queue(Repo.t(), Queue.name(), [PGMQ.query_opt()]) :: :ok
  defdelegate drop_queue(repo, queue, opts \\ []), to: PGMQ

  @doc """
  Gets the given queue.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> queue = get_queue(Repo, "my_queue")
      iex> match?(%EctoPGMQ.Queue{}, queue)
      true

      iex> get_queue(Repo, "my_non_existent_queue")
      nil
  """
  @doc group: "Queue API"
  @spec get_queue(Repo.t(), Queue.name()) :: Queue.t() | nil
  @spec get_queue(Repo.t(), Queue.name(), [PGMQ.query_opt()]) :: Queue.t() | nil
  def get_queue(repo, queue, opts \\ []), do: repo.get(Queue.query(), queue, opts)

  @doc """
  Purges the given queue.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> send_messages(Repo, "my_queue", [%{"foo" => 1}, %{"bar" => 2}])
      iex> purge_queue(Repo, "my_queue")
      2
  """
  @doc group: "Queue API"
  @spec purge_queue(Repo.t(), Queue.name()) :: PGMQ.purged_messages()
  @spec purge_queue(Repo.t(), Queue.name(), [PGMQ.query_opt()]) :: PGMQ.purged_messages()
  defdelegate purge_queue(repo, queue, opts \\ []), to: PGMQ

  @doc """
  Updates the given queue.

  > #### Warning {: .warning}
  >
  > Because the underlying tables are owned by PGMQ, this function avoids row
  > locks so as not to disturb any internal PGMQ processes. As a result, if
  > multiple processes attempt to update a queue simultaneously, they may get
  > unexpected results.

  To update a queue in an `Ecto.Migration`, see
  `EctoPGMQ.Migrations.update_queue/2`.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> throttle = Duration.new!(second: 5)
      iex> queue = update_queue(Repo, "my_queue", %{notifications: throttle})
      iex> match?(%EctoPGMQ.Queue{notifications: %EctoPGMQ.Throttle{}}, queue)
      true
  """
  @doc group: "Queue API"
  @spec update_queue(Repo.t(), Queue.name(), queue_update_attributes()) :: Queue.t()
  @spec update_queue(Repo.t(), Queue.name(), queue_create_attributes(), [PGMQ.query_opt()]) :: Queue.t()
  def update_queue(repo, queue, attributes, opts \\ []) do
    transaction(
      repo,
      fn ->
        # Fetch existing throttle record
        current_throttle = repo.get(Throttle, queue, opts)

        # Update notifications when specified
        case {current_throttle, Map.fetch(attributes, :notifications)} do
          # Enable notifications when not already enabled
          {nil, {:ok, nt}} when not is_nil(nt) ->
            enable_notifications(repo, queue, nt, opts)

          # Disable notifications when specified
          {%Throttle{}, {:ok, nil}} ->
            PGMQ.disable_notify_insert(repo, queue, opts)

          # Update notification throttle when specified
          {%Throttle{} = throttle, {:ok, nt}} ->
            throttle
            |> Ecto.Changeset.cast(%{throttle: nt}, [:throttle])
            |> repo.update!(opts)

          # Do nothing when no change is specified
          _ ->
            :ok
        end

        # Create FIFO index when specified
        if Map.get(attributes, :message_groups?, false) do
          PGMQ.create_fifo_index(repo, queue, opts)
        end

        # Fetch updated queue record
        repo.get!(Queue.query(), queue, opts)
      end,
      opts
    )
  end

  ################################
  # Message API
  ################################

  @doc """
  Archives the given messages from the given queue.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> message_ids = send_messages(Repo, "my_queue", [%{"foo" => 1}, %{"bar" => 2}])
      iex> archive_messages(Repo, "my_queue", message_ids)
      :ok
  """
  @doc group: "Message API"
  @spec archive_messages(Repo.t(), Queue.name(), [Message.id()]) :: :ok
  @spec archive_messages(Repo.t(), Queue.name(), [Message.id()], [PGMQ.query_opt()]) :: :ok
  defdelegate archive_messages(repo, queue, message_ids, opts \\ []), to: PGMQ, as: :archive

  @doc """
  Deletes the given messages from the given queue.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> message_ids = send_messages(Repo, "my_queue", [%{"foo" => 1}, %{"bar" => 2}])
      iex> delete_messages(Repo, "my_queue", message_ids)
      :ok
  """
  @doc group: "Message API"
  @spec delete_messages(Repo.t(), Queue.name(), [Message.id()]) :: :ok
  @spec delete_messages(Repo.t(), Queue.name(), [Message.id()], [PGMQ.query_opt()]) :: :ok
  defdelegate delete_messages(repo, queue, message_ids, opts \\ []), to: PGMQ, as: :delete

  @doc """
  Reads messages from the given queue.

  ## Options

  See `t:read_messages_opts/0` for information about the options supported by
  this function.

  ## Examples

      iex> send_messages(Repo, "my_queue", [%{"foo" => 1}])
      iex> [message] = read_messages(Repo, "my_queue", 5, 2)
      iex> match?(%EctoPGMQ.Message{reads: 1}, message)
      true
  """
  @doc group: "Message API"
  @spec read_messages(Repo.t(), Queue.name(), visibility_timeout(), PGMQ.quantity()) :: [Message.t()]
  @spec read_messages(
          Repo.t(),
          Queue.name(),
          visibility_timeout(),
          PGMQ.quantity(),
          [
            {:delete?, boolean()}
            | {:polling, poll_config() | nil}
            | {:message_grouping, :round_robin | :throughput_optimized | nil}
            | PGMQ.query_opt()
          ]
        ) :: [Message.t()]
  def read_messages(repo, queue, visibility_timeout, quantity, opts \\ []) do
    {delete?, opts} = Keyword.pop(opts, :delete?, false)
    {poll_config, opts} = Keyword.pop(opts, :polling)
    {grouping, opts} = Keyword.pop(opts, :message_grouping)
    visibility_timeout = maybe_to_time(visibility_timeout, :second)

    case {delete?, poll_config, grouping} do
      # Immediately delete messages when specified
      {true, _, _} ->
        PGMQ.pop(repo, queue, quantity, opts)

      # Otherwise, delegate based on poll config and grouping
      {false, nil, nil} ->
        PGMQ.read(repo, queue, visibility_timeout, quantity, %{}, opts)

      {false, nil, :round_robin} ->
        PGMQ.read_grouped_rr(repo, queue, visibility_timeout, quantity, opts)

      {false, nil, :throughput_optimized} ->
        PGMQ.read_grouped(repo, queue, visibility_timeout, quantity, opts)

      {false, poll_config, nil} ->
        {interval, timeout} = parse_poll_config(poll_config)
        PGMQ.read_with_poll(repo, queue, visibility_timeout, quantity, timeout, interval, %{}, opts)

      {false, poll_config, :round_robin} ->
        {interval, timeout} = parse_poll_config(poll_config)
        PGMQ.read_grouped_rr_with_poll(repo, queue, visibility_timeout, quantity, timeout, interval, opts)

      {false, poll_config, :throughput_optimized} ->
        {interval, timeout} = parse_poll_config(poll_config)
        PGMQ.read_grouped_with_poll(repo, queue, visibility_timeout, quantity, timeout, interval, opts)
    end
  end

  @doc """
  Sends messages to the given queue.

  ## Options

  In addition to the standard [query options](`m:EctoPGMQ.PGMQ#query-options`),
  this function also supports the following options:

    * `:delay` - An optional `t:delay/0` for the messages. Defaults to `0`.

  ## Examples

      iex> delay = Duration.new!(hour: 1)
      iex> message_ids = send_messages(Repo, "my_queue", [%{"foo" => 1}, %{"bar" => 2}], delay: delay)
      iex> Enum.all?(message_ids, &is_integer/1)
      true
  """
  @doc group: "Message API"
  @spec send_messages(Repo.t(), Queue.name(), [Message.specification()]) :: [Message.id()]
  @spec send_messages(
          Repo.t(),
          Queue.name(),
          [Message.specification()],
          [{:delay, delay()} | PGMQ.query_opt()]
        ) :: [Message.id()]
  def send_messages(repo, queue, messages, opts \\ []) do
    {delay, opts} = Keyword.pop(opts, :delay, 0)
    delay = maybe_to_time(delay, :second)

    {payloads, headers} =
      messages
      |> Enum.map(fn spec ->
        {payload, _, headers} = Message.normalize_specification(spec)
        {payload, headers}
      end)
      |> Enum.unzip()

    PGMQ.send_batch(repo, queue, payloads, headers, delay, opts)
  end

  @doc """
  Updates the given messages in the given queue.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> visibility_timeout = Duration.new!(minute: 5)
      iex> message_ids = send_messages(Repo, "my_queue", [%{"foo" => 1}, %{"bar" => 2}])
      iex> update_messages(Repo, "my_queue", message_ids, %{visibility_timeout: visibility_timeout})
      :ok
  """
  @doc group: "Message API"
  @spec update_messages(Repo.t(), Queue.name(), [Message.id()], message_update_attributes()) :: :ok
  @spec update_messages(Repo.t(), Queue.name(), [Message.id()], message_update_attributes(), [PGMQ.query_opt()]) :: :ok
  def update_messages(repo, queue, message_ids, %{visibility_timeout: visibility_timeout}, opts \\ []) do
    visibility_timeout = maybe_to_time(visibility_timeout, :second)
    PGMQ.set_vt(repo, queue, message_ids, visibility_timeout, opts)
    :ok
  end

  ################################
  # Private API
  ################################

  defp transaction(repo, fun, opts) do
    {:ok, result} =
      repo.transact(
        fn ->
          result = fun.()
          {:ok, result}
        end,
        opts
      )

    result
  end

  defp enable_notifications(repo, queue, throttle, opts) do
    throttle = maybe_to_time(throttle, :millisecond)
    PGMQ.enable_notify_insert(repo, queue, throttle, opts)
  end

  defp parse_poll_config({interval, timeout}) do
    interval = maybe_to_time(interval, :millisecond)
    timeout = maybe_to_time(timeout, :second)
    {interval, timeout}
  end

  defp maybe_to_time(%Duration{} = duration, unit) do
    DurationType.to_time(duration, unit)
  end

  defp maybe_to_time(time, _) when is_integer(time), do: time
end
