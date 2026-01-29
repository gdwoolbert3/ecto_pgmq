defmodule EctoPGMQ.PGMQ do
  @moduledoc """
  An SDK that fully covers the
  [PGMQ API-space](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md).

  Instead of implementing a corresponding function for every alias and
  parameterization supported by PGMQ, this module relies on client-side defaults
  to implement a single function for each distinct piece of PGMQ functionality.

  ## Query Options

  All of the functions in this module support a common set of query options.

  For a detailed description of these options, see `t:query_opt/0`.
  """

  import Ecto.Query

  alias Ecto.Repo
  alias EctoPGMQ.Message
  alias EctoPGMQ.Metrics
  alias EctoPGMQ.Queue
  alias Postgrex.Result

  @group_header "x-pgmq-group"
  @extension "pgmq"

  ################################
  # Types
  ################################

  @typedoc """
  Filter conditions to be applied when reading messages from a queue.

  Note that the filter conditions are applied to the message body, not the
  headers.

  > #### Warning {: .warning}
  >
  > As stated in the
  > [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#reading-messages),
  > conditional message reading is an experimental feature and the API might be
  > subject to change in future releases.
  """
  @type conditional :: %{optional(String.t()) => term()}

  @typedoc """
  A delay before a message becomes visible.

  This can take either of the following forms:

    * An `t:integer/0` denoting the time (in seconds) to wait before a message
      becomes visible.

    * A `t:DateTime.t/0` denoting when a message should become visible.
  """
  @type delay :: non_neg_integer() | DateTime.t()

  @typedoc "The number of partitions to create preemptively."
  @type leading_partitions :: non_neg_integer()

  @typedoc """
  The interval at which new partitions should be created.

  This can take either of the following forms:

    * A `t:pos_integer/0` denoting how many messages per partition.

    * A `t:Duration.t/0` denoting a time range per partition.
  """
  @type partition_interval :: pos_integer() | Duration.t()

  @typedoc "The time (in milliseconds) to wait between polls."
  @type poll_interval :: pos_integer()

  @typedoc "The maximum time (in seconds) to poll for messages."
  @type poll_timeout :: pos_integer()

  @typedoc "The number of purged messages."
  @type purged_messages :: non_neg_integer()

  @typedoc "The maximum number of messages to read."
  @type quantity :: pos_integer()

  @typedoc """
  A query configuration option.

  The following query configuration options are supported:

    * `:log` - A `t:boolean/0` denoting whether or to log the query. Defaults to
    `true`.

    * `:timeout` - A `t:timeout/0` for the query (in milliseconds). Defaults to
    `15_000`.
  """
  @type query_opt :: {:log, boolean()} | {:timeout, timeout()}

  @typedoc """
  The interval at which old partitions should be dropped.

  This can take either of the following forms:

    * A `t:pos_integer/0` denoting how many messages to retain in total.

    * A `t:Duration.t/0` denoting a total time range to retain.
  """
  @type retention_interval :: pos_integer() | Duration.t()

  @typedoc """
  The minimum time (in milliseconds) between notifications.

  A throttle interval of `0` effectively disables notification throttling.

  For more information about notification throttling, see
  [Throttling](m:EctoPGMQ.Notifications#throttling).
  """
  @type throttle_interval :: non_neg_integer()

  @typedoc "The time from now (in seconds) that a message is invisible."
  @type visibility_timeout :: integer()

  ################################
  # Private Macros
  ################################

  defmacrop read_message_query(cte) do
    # Prefix must be assigned in the initial query to override schema prefix and
    # CTE name must be known at compile time, hence the usage of a macro
    quote do
      decorate_message_query(from(m in {unquote(cte), Message}, prefix: nil))
    end
  end

  ################################
  # PGMQ API
  ################################

  @doc """
  Archives the given messages in the given queue.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#archive-batch).

  ## Examples

      iex> message_ids = send_batch(Repo, "my_queue", [%{"foo" => 1}, %{"bar" => 2}])
      iex> archive(Repo, "my_queue", message_ids)
      :ok
  """
  @spec archive(Repo.t(), Queue.name(), [Message.id()]) :: :ok
  @spec archive(Repo.t(), Queue.name(), [Message.id()], [query_opt()]) :: :ok
  def archive(repo, queue, message_ids, opts \\ []) do
    sql = "SELECT * FROM pgmq.archive($1::text, $2::bigint[])"
    params = [queue, message_ids]
    repo.query!(sql, params, opts)
    :ok
  end

  @doc """
  Converts the archive for the given queue into a partitioned table.

  > #### Warning {: .warning}
  >
  > This function postfixes the old archive table name with `_old` and leaves
  > its contents untouched. Additional cleanup (table deletion, message
  > movement, etc.) is left to the user.

  For more information about partitioning, see
  [Partitioning](`m:EctoPGMQ#partitioning`).

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#convert_archive_partitioned).

  ## Examples

      iex> convert_archive_partitioned(Repo, "my_queue", 10_000, 100_000, 10)
      :ok

      iex> partition = Duration.new!(hour: 1)
      iex> retention = Duration.new!(day: 1)
      iex> convert_archive_partitioned(Repo, "my_queue", partition, retention, 10)
      :ok
  """
  @spec convert_archive_partitioned(Repo.t(), Queue.name()) :: :ok
  @spec convert_archive_partitioned(Repo.t(), Queue.name(), partition_interval()) :: :ok
  @spec convert_archive_partitioned(Repo.t(), Queue.name(), partition_interval(), retention_interval()) :: :ok
  @spec convert_archive_partitioned(
          Repo.t(),
          Queue.name(),
          partition_interval(),
          retention_interval(),
          leading_partitions()
        ) :: :ok
  @spec convert_archive_partitioned(
          Repo.t(),
          Queue.name(),
          partition_interval(),
          retention_interval(),
          leading_partitions(),
          [query_opt()]
        ) :: :ok
  def convert_archive_partitioned(
        repo,
        queue,
        partition_interval \\ 10_000,
        retention_interval \\ 100_000,
        leading_partitions \\ 10,
        opts \\ []
      ) do
    p_type = pg_type(partition_interval)
    r_type = pg_type(retention_interval)
    sql = "SELECT pgmq.convert_archive_partitioned($1::text, $2::#{p_type}::text, $3::#{r_type}::text, $4::integer)"
    params = [queue, partition_interval, retention_interval, leading_partitions]
    repo.query!(sql, params, opts)
    :ok
  end

  @doc """
  Creates an index to optimize FIFO message group read performance for the given
  queue.

  For more information about FIFO message groups, see
  [FIFO Message Groups](`m:EctoPGMQ#fifo-message-groups`).

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#create_fifo_index).

  ## Examples

      iex> create_fifo_index(Repo, "my_queue")
      :ok
  """
  @spec create_fifo_index(Repo.t(), Queue.name()) :: :ok
  @spec create_fifo_index(Repo.t(), Queue.name(), [query_opt()]) :: :ok
  def create_fifo_index(repo, queue, opts \\ []) do
    sql = "SELECT pgmq.create_fifo_index($1::text)"
    params = [queue]
    repo.query!(sql, params, opts)
    :ok
  end

  @doc """
  Creates an unpartitioned queue with the given name.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#create_non_partitioned).

  ## Examples

      iex> create_non_partitioned(Repo, "my_unpartitioned_queue")
      :ok
  """
  @spec create_non_partitioned(Repo.t(), Queue.name()) :: :ok
  @spec create_non_partitioned(Repo.t(), Queue.name(), [query_opt()]) :: :ok
  def create_non_partitioned(repo, queue, opts \\ []) do
    sql = "SELECT pgmq.create_non_partitioned($1::text)"
    params = [queue]
    repo.query!(sql, params, opts)
    :ok
  end

  @doc """
  Creates a partitioned queue with the given name.

  For more information about partitioning, see
  [Partitioning](`m:EctoPGMQ#partitioning`).

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#create_partitioned).

  ## Examples

      iex> create_partitioned(Repo, "my_partitioned_queue", 10_000, 100_000)
      :ok

      iex> partition = Duration.new!(hour: 1)
      iex> retention = Duration.new!(day: 1)
      iex> create_partitioned(Repo, "my_partitioned_queue", partition, retention)
      :ok
  """
  @spec create_partitioned(Repo.t(), Queue.name()) :: :ok
  @spec create_partitioned(Repo.t(), Queue.name(), partition_interval()) :: :ok
  @spec create_partitioned(Repo.t(), Queue.name(), partition_interval(), retention_interval()) :: :ok
  @spec create_partitioned(Repo.t(), Queue.name(), partition_interval(), retention_interval(), [query_opt()]) :: :ok
  def create_partitioned(repo, queue, partition_interval \\ 10_000, retention_interval \\ 100_000, opts \\ []) do
    p_type = pg_type(partition_interval)
    r_type = pg_type(retention_interval)
    sql = "SELECT pgmq.create_partitioned($1::text, $2::#{p_type}::text, $3::#{r_type}::text)"
    params = [queue, partition_interval, retention_interval]
    repo.query!(sql, params, opts)
    :ok
  end

  @doc """
  Creates an unlogged queue with the given name.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#create_unlogged).

  > #### Warning {: .warning}
  >
  > Unlogged tables benefit from faster write operations but they risk data loss
  > if the Postgres server restarts. Use with caution.

  ## Examples

      iex> create_unlogged(Repo, "my_unlogged_queue")
      :ok
  """
  @spec create_unlogged(Repo.t(), Queue.name()) :: :ok
  @spec create_unlogged(Repo.t(), Queue.name(), [query_opt()]) :: :ok
  def create_unlogged(repo, queue, opts \\ []) do
    sql = "SELECT pgmq.create_unlogged($1::text)"
    params = [queue]
    repo.query!(sql, params, opts)
    :ok
  end

  @doc """
  Deletes the given messages from the given queue.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#delete-batch).

  ## Examples

      iex> message_ids = send_batch(Repo, "my_queue", [%{"foo" => 1}, %{"bar" => 2}])
      iex> delete(Repo, "my_queue", message_ids)
      :ok
  """
  @spec delete(Repo.t(), Queue.name(), [Message.id()]) :: :ok
  @spec delete(Repo.t(), Queue.name(), [Message.id()], [query_opt()]) :: :ok
  def delete(repo, queue, message_ids, opts \\ []) do
    sql = "SELECT * FROM pgmq.delete($1::text, $2::bigint[])"
    params = [queue, message_ids]
    repo.query!(sql, params, opts)
    :ok
  end

  @doc """
  Disables insert notifications for the given queue.

  For more information about notifications, see `EctoPGMQ.Notifications`.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#disable_notify_insert).

  ## Examples

      iex> disable_notify_insert(Repo, "my_queue")
      :ok
  """
  @spec disable_notify_insert(Repo.t(), Queue.name()) :: :ok
  @spec disable_notify_insert(Repo.t(), Queue.name(), [query_opt()]) :: :ok
  def disable_notify_insert(repo, queue, opts \\ []) do
    sql = "SELECT pgmq.disable_notify_insert($1::text)"
    params = [queue]
    repo.query!(sql, params, opts)
    :ok
  end

  @doc """
  Drops the given queue.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#drop_queue).

  ## Examples

      iex> drop_queue(Repo, "my_queue")
      :ok
  """
  @spec drop_queue(Repo.t(), Queue.name()) :: :ok
  @spec drop_queue(Repo.t(), Queue.name(), [query_opt()]) :: :ok
  def drop_queue(repo, queue, opts \\ []) do
    sql = "SELECT pgmq.drop_queue($1::text)"
    params = [queue]
    repo.query!(sql, params, opts)
    :ok
  end

  @doc """
  Enables insert notifications for the given queue.

  For more information about notifications, see `EctoPGMQ.Notifications`.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#enable_notify_insert).

  ## Examples

      iex> enable_notify_insert(Repo, "my_queue", 1_000)
      :ok
  """
  @spec enable_notify_insert(Repo.t(), Queue.name()) :: :ok
  @spec enable_notify_insert(Repo.t(), Queue.name(), throttle_interval()) :: :ok
  @spec enable_notify_insert(Repo.t(), Queue.name(), throttle_interval(), [query_opt()]) :: :ok
  def enable_notify_insert(repo, queue, throttle_interval \\ 250, opts \\ []) do
    sql = "SELECT pgmq.enable_notify_insert($1::text, $2::integer)"
    params = [queue, throttle_interval]
    repo.query!(sql, params, opts)
    :ok
  end

  @doc """
  Lists all queues.

  Because this function naively wraps the corresponding PGMQ function, the
  `:metrics` and `:notifications` fields in the returned `EctoPGMQ.Queue` structs
  will not be populated.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#list_queues).

  ## Examples

      iex> queues = list_queues(Repo)
      iex> Enum.all?(queues, &is_struct(&1, EctoPGMQ.Queue))
      true
  """
  @spec list_queues(Repo.t()) :: [Queue.t()]
  @spec list_queues(Repo.t(), [query_opt()]) :: [Queue.t()]
  def list_queues(repo, opts \\ []) do
    # Mask `Ecto.Association.NotLoaded` struct
    list_queues_query()
    |> select_merge([q], %{notifications: nil})
    |> repo.all(opts)
  end

  @doc """
  Returns metrics for all queues.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#metrics_all).

  ## Examples

      iex> metrics = metrics_all(Repo)
      iex> Enum.all?(metrics, &is_struct(&1, EctoPGMQ.Metrics))
      true
  """
  @spec metrics_all(Repo.t()) :: [Metrics.t()]
  @spec metrics_all(Repo.t(), [query_opt()]) :: [Metrics.t()]
  def metrics_all(repo, opts \\ []), do: repo.all(metrics_all_query(), opts)

  @doc """
  Simultaneously fetches and deletes messages from the given queue.

  This function does **NOT** increment the read count of the fetched messages.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#pop).

  ## Examples

      iex> send_batch(Repo, "my_queue", [%{"foo" => 1}])
      iex> [message] = pop(Repo, "my_queue", 1)
      iex> match?(%EctoPGMQ.Message{reads: 0}, message)
      true
  """
  @spec pop(Repo.t(), Queue.name(), quantity()) :: [Message.t()]
  @spec pop(Repo.t(), Queue.name(), quantity(), [query_opt()]) :: [Message.t()]
  def pop(repo, queue, quantity, opts \\ []) do
    # Prefix must be assigned in the initial query to override schema prefix and
    # CTE name must be known at compile time
    from(m in {"pop", Message}, prefix: nil)
    |> decorate_message_query()
    |> with_cte("pop", as: fragment("SELECT * FROM pgmq.pop(?::text, ?::integer)", ^queue, ^quantity))
    |> repo.all(opts)
  end

  @doc """
  Purges all messages from the given queue and returns the number of messages
  that were deleted.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#purge_queue).

  ## Examples

      iex> send_batch(Repo, "my_queue", [%{"foo" => 1}, %{"bar" => 2}])
      iex> purge_queue(Repo, "my_queue")
      2
  """
  @spec purge_queue(Repo.t(), Queue.name()) :: purged_messages()
  @spec purge_queue(Repo.t(), Queue.name(), [query_opt()]) :: purged_messages()
  def purge_queue(repo, queue, opts \\ []) do
    sql = "SELECT * FROM pgmq.purge_queue($1::text)"
    params = [queue]
    %Result{rows: [[deleted]]} = repo.query!(sql, params, opts)
    deleted
  end

  @doc """
  Reads messages from the given queue.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#read).

  ## Examples

      iex> send_batch(Repo, "my_queue", [%{"foo" => 1}])
      iex> [message] = read(Repo, "my_queue", 5, 1)
      iex> match?(%EctoPGMQ.Message{reads: 1}, message)
      true
  """
  @spec read(Repo.t(), Queue.name(), visibility_timeout(), quantity()) :: [Message.t()]
  @spec read(Repo.t(), Queue.name(), visibility_timeout(), quantity(), conditional()) :: [Message.t()]
  @spec read(Repo.t(), Queue.name(), visibility_timeout(), quantity(), conditional(), [query_opt()]) :: [Message.t()]
  def read(repo, queue, visibility_timeout, quantity, conditional \\ %{}, opts \\ []) do
    "read"
    |> read_message_query()
    |> with_cte("read",
      as:
        fragment(
          "SELECT * FROM pgmq.read(?::text, ?::integer, ?::integer, ?::jsonb)",
          ^queue,
          ^visibility_timeout,
          ^quantity,
          ^conditional
        )
    )
    |> repo.all(opts)
  end

  @doc """
  Reads messages from the given queue while respecting FIFO message groups and
  optimizing throughput.

  For more information about FIFO message groups, see
  [FIFO Message Groups](`m:EctoPGMQ#fifo-message-groups`).

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#read_grouped).

  ## Examples

      iex> send_batch(Repo, "my_queue", [%{"foo" => 1}])
      iex> [message] = read_grouped(Repo, "my_queue", 5, 1)
      iex> match?(%EctoPGMQ.Message{reads: 1}, message)
      true
  """
  @spec read_grouped(Repo.t(), Queue.name(), visibility_timeout(), quantity()) :: [Message.t()]
  @spec read_grouped(Repo.t(), Queue.name(), visibility_timeout(), quantity(), [query_opt()]) :: [Message.t()]
  def read_grouped(repo, queue, visibility_timeout, quantity, opts \\ []) do
    "read_grouped"
    |> read_message_query()
    |> with_cte("read_grouped",
      as:
        fragment(
          "SELECT * FROM pgmq.read_grouped(?::text, ?::integer, ?::integer)",
          ^queue,
          ^visibility_timeout,
          ^quantity
        )
    )
    |> repo.all(opts)
  end

  @doc """
  Reads messages from the given queue while respecting and round-robin
  interleaving FIFO message groups.

  For more information about FIFO message groups, see
  [FIFO Message Groups](`m:EctoPGMQ#fifo-message-groups`).

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#read_grouped_rr).

  ## Examples

      iex> send_batch(Repo, "my_queue", [%{"foo" => 1}])
      iex> [message] = read_grouped_rr(Repo, "my_queue", 5, 1)
      iex> match?(%EctoPGMQ.Message{reads: 1}, message)
      true
  """
  @spec read_grouped_rr(Repo.t(), Queue.name(), visibility_timeout(), quantity()) :: [Message.t()]
  @spec read_grouped_rr(Repo.t(), Queue.name(), visibility_timeout(), quantity(), [query_opt()]) :: [Message.t()]
  def read_grouped_rr(repo, queue, visibility_timeout, quantity, opts \\ []) do
    "read_grouped_rr"
    |> read_message_query()
    |> with_cte("read_grouped_rr",
      as:
        fragment(
          "SELECT * FROM pgmq.read_grouped_rr(?::text, ?::integer, ?::integer)",
          ^queue,
          ^visibility_timeout,
          ^quantity
        )
    )
    |> repo.all(opts)
  end

  @doc """
  Reads messages from the given queue with a Postgres server-side poll while
  respecting and round-robin interleaving FIFO message groups.

  For more information about FIFO message groups, see
  [FIFO Message Groups](`m:EctoPGMQ#fifo-message-groups`).

  For more information about polling, see [Polling](`m:EctoPGMQ#polling`).

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#read_grouped_rr_with_poll).

  ## Examples

      iex> send_batch(Repo, "my_queue", [%{"foo" => 1}])
      iex> [message] = read_grouped_rr_with_poll(Repo, "my_queue", 5, 1, 5, 500)
      iex> match?(%EctoPGMQ.Message{reads: 1}, message)
      true
  """
  @spec read_grouped_rr_with_poll(Repo.t(), Queue.name(), visibility_timeout(), quantity()) :: [Message.t()]
  @spec read_grouped_rr_with_poll(
          Repo.t(),
          Queue.name(),
          visibility_timeout(),
          quantity(),
          poll_timeout()
        ) :: [Message.t()]
  @spec read_grouped_rr_with_poll(
          Repo.t(),
          Queue.name(),
          visibility_timeout(),
          quantity(),
          poll_timeout(),
          poll_interval()
        ) :: [Message.t()]
  @spec read_grouped_rr_with_poll(
          Repo.t(),
          Queue.name(),
          visibility_timeout(),
          quantity(),
          poll_timeout(),
          poll_interval(),
          [query_opt()]
        ) :: [Message.t()]
  def read_grouped_rr_with_poll(
        repo,
        queue,
        visibility_timeout,
        quantity,
        poll_timeout \\ 5,
        poll_interval \\ 100,
        opts \\ []
      ) do
    "read_grouped_rr_with_poll"
    |> read_message_query()
    |> with_cte("read_grouped_rr_with_poll",
      as:
        fragment(
          "SELECT * FROM pgmq.read_grouped_rr_with_poll(?::text, ?::integer, ?::integer, ?::integer, ?::integer)",
          ^queue,
          ^visibility_timeout,
          ^quantity,
          ^poll_timeout,
          ^poll_interval
        )
    )
    |> repo.all(opts)
  end

  @doc """
  Reads messages from the given queue with a Postgres server-side poll while
  respecting FIFO message groups and optimizing throughput.

  For more information about FIFO message groups, see
  [FIFO Message Groups](`m:EctoPGMQ#fifo-message-groups`).

  For more information about polling, see [Polling](`m:EctoPGMQ#polling`).

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#read_grouped_with_poll).

  ## Examples

      iex> send_batch(Repo, "my_queue", [%{"foo" => 1}])
      iex> [message] = read_grouped_with_poll(Repo, "my_queue", 5, 1, 5, 500)
      iex> match?(%EctoPGMQ.Message{reads: 1}, message)
      true
  """
  @spec read_grouped_with_poll(Repo.t(), Queue.name(), visibility_timeout(), quantity()) :: [Message.t()]
  @spec read_grouped_with_poll(Repo.t(), Queue.name(), visibility_timeout(), quantity(), poll_timeout()) :: [Message.t()]
  @spec read_grouped_with_poll(
          Repo.t(),
          Queue.name(),
          visibility_timeout(),
          quantity(),
          poll_timeout(),
          poll_interval()
        ) :: [Message.t()]
  @spec read_grouped_with_poll(
          Repo.t(),
          Queue.name(),
          visibility_timeout(),
          quantity(),
          poll_timeout(),
          poll_interval(),
          [query_opt()]
        ) :: [Message.t()]
  def read_grouped_with_poll(
        repo,
        queue,
        visibility_timeout,
        quantity,
        poll_timeout \\ 5,
        poll_interval \\ 100,
        opts \\ []
      ) do
    "read_grouped_with_poll"
    |> read_message_query()
    |> with_cte("read_grouped_with_poll",
      as:
        fragment(
          "SELECT * FROM pgmq.read_grouped_with_poll(?::text, ?::integer, ?::integer, ?::integer, ?::integer)",
          ^queue,
          ^visibility_timeout,
          ^quantity,
          ^poll_timeout,
          ^poll_interval
        )
    )
    |> repo.all(opts)
  end

  @doc """
  Reads messages from the given queue with a Postgres server-side poll.

  For more information about polling, see [Polling](`m:EctoPGMQ#polling`).

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#read_with_poll).

  ## Examples

      iex> send_batch(Repo, "my_queue", [%{"foo" => 1}])
      iex> [message] = read_with_poll(Repo, "my_queue", 5, 1, 5, 500)
      iex> match?(%EctoPGMQ.Message{reads: 1}, message)
      true
  """
  @spec read_with_poll(Repo.t(), Queue.name(), visibility_timeout(), quantity()) :: [Message.t()]
  @spec read_with_poll(Repo.t(), Queue.name(), visibility_timeout(), quantity(), poll_timeout()) :: [Message.t()]
  @spec read_with_poll(
          Repo.t(),
          Queue.name(),
          visibility_timeout(),
          quantity(),
          poll_timeout(),
          poll_interval()
        ) :: [Message.t()]
  @spec read_with_poll(
          Repo.t(),
          Queue.name(),
          visibility_timeout(),
          quantity(),
          poll_timeout(),
          poll_interval(),
          conditional()
        ) :: [Message.t()]
  @spec read_with_poll(
          Repo.t(),
          Queue.name(),
          visibility_timeout(),
          quantity(),
          poll_timeout(),
          poll_interval(),
          conditional(),
          [query_opt()]
        ) :: [Message.t()]
  def read_with_poll(
        repo,
        queue,
        visibility_timeout,
        quantity,
        poll_timeout \\ 5,
        poll_interval \\ 100,
        conditional \\ %{},
        opts \\ []
      ) do
    # Prefix must be assigned in the initial query to override schema prefix and
    # CTE name must be known at compile time
    from(m in {"read_with_poll", Message}, prefix: nil)
    |> decorate_message_query()
    |> with_cte("read_with_poll",
      as:
        fragment(
          "SELECT * FROM pgmq.read_with_poll(?::text, ?::integer, ?::integer, ?::integer, ?::integer, ?::jsonb)",
          ^queue,
          ^visibility_timeout,
          ^quantity,
          ^poll_timeout,
          ^poll_interval,
          ^conditional
        )
    )
    |> repo.all(opts)
  end

  @doc """
  Sends the given messages to the given queue.

  The `headers` arg defaults to `nil`, which is a shorthand for `NULL` headers
  for all messages. If a list is given for the `headers` arg, the length of the
  list must match the length of the given list of messages.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#send_batch).

  ## Examples

      iex> message_ids = send_batch(Repo, "my_queue", [%{"foo" => 1}, %{"bar" => 2}])
      iex> Enum.all?(message_ids, &is_integer/1)
      true

      iex> delay = DateTime.utc_now()
      iex> message_ids = send_batch(Repo, "my_queue", [%{"foo" => 1}, %{"bar" => 2}], nil, delay)
      iex> Enum.all?(message_ids, &is_integer/1)
      true
  """
  @spec send_batch(Repo.t(), Queue.name(), [Message.payload()]) :: [Message.id()]
  @spec send_batch(Repo.t(), Queue.name(), [Message.payload()], [Message.headers() | nil] | nil) :: [Message.id()]
  @spec send_batch(
          Repo.t(),
          Queue.name(),
          [Message.payload()],
          [Message.headers() | nil] | nil,
          delay()
        ) :: [Message.id()]
  @spec send_batch(
          Repo.t(),
          Queue.name(),
          [Message.payload()],
          [Message.headers() | nil] | nil,
          delay(),
          [query_opt()]
        ) :: [Message.id()]
  def send_batch(repo, queue, payloads, headers \\ nil, delay \\ 0, opts \\ []) do
    type = pg_type(delay)
    sql = "SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::jsonb[], $4::#{type})"

    # Expand short-form representation of no headers if applicable
    headers = if is_nil(headers), do: List.duplicate(nil, length(payloads)), else: headers
    params = [queue, payloads, headers, delay]
    %Result{rows: rows} = repo.query!(sql, params, opts)
    Enum.map(rows, fn [message_id] -> message_id end)
  end

  @doc """
  Sets the visibility timeout of the given messages in the given queue.

  For more information about this function, see the
  [PGMQ docs](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md#set_vt-batch).

  ## Examples

      iex> message_ids = send_batch(Repo, "my_queue", [%{"foo" => 1}])
      iex> [read_message] = read(Repo, "my_queue", 5, 1)
      iex> [updated_message] = set_vt(Repo, "my_queue", message_ids, 10)
      iex> DateTime.diff(updated_message.visible_at, read_message.visible_at) > 0
      true
  """
  @spec set_vt(Repo.t(), Queue.name(), [Message.id()], visibility_timeout()) :: [Message.t()]
  @spec set_vt(Repo.t(), Queue.name(), [Message.id()], visibility_timeout(), [query_opt()]) :: [Message.t()]
  def set_vt(repo, queue, message_ids, visibility_timeout, opts \\ []) do
    # Prefix must be assigned in the initial query to override schema prefix
    from(m in {"set_vt", Message}, prefix: nil)
    |> with_cte("set_vt",
      as:
        fragment(
          "SELECT * FROM pgmq.set_vt(?::text, ?::bigint[], ?::integer)",
          ^queue,
          ^message_ids,
          ^visibility_timeout
        )
    )
    |> repo.all(opts)
  end

  ################################
  # Utility API
  ################################

  @doc false
  @spec archive_table_name(Queue.name()) :: String.t()
  def archive_table_name(queue), do: "a_#{queue}"

  @doc false
  @spec decorate_message_query(Ecto.Queryable.t()) :: Ecto.Query.t()
  def decorate_message_query(query) do
    select_merge(query, [m], %{group: fragment("?->>?", m.headers, ^@group_header)})
  end

  @doc false
  @spec extension :: String.t()
  def extension, do: @extension

  @doc false
  @spec group_header :: String.t()
  def group_header, do: @group_header

  @doc false
  @spec list_queues_query :: Ecto.Query.t()
  def list_queues_query do
    with_cte(
      # Prefix must be assigned in the initial query to override schema prefix
      from(q in {"list_queues", Queue}, prefix: nil),
      "list_queues",
      as: fragment("SELECT * FROM pgmq.list_queues()")
    )
  end

  @doc false
  @spec metrics_all_query :: Ecto.Query.t()
  def metrics_all_query do
    with_cte({"metrics_all", Metrics}, "metrics_all", as: fragment("SELECT * FROM pgmq.metrics_all()"))
  end

  @doc false
  @spec queue_table_name(Queue.name()) :: String.t()
  def queue_table_name(queue), do: "q_#{queue}"

  @doc false
  @spec schema :: String.t()
  def schema, do: @extension

  ################################
  # Private API
  ################################

  defp pg_type(%Duration{}), do: "interval"
  defp pg_type(%DateTime{}), do: "timestamp with time zone"
  defp pg_type(int) when is_integer(int), do: "integer"
end
