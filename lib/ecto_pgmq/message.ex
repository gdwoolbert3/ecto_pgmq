defmodule EctoPGMQ.Message do
  @moduledoc """
  Schema for PGMQ messages.
  """

  use Ecto.Schema

  import Ecto.Query, only: [select_merge: 2, select_merge: 3]

  alias EctoPGMQ.PGMQ
  alias EctoPGMQ.Queue

  ################################
  # Types
  ################################

  @typedoc """
  A PGMQ message group.

  For more information about FIFO message groups, see
  [FIFO Message Groups](`m:EctoPGMQ#fifo-message-groups`).
  """
  @type group :: String.t()

  @typedoc "PGMQ message headers."
  @type headers :: %{optional(String.Chars.t()) => term()}

  @typedoc "A PGMQ message ID."
  @type id :: pos_integer()

  @typedoc "A PGMQ message payload."
  @type payload :: %{optional(String.Chars.t()) => term()}

  @typedoc """
  A PGMQ message specification.

  > #### Warning {: .warning}
  >
  > If the group is not `nil`, it will override any group that may already be
  > specified in the headers.
  """
  @type specification :: payload() | {payload(), group() | nil} | {payload(), group() | nil, headers() | nil}

  @typedoc "A PGMQ message."
  @type t :: %__MODULE__{
          id: id(),
          reads: non_neg_integer(),
          enqueued_at: DateTime.t(),
          archived_at: DateTime.t() | nil,
          visible_at: DateTime.t(),
          last_read_at: DateTime.t() | nil,
          payload: payload(),
          headers: headers() | nil,
          group: group() | nil
        }

  ################################
  # Schema
  ################################

  @primary_key false
  @schema_prefix PGMQ.schema()
  embedded_schema do
    field(:id, :integer, primary_key: true, source: :msg_id)
    field(:reads, :integer, source: :read_ct)
    field(:enqueued_at, :utc_datetime_usec)
    field(:archived_at, :utc_datetime_usec, load_in_query: false)
    field(:visible_at, :utc_datetime_usec, source: :vt)
    field(:last_read_at, :utc_datetime_usec)
    field(:payload, :map, source: :message)
    field(:headers, :map)
    field(:group, :string, virtual: true)
  end

  ################################
  # Query API
  ################################

  @doc """
  Returns a query for the messages in the archive for the given queue.

  ## Examples

      iex> message_ids = EctoPGMQ.send_messages(Repo, "my_queue", [%{"foo" => 1}, %{"bar" => 2}])
      iex> EctoPGMQ.archive_messages(Repo, "my_queue", message_ids)
      iex> messages = Repo.all(archive_query("my_queue"))
      iex> Enum.all?(messages, &is_struct(&1, EctoPGMQ.Message))
      true
  """
  @spec archive_query(Queue.name()) :: Ecto.Query.t()
  def archive_query(queue) do
    queue
    |> PGMQ.archive_table_name()
    |> base_query()
    |> select_merge([m], %{archived_at: m.archived_at})
  end

  @doc """
  Returns a query for the messages in the given queue.

  ## Options

    * `:archived_at?` - An optional `t:boolean/0` denoting whether or not to
      select a `NULL` `:archived_at` column. This can be used to make the query
      structure match that of `archive_query/1`. Defaults to `false`.

  ## Examples

      iex> EctoPGMQ.send_messages(Repo, "my_queue", [%{"foo" => 1}, %{"bar" => 2}])
      iex> messages = Repo.all(queue_query("my_queue"))
      iex> Enum.all?(messages, &is_struct(&1, EctoPGMQ.Message))
      true
  """
  @spec queue_query(Queue.name()) :: Ecto.Query.t()
  @spec queue_query(Queue.name(), keyword()) :: Ecto.Query.t()
  def queue_query(queue, opts \\ []) do
    base_query =
      queue
      |> PGMQ.queue_table_name()
      |> base_query()

    if Keyword.get(opts, :archived_at?, false) do
      select_merge(base_query, %{archived_at: nil})
    else
      base_query
    end
  end

  ################################
  # Utility API
  ################################

  @doc false
  @spec normalize_specification(specification()) :: specification()
  def normalize_specification(payload) when is_map(payload) do
    {payload, nil, nil}
  end

  def normalize_specification({payload, group}) do
    {group, headers} = group_and_headers(group)
    {payload, group, headers}
  end

  def normalize_specification({payload, group, headers}) do
    {group, headers} = group_and_headers(group, headers)
    {payload, group, headers}
  end

  ################################
  # Private API
  ################################

  defp base_query(table) do
    PGMQ.decorate_message_query({table, __MODULE__})
  end

  defp group_and_headers(group, headers \\ nil)
  defp group_and_headers(nil, nil), do: {nil, nil}
  defp group_and_headers(group, nil), do: group_and_headers(group, %{})

  defp group_and_headers(nil, headers) do
    {Map.get(headers, PGMQ.group_header()), headers}
  end

  defp group_and_headers(group, headers) do
    {group, Map.put(headers, PGMQ.group_header(), group)}
  end
end
