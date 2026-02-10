defmodule EctoPGMQ.Message do
  @moduledoc """
  Schema for PGMQ messages.
  """

  use Ecto.Schema

  import Ecto.Query, only: [select_merge: 2, select_merge: 3]

  alias EctoPGMQ.PGMQ
  alias EctoPGMQ.Queue

  require Record

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

  @typedoc """
  A PGMQ message payload.

  For more information about valid PGMQ message payloads, see
  [Custom Payload Types](m:EctoPGMQ#custom-payload-types).
  """
  @type payload :: PGMQ.payload() | term()

  @typedoc """
  A PGMQ Message payload type.

  This can take any of the following forms:

    * `:map` to denote `t:map/0` payloads.

    * A `t:module/0` that implements the `Ecto.Type` behaviour and dumps to and
      loads from a `t:map/0`.

    * `{module, opts}` where `module` is a `t:module/0` that implements the
      `Ecto.ParameterizedType` behaviour and dumps to and loads from a `t:map/0`
      and `opts` is a `t:keyword/0` that contains the init parameters.

  For more information about custom PGMQ payloads, see
  [Custom Payload Types](m:EctoPGMQ#custom-payload-types).
  """
  @type payload_type :: module() | {module(), keyword()} | :map

  @typedoc """
  A PGMQ message specification.

  This type is public because it is safe to inspect  but, in most cases, message
  specifications should only be `constructed` with `build/3`.
  """
  @type specification :: record(:spec, payload: payload() | nil, group: group() | nil, headers: headers() | nil)

  @typedoc "A PGMQ message."
  @type t :: %__MODULE__{
          id: id(),
          reads: non_neg_integer(),
          enqueued_at: DateTime.t(),
          archived_at: DateTime.t() | nil,
          visible_at: DateTime.t(),
          last_read_at: DateTime.t() | nil,
          payload: payload() | nil,
          headers: headers() | nil,
          group: group() | nil
        }

  ################################
  # Private Records
  ################################

  Record.defrecordp(:spec, payload: nil, group: nil, headers: nil)

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
    field(:payload, :map, source: :message, load_in_query: false)
    field(:headers, :map)
    field(:group, :string, virtual: true)
  end

  ################################
  # Public API
  ################################

  @doc """
  Returns a query for the messages in the archive for the given queue.

  ## Options

  An archive message query can be built with the following options:

    * `:payload_type` - An optional `t:payload_type/0` for the message payloads.
      Defaults to `:map`.

  ## Examples

      iex> message_specs = [Message.build(%{"foo" => 1})]
      iex> message_ids = EctoPGMQ.send_messages(Repo, "my_queue", message_specs)
      iex> EctoPGMQ.archive_messages(Repo, "my_queue", message_ids)
      iex> [%Message{}] = Repo.all(archive_query("my_queue"))
  """
  @spec archive_query(Queue.name()) :: Ecto.Query.t()
  @spec archive_query(Queue.name(), [{:payload_type, payload_type()}]) :: Ecto.Query.t()
  def archive_query(queue, opts \\ []) do
    payload_ecto_type =
      opts
      |> Keyword.get(:payload_type, :map)
      |> payload_type_to_ecto_type()

    queue
    |> PGMQ.archive_table_name()
    |> message_table_query(payload_ecto_type)
    |> select_merge([m], %{archived_at: m.archived_at})
  end

  @doc """
  Constructs a message specification for the given payload, group, and headers.

  > #### Warning {: .warning}
  >
  > If the group is not `nil`, it will override any group that may already be
  > specified in the headers.
  """
  @spec build(payload() | nil) :: specification()
  @spec build(payload() | nil, group() | nil) :: specification()
  @spec build(payload() | nil, group() | nil, headers() | nil) :: specification()
  def build(payload, group \\ nil, headers \\ nil)
  def build(payload, nil, nil), do: spec(payload: payload)
  def build(payload, group, nil), do: build(payload, group, %{})

  def build(payload, nil, headers) do
    group = Map.get(headers, PGMQ.group_header())
    spec(payload: payload, group: group, headers: headers)
  end

  def build(payload, group, headers) do
    headers = Map.put(headers, PGMQ.group_header(), group)
    spec(payload: payload, group: group, headers: headers)
  end

  @doc """
  Returns a query for the messages in the given queue.

  ## Options

  A queue message query can be built with the following options:

    * `:archived_at?` - An optional `t:boolean/0` denoting whether or not to
      select a `NULL` `:archived_at` column. This can be used to make the query
      structure match that of `archive_query/1`. Defaults to `false`.

    * `:payload_type` - An optional `t:payload_type/0` for the message payloads.
      Defaults to `:map`.

  ## Examples

      iex> message_specs = [Message.build(%{"foo" => 1})]
      iex> EctoPGMQ.send_messages(Repo, "my_queue", message_specs)
      iex> [%Message{}] = Repo.all(queue_query("my_queue"))
  """
  @spec queue_query(Queue.name()) :: Ecto.Query.t()
  @spec queue_query(Queue.name(), [{:archived_at?, boolean()} | {:payload_type, payload_type()}]) :: Ecto.Query.t()
  def queue_query(queue, opts \\ []) do
    payload_ecto_type =
      opts
      |> Keyword.get(:payload_type, :map)
      |> payload_type_to_ecto_type()

    query =
      queue
      |> PGMQ.queue_table_name()
      |> message_table_query(payload_ecto_type)

    if Keyword.get(opts, :archived_at?, false) do
      select_merge(query, %{archived_at: nil})
    else
      query
    end
  end

  ################################
  # Protected Utility API
  ################################

  @doc false
  @spec to_pgmq_payloads_and_headers([specification()], payload_type()) :: {[PGMQ.payload()], [headers()]}
  def to_pgmq_payloads_and_headers(specs, payload_type) do
    payload_ecto_type = payload_type_to_ecto_type(payload_type)

    specs
    |> Enum.map(fn spec ->
      payload = spec(spec, :payload)
      headers = spec(spec, :headers)

      case Ecto.Type.dump(payload_ecto_type, payload) do
        {:ok, payload} when is_map(payload) ->
          {payload, headers}

        _ ->
          raise ArgumentError, """
          Unable to dump payload to map: #{inspect(payload)}
          """
      end
    end)
    |> Enum.unzip()
  end

  ################################
  # Private API
  ################################

  defp payload_type_to_ecto_type({type, opts}) do
    Ecto.ParameterizedType.init(type, opts)
  end

  defp payload_type_to_ecto_type(type), do: type

  defp message_table_query(table, ecto_type) do
    PGMQ.message_query_select({table, __MODULE__}, ecto_type)
  end
end
