defmodule EctoPGMQ.Metrics do
  @moduledoc """
  Schema for PGMQ queue metrics.
  """

  use Ecto.Schema

  alias EctoPGMQ.DurationType

  ################################
  # Types
  ################################

  @typedoc "PGMQ queue metrics."
  @type t :: %__MODULE__{
          queue: EctoPGMQ.Queue.name(),
          total_messages: non_neg_integer(),
          visible_messages: non_neg_integer(),
          lifetime_messages: non_neg_integer(),
          newest_message_age: Duration.t() | nil,
          oldest_message_age: Duration.t() | nil,
          requested_at: DateTime.t()
        }

  ################################
  # Schema
  ################################

  @primary_key false
  embedded_schema do
    field(:queue, :string, source: :queue_name)
    field(:total_messages, :integer, source: :queue_length)
    field(:visible_messages, :integer, source: :queue_visible_length)
    field(:lifetime_messages, :integer, source: :total_messages)
    field(:newest_message_age, DurationType, source: :newest_msg_age_sec)
    field(:oldest_message_age, DurationType, source: :oldest_msg_age_sec)
    field(:requested_at, :utc_datetime_usec, source: :scrape_time)
  end

  ################################
  # Public API
  ################################

  @doc """
  Returns a query for queue metrics.

  Metrics are fetched transparently when querying queues via
  `EctoPGMQ.Queue.query/0`.

  > #### Warning {: .warning}
  >
  > This query only supports read operations.

  ## Examples

      iex> metrics = Repo.all(query())
      iex> Enum.all?(metrics, &is_struct(&1, EctoPGMQ.Metrics))
      true
  """
  @spec query :: Ecto.Query.t()
  defdelegate query, to: EctoPGMQ.PGMQ, as: :metrics_all_query
end
