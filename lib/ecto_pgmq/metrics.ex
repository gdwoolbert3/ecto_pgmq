defmodule EctoPGMQ.Metrics do
  @moduledoc """
  Schema for PGMQ queue metrics.

  > #### Read-Only {: .warning}
  >
  > This schema should be treated as read-only.
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

  ## Message Age Filtering

  This schema uses a custom `Ecto.Type` to load the message age fields into a
  `t:Duration.t/0`. This custom type can cast both `t:Duration.t/0` structs and
  `t:non_neg_integer/0` times (in seconds):

  ```elixir
  # Casting a Duration struct
  where(query(), [m], m.oldest_message_age >= ^Duration.new!(hour: 1))

  # Casting an integer time
  where(query(), [m], m.newest_message_age <= 30)
  ```

  ## Examples

      iex> [%Metrics{} | _] = Repo.all(query())
  """
  @spec query :: Ecto.Query.t()
  defdelegate query, to: EctoPGMQ.PGMQ, as: :metrics_all_query
end
