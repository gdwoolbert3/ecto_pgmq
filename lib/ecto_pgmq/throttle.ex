defmodule EctoPGMQ.Throttle do
  @moduledoc """
  Schema for PGMQ queue notification throttles.

  > #### Read-Only {: .warning}
  >
  > This schema should be treated as read-only. Notification throttles can be
  > configured with `EctoPGMQ.create_queue/4` and `EctoPGMQ.drop_queue/3`.

  For more information about notification throttling, see
  [Throttling](m:EctoPGMQ.Notifications#throttling).
  """

  use Ecto.Schema

  alias EctoPGMQ.DurationType

  ################################
  # Types
  ################################

  @typedoc "A PGMQ queue notification throttle."
  @type t :: %__MODULE__{
          queue: EctoPGMQ.Queue.name(),
          interval: Duration.t(),
          last_notified_at: DateTime.t() | nil
        }

  ################################
  # Schema
  ################################

  @primary_key false
  embedded_schema do
    field(:queue, :string, primary_key: true, source: :queue_name)
    field(:interval, DurationType, source: :throttle_interval_ms, time_unit: :millisecond)
    field(:last_notified_at, :utc_datetime_usec)
  end

  ################################
  # Public API
  ################################

  @doc """
  Returns a query for notification throttles.

  Notification throttles are fetched transparently when querying queues via
  `EctoPGMQ.Queue.query/0`.

  ## Interval Filtering

  This schema uses a custom `Ecto.Type` to load the message age fields into a
  `t:Duration.t/0`. This custom type can cast both `t:Duration.t/0` structs and
  `t:non_neg_integer/0` times (in milliseconds):

  ```elixir
  # Casting a Duration struct
  where(query(), [t], t.interval >= ^Duration.new!(hour: 1))

  # Casting an integer time
  where(query(), [t], t.interval <= 250)
  ```

  ## Examples

      iex> [%Throttle{} | _] = Repo.all(query())
  """
  @spec query :: Ecto.Query.t()
  defdelegate query, to: EctoPGMQ.PGMQ, as: :list_notify_insert_throttles_query
end
