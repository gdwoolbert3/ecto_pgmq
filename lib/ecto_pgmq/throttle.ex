defmodule EctoPGMQ.Throttle do
  @moduledoc """
  Schema for PGMQ queue notification throttles.

  > #### Read-Only {: .warning}
  >
  > This schema should be treated as read-only. Notification throttles can be
  > configured with `EctoPGMQ.create_queue/4` and `EctoPGMQ.drop_queue/3`.

  For more information about notification throttling, see
  [Throttling](m:EctoPGMQ.Notifications#throttling).

  TODO(Gordon) - expose query function, remove source, update docs
  TODO(Gordon) - point at notifications
  TODO(Gordon) - rename "throttle" field to "interval"
  """

  use Ecto.Schema

  alias EctoPGMQ.DurationType

  ################################
  # Types
  ################################

  @typedoc "A PGMQ queue notification throttle."
  @type t :: %__MODULE__{
          queue: EctoPGMQ.Queue.name(),
          throttle: Duration.t(),
          last_notified_at: DateTime.t() | nil
        }

  ################################
  # Schema
  ################################

  @primary_key false
  embedded_schema do
    field(:queue, :string, primary_key: true, source: :queue_name)
    field(:throttle, DurationType, source: :throttle_interval_ms, time_unit: :millisecond)
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

  TODO(Gordon) - add this after changing field name

  ## Examples

      iex> [%Throttle{} | _] = Repo.all(query())
  """
  @spec query :: Ecto.Query.t()
  defdelegate query, to: EctoPGMQ.PGMQ, as: :list_notify_insert_throttles_query
end
