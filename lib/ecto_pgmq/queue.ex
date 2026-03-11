defmodule EctoPGMQ.Queue do
  @moduledoc """
  Schema for PGMQ queues.

  > #### Read-Only {: .warning}
  >
  > This schema should be treated as read-only.
  """

  use Ecto.Schema

  import Ecto.Query, only: [preload: 2]

  alias EctoPGMQ.Binding
  alias EctoPGMQ.Metrics
  alias EctoPGMQ.Throttle

  ################################
  # Types
  ################################

  @typedoc "A PGMQ queue name."
  @type name :: String.t()

  @typedoc "A PGMQ queue."
  @type t :: %__MODULE__{
          name: name(),
          created_at: DateTime.t(),
          partitioned?: boolean(),
          unlogged?: boolean(),
          metrics: Metrics.t() | nil,
          notifications: Throttle.t() | nil,
          bindings: [Binding.t()]
        }

  ################################
  # Schema
  ################################

  @primary_key false
  embedded_schema do
    field(:name, :string, source: :queue_name, primary_key: true)
    field(:created_at, :utc_datetime_usec)
    field(:partitioned?, :boolean, source: :is_partitioned)
    field(:unlogged?, :boolean, source: :is_unlogged)

    has_one(:metrics, Metrics, foreign_key: :queue, references: :name)
    has_one(:notifications, Throttle, foreign_key: :queue, references: :name)
    has_many(:bindings, Binding, foreign_key: :queue, references: :name)
  end

  ################################
  # Public API
  ################################

  @doc """
  Returns a query for all queues.

  The returned query joins and populates bindings, metrics, and notification
  throttles.

  ## Examples

      iex> [%Queue{} | _] = Repo.all(query())
  """
  @spec query :: Ecto.Query.t()
  def query do
    preload(
      EctoPGMQ.PGMQ.list_queues_query(),
      bindings: ^Binding.query(),
      metrics: ^Metrics.query(),
      notifications: ^Throttle.query()
    )
  end
end
