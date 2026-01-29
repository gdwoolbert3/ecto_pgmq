defmodule EctoPGMQ.Queue do
  @moduledoc """
  Schema for PGMQ queues.
  """

  use Ecto.Schema

  import Ecto.Query, only: [join: 5, preload: 2, select: 3]

  ################################
  # Types
  ################################

  @typedoc "A PGMQ queue name."
  @type name :: String.t()

  @typedoc "A PGMQ queue."
  @type t :: %__MODULE__{
          name: name(),
          created_at: DateTime.t(),
          is_partitioned: boolean(),
          is_unlogged: boolean(),
          metrics: EctoPGMQ.Metrics.t() | nil,
          notifications: EctoPGMQ.Throttle.t() | nil
        }

  ################################
  # Schema
  ################################

  @primary_key false
  embedded_schema do
    field(:name, :string, source: :queue_name, primary_key: true)
    field(:created_at, :utc_datetime)
    field(:is_partitioned, :boolean)
    field(:is_unlogged, :boolean)
    field(:metrics, :map, virtual: true)

    has_one(:notifications, EctoPGMQ.Throttle, foreign_key: :queue, references: :name)
  end

  ################################
  # Public API
  ################################

  @doc """
  Returns a query for all queues.

  The returned query joins and populates queue metrics and queue notification
  throttles.

  ## Examples

      iex> queues = Repo.all(query())
      iex> Enum.all?(queues, &is_struct(&1, EctoPGMQ.Queue))
      true
  """
  @spec query :: Ecto.Query.t()
  def query do
    EctoPGMQ.PGMQ.list_queues_query()
    |> preload(:notifications)
    |> join(:left, [q], m in subquery(EctoPGMQ.PGMQ.metrics_all_query()), on: q.name == m.queue)
    |> select([q, m], %{q | metrics: m})
  end
end
