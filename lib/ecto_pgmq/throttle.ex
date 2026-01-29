defmodule EctoPGMQ.Throttle do
  @moduledoc """
  Schema for PGMQ queue notification throttles.

  This schema can be queried directly but throttles are fetched transparently
  when querying queues via `EctoPGMQ.Queue.query/0`.

  ## Examples

      iex> throttles = Repo.all(EctoPGMQ.Throttle)
      iex> Enum.all?(throttles, &is_struct(&1, EctoPGMQ.Throttle))
      true
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
  @schema_prefix EctoPGMQ.PGMQ.schema()
  schema "notify_insert_throttle" do
    field(:queue, :string, primary_key: true, source: :queue_name)
    field(:throttle, DurationType, source: :throttle_interval_ms, time_unit: :millisecond)
    field(:last_notified_at, :utc_datetime)
  end
end
