defmodule EctoPGMQ.TestType do
  @moduledoc """
  An `Ecto.Type` to be used for `EctoPGMQ` unit tests.

  This type is effectively a lightweight `Date.Range` wrapper.
  """

  use Ecto.Type

  ################################
  # Type Callbacks
  ################################

  @doc false
  @impl Ecto.Type
  @spec cast(term()) :: {:ok, Date.Range.t()} | :error
  def cast(%Date.Range{} = range), do: {:ok, range}
  def cast(_), do: :error

  @doc false
  @impl Ecto.Type
  @spec dump(term()) :: {:ok, map()} | :error
  def dump(%Date.Range{} = range) do
    map =
      %{
        "first" => Date.to_iso8601(range.first),
        "last" => Date.to_iso8601(range.last),
        "step" => range.step
      }

    {:ok, map}
  end

  def dump(_), do: :error

  @doc false
  @impl Ecto.Type
  @spec load(term()) :: {:ok, Date.Range.t()} | :error
  def load(%{"first" => first, "last" => last, "step" => step}) do
    range =
      Date.range(
        Date.from_iso8601!(first),
        Date.from_iso8601!(last),
        step
      )

    {:ok, range}
  end

  def load(_), do: :error

  @doc false
  @impl Ecto.Type
  @spec type :: :map
  def type, do: :map
end
