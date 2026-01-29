defmodule EctoPGMQ.DurationType do
  @moduledoc false

  use Ecto.ParameterizedType

  @time_units [:second, :millisecond]

  ################################
  # Types
  ################################

  @typedoc false
  @type time_unit :: :second | :millisecond

  ################################
  # Public API
  ################################

  @doc false
  @spec to_time(Duration.t(), time_unit()) :: integer()
  def to_time(%Duration{year: 0, month: 0} = duration, :second), do: collapse_seconds(duration)

  def to_time(%Duration{year: 0, month: 0, microsecond: {micro, _}} = duration, :millisecond) do
    duration
    |> collapse_seconds()
    |> System.convert_time_unit(:second, :millisecond)
    |> Kernel.+(System.convert_time_unit(micro, :microsecond, :millisecond))
  end

  def to_time(%Duration{} = duration, unit) when unit in @time_units do
    raise ArgumentError, """
    Duration structs that span multiple months are not supported: #{inspect(duration)}
    """
  end

  ################################
  # ParameterizedType Callbacks
  ################################

  @doc false
  @impl Ecto.ParameterizedType
  @spec init(keyword()) :: time_unit()
  def init(opts) do
    case Keyword.get(opts, :time_unit, :second) do
      unit when unit in @time_units -> unit
      unit -> raise ArgumentError, "Invalid time unit: #{inspect(unit)}"
    end
  end

  @doc false
  @impl Ecto.ParameterizedType
  @spec cast(term(), time_unit()) :: {:ok, Duration.t()} | :error
  def cast(time, unit) when is_integer(time), do: {:ok, from_time(time, unit)}
  def cast(%Duration{year: 0, month: 0} = duration, _), do: {:ok, duration}
  def cast(_, _), do: :error

  @doc false
  @impl Ecto.ParameterizedType
  @spec dump(term(), function(), time_unit()) :: {:ok, integer() | nil} | :error
  def dump(%Duration{year: 0, month: 0} = duration, _, unit), do: {:ok, to_time(duration, unit)}
  def dump(time, _, _) when is_integer(time), do: {:ok, time}
  def dump(nil, _, _), do: {:ok, nil}
  def dump(_, _, _), do: :error

  @doc false
  @impl Ecto.ParameterizedType
  @spec load(term(), function(), time_unit()) :: {:ok, Duration.t() | nil} | :error
  def load(time, _, unit) when is_integer(time), do: {:ok, from_time(time, unit)}
  def load(%Duration{} = duration, _, _), do: {:ok, duration}
  def load(nil, _, _), do: {:ok, nil}
  def load(_, _, _), do: :error

  @doc false
  @impl Ecto.ParameterizedType
  @spec type(time_unit()) :: :integer
  def type(_), do: :integer

  ################################
  # Private API
  ################################

  defp from_time(seconds, :second), do: Duration.new!(second: seconds)

  defp from_time(milliseconds, :millisecond) do
    # Always rounds down, effectively div(milliseconds, 1_000)
    seconds = System.convert_time_unit(milliseconds, :millisecond, :second)

    microseconds =
      case rem(milliseconds, 1_000) do
        0 -> {0, 0}
        milli -> {System.convert_time_unit(milli, :millisecond, :microsecond), 3}
      end

    Duration.new!(second: seconds, microsecond: microseconds)
  end

  defp collapse_seconds(d) do
    # Effectively reduces from weeks down to seconds
    (((d.week * 7 + d.day) * 24 + d.hour) * 60 + d.minute) * 60 + d.second
  end
end
