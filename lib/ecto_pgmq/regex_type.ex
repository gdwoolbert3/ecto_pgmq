defmodule EctoPGMQ.RegexType do
  @moduledoc false

  use Ecto.Type

  ################################
  # Type Callbacks
  ################################

  @doc false
  @impl Ecto.Type
  @spec cast(term()) :: {:ok, Regex.t()} | :error
  def cast(source) when is_binary(source) do
    with {:error, _} <- Regex.compile(source) do
      :error
    end
  end

  def cast(%Regex{} = regex), do: {:ok, regex}
  def cast(nil), do: {:ok, nil}
  def cast(_), do: :error

  @doc false
  @impl Ecto.Type
  @spec dump(term()) :: {:ok, String.t()} | :error
  def dump(%Regex{} = regex), do: {:ok, Regex.source(regex)}
  def dump(source) when is_binary(source), do: {:ok, source}
  def dump(nil), do: {:ok, nil}
  def dump(_), do: :error

  @doc false
  @impl Ecto.Type
  @spec equal?(Regex.t(), Regex.t()) :: boolean()
  def equal?(%Regex{} = regex_1, %Regex{} = regex_2) do
    Regex.source(regex_1) == Regex.source(regex_2)
  end

  @doc false
  @impl Ecto.Type
  @spec load(term()) :: {:ok, Regex.t()} | :error
  def load(source) when is_binary(source) do
    with {:error, _} <- Regex.compile(source) do
      :error
    end
  end

  def load(%Regex{} = regex), do: {:ok, regex}
  def load(nil), do: {:ok, nil}
  def load(_), do: :error

  @doc false
  @impl Ecto.Type
  @spec type :: :text
  def type, do: :text
end
