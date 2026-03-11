defmodule EctoPGMQ.Binding do
  @moduledoc """
  Schema for PGMQ queue bindings.

  > #### Read-Only {: .warning}
  >
  > This schema should be treated as read-only. Queue bindings can be configured
  > with `EctoPGMQ.create_queue/4` and `EctoPGMQ.drop_queue/3`.

  ## Message Routing

  TODO(Gordon) - Add this
  """

  use Ecto.Schema

  alias EctoPGMQ.RegexType

  ################################
  # Types
  ################################

  @typedoc """
  A PGMQ binding pattern.

  Binding patterns must meet the following conditions:

    * Must only contain alphanumeric characters, dots (`.`), hyphens (`-`),
      underscores (`_`), and wildcards (`*` and `#`).

    * Cannot start with a dot.

    * Cannot contain consecutive dots or wildcards.

    * Cannot be longer than 255 characters.

  Binding patterns can be validated with
  `EctoPGMQ.PGMQ.validate_topic_pattern/3`.
  """
  @type pattern :: String.t()

  @typedoc "A PGMQ queue binding."
  @type t :: %__MODULE__{
          queue: EctoPGMQ.Queue.name(),
          pattern: pattern(),
          regex: Regex.t()
        }

  ################################
  # Schema
  ################################

  @primary_key false
  embedded_schema do
    field(:queue, :string, primary_key: true, source: :queue_name)
    field(:pattern, :string, primary_key: true)
    field(:regex, RegexType, source: :compiled_regex)
    field(:bound_at, :utc_datetime_usec, load_in_query: false)
  end

  ################################
  # Public API
  ################################

  @doc """
  Returns a query for queue bindings.

  Bindings are fetched transparently when querying queues via
  `EctoPGMQ.Queue.query/0`.

  ## Regex Filtering

  This schema uses a custom `Ecto.Type` to load the stored compiled regex into a
  `t:Regex.t/0`. This custom type can cast both `t:Regex.t/0` structs and
  `t:binary/0` sources:

  ```elixir
  # Casting a Regex struct
  where(query(), [b], b.regex == ^~r/^.*$/)

  # Casting a binary source
  where(query(), [b], b.regex == "^.*$")
  ```

  It is also possible to check for a regex match using the tools provided by
  `Ecto.Query`:

  ```elixir
  where(query(), [b], fragment("? ~ ?", "my.routing.key", b.regex))
  ```

  ## Examples

      iex> [%Binding{} | _] = Repo.all(query())
  """
  @spec query :: Ecto.Query.t()
  defdelegate query, to: EctoPGMQ.PGMQ, as: :list_topic_bindings_query
end
