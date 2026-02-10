defmodule EctoPGMQ.Migrations do
  @moduledoc """
  The entrypoint for managing PGMQ-related `Ecto.Migration` implementations.
  """

  alias Ecto.Migration
  alias EctoPGMQ.PGMQ
  alias EctoPGMQ.Queue

  ################################
  # EctoPGMQ API
  ################################

  @doc """
  Creates a queue with the given name in an `Ecto.Migration`.

  For more information, see `EctoPGMQ.create_queue/3`.

  ## Examples

  ```elixir
  create_queue("my_queue")
  ```
  """
  @doc group: "EctoPGMQ API"
  @spec create_queue(Queue.name()) :: :ok
  @spec create_queue(Queue.name(), EctoPGMQ.queue_create_attributes()) :: :ok
  @spec create_queue(Queue.name(), EctoPGMQ.queue_create_attributes(), [PGMQ.query_opt()]) :: :ok
  def create_queue(queue, attributes \\ %{}, opts \\ []) do
    repo = Migration.repo()

    Migration.execute(
      fn -> EctoPGMQ.create_queue(repo, queue, attributes, opts) end,
      fn -> EctoPGMQ.drop_queue(repo, queue, opts) end
    )
  end

  @doc """
  Drops the given queue in an `Ecto.Migration`.

  For more information, see `EctoPGMQ.drop_queue/3`.

  ## Examples

  ```elixir
  drop_queue("my_queue")
  ```
  """
  @doc group: "EctoPGMQ API"
  @spec drop_queue(Queue.name()) :: :ok
  @spec drop_queue(Queue.name(), [PGMQ.query_opt()]) :: :ok
  def drop_queue(queue, opts \\ []) do
    Migration.execute(fn ->
      EctoPGMQ.drop_queue(Migration.repo(), queue, opts)
    end)
  end

  @doc """
  Updates the given queue in an `Ecto.Migration`.

  For more information, see `EctoPGMQ.update_queue/4`.

  ## Examples

  ```elixir
  update_queue("my_queue", %{notifications: 1_000})
  ```
  """
  @doc group: "EctoPGMQ API"
  @spec update_queue(Queue.name(), EctoPGMQ.queue_update_attributes()) :: :ok
  @spec update_queue(Queue.name(), EctoPGMQ.queue_update_attributes(), [PGMQ.query_opt()]) :: :ok
  def update_queue(queue, attributes, opts \\ []) do
    Migration.execute(fn ->
      EctoPGMQ.update_queue(Migration.repo(), queue, attributes, opts)
    end)
  end

  ################################
  # Extension Installation API
  ################################

  @doc """
  Creates the PGMQ extension in an `Ecto.Migration`.

  For more information, see [PGMQ Installation](#pgmq-installation).

  ## Examples

  ```elixir
  create_extension()
  ```
  """
  @doc group: "Extension Installation API"
  @spec create_extension :: :ok
  def create_extension do
    Migration.execute(
      "CREATE EXTENSION #{PGMQ.extension()}",
      "DROP EXTENSION #{PGMQ.extension()}"
    )
  end

  @doc """
  Drops the PGMQ extension in an `Ecto.Migration`.

  For more information, see [PGMQ Installation](#pgmq-installation).

  ## Examples

  ```elixir
  drop_extension()
  ```
  """
  @doc group: "Extension Installation API"
  @spec drop_extension :: :ok
  def drop_extension do
    Migration.execute("DROP EXTENSION #{PGMQ.extension()}")
  end

  @doc """
  Updates the PGMQ extension to the default version in an `Ecto.Migration`.

  For more information, see [PGMQ Installation](#pgmq-installation).

  ## Examples

  ```elixir
  update_extension()
  ```
  """
  @doc group: "Extension Installation API"
  def update_extension do
    Migration.execute("ALTER EXTENSION #{PGMQ.extension()} UPDATE")
  end

  @doc """
  Updates the PGMQ extension in an `Ecto.Migration`.

  For more information, see [PGMQ Installation](#pgmq-installation).

  ## Examples

  ```elixir
  update_extension("1.9.0")

  update_extension(Version.parse!("1.9.0"))
  ```
  """
  @doc group: "Extension Installation API"
  @spec update_extension(Version.version()) :: :ok
  def update_extension(version) do
    Migration.execute(fn ->
      Migration.repo().query!(
        "ALTER EXTENSION #{PGMQ.extension()} UPDATE TO $1::text",
        [version]
      )
    end)
  end

  ################################
  # SQL Installation API
  ################################

  @doc """
  Imports a PGMQ schema file in an `Ecto.Migration`.

  > #### Warning {: .warning}
  >
  > This function leverages the same adapter callback as
  > [`mix ecto.load`](https://hexdocs.pm/ecto_sql/Mix.Tasks.Ecto.Load.html) and
  > therefore, requires that the `psql` shell utility is available.

  For more information, see [PGMQ Installation](#pgmq-installation).

  ## Examples

  ```elixir
  path =
    :my_app
    |> :code.priv_dir()
    |> Path.join("repo/extensions/pgmq--1.9.0.sql")

  import_schema(path)
  ```
  """
  @doc group: "SQL Installation API"
  @spec import_schema(Path.t()) :: :ok
  def import_schema(path) do
    Migration.execute(fn ->
      repo = Migration.repo()
      config = Keyword.put(repo.config(), :dump_path, path)

      case repo.__adapter__().structure_load(".", config) do
        {:ok, _} -> :ok
        {:error, output} -> raise inspect(output)
      end
    end)
  end

  @doc """
  Drops the PGMQ schema in an `Ecto.Migration`.

  > #### Warning {: .warning}
  >
  > This function should **NOT** be used if PGMQ is installed as an extension.

  For more information, see [PGMQ Installation](#pgmq-installation).

  ## Examples

  ```elixir
  drop_schema()
  ```
  """
  @doc group: "SQL Installation API"
  @spec drop_schema :: :ok
  def drop_schema do
    Migration.execute("DROP SCHEMA #{PGMQ.schema()} CASCADE")
  end
end
