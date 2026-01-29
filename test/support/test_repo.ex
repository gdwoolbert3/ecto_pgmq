defmodule EctoPGMQ.TestRepo do
  @moduledoc """
  An `Ecto.Repo` to be used for `EctoPGMQ` unit tests.
  """

  use Ecto.Repo, otp_app: :ecto_pgmq, adapter: Ecto.Adapters.Postgres

  ################################
  # Ecto.Repo Callbacks
  ################################

  @doc false
  @impl Ecto.Repo
  @spec init(:supervisor | :runtime, keyword()) :: {:ok, keyword()}
  def init(_context, config) do
    config =
      Keyword.merge(config,
        url: System.fetch_env!("ECTO_PGMQ_POSTGRES_URL"),
        pool: Ecto.Adapters.SQL.Sandbox,
        log: false
      )

    {:ok, config}
  end
end
