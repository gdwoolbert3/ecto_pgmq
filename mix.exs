defmodule EctoPGMQ.MixProject do
  use Mix.Project

  @github "https://github.com/gdwoolbert3/ecto_pgmq"
  @version "1.0.0"

  ################################
  # Public API
  ################################

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  def project do
    [
      aliases: aliases(),
      app: :ecto_pgmq,
      cli: cli(),
      deps: deps(),
      description: description(),
      dialyzer: dialyzer(),
      docs: docs(),
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      start_permanent: Mix.env() == :prod,
      test_coverage: test_coverage(),
      version: @version
    ]
  end

  ################################
  # Private API
  ################################

  defp aliases do
    [
      ci: [
        "compile --warnings-as-errors",
        "format --check-formatted",
        "credo --strict",
        "test --cover --export-coverage default",
        "dialyzer --format github"
      ]
    ]
  end

  def cli do
    [
      preferred_envs: [ci: :test]
    ]
  end

  defp deps do
    [
      {:ecto_sql, "~> 3.11"},
      {:postgrex, ">= 0.0.0"},
      {:broadway, "~> 1.0", optional: true},
      {:ex_doc, "~> 0.40.0", only: :dev, runtime: false},
      {:credo, "~> 1.7.13", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.7", only: [:dev, :test], runtime: false},
      {:styler, "~> 1.9.1", only: [:dev, :test], runtime: false}
    ]
  end

  defp description do
    """
    An opinionated PGMQ client for Elixir.
    """
  end

  defp dialyzer do
    [
      plt_file: {:no_warn, "dialyzer/dialyzer.plt"},
      plt_add_apps: [:ex_unit, :mix]
    ]
  end

  defp docs do
    [
      authors: ["Gordon Woolbert"],
      extras: [
        "README.md",
        "CONTRIBUTING.md",
        "CHANGELOG.md"
      ],
      groups_for_modules: [
        Core: [
          EctoPGMQ,
          EctoPGMQ.Migrations,
          EctoPGMQ.Notifications,
          EctoPGMQ.PGMQ,
          EctoPGMQ.Producer
        ],
        Schemas: [
          EctoPGMQ.Message,
          EctoPGMQ.Metrics,
          EctoPGMQ.Queue,
          EctoPGMQ.Throttle
        ]
      ],
      main: "readme",
      source_url: @github
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      files: ["lib", "mix.exs", "README*", "LICENSE"],
      maintainers: ["Gordon Woolbert"],
      licenses: ["MIT"],
      links: %{"GitHub" => @github}
    ]
  end

  defp test_coverage do
    [
      ignore_modules: []
    ]
  end
end
