# EctoPGMQ

![CI](https://github.com/gdwoolbert3/ecto_pgmq/actions/workflows/ci.yml/badge.svg)
[![Package](https://img.shields.io/hexpm/v/ecto_pgmq.svg)](https://hex.pm/packages/ecto_pgmq)

An opinionated [PGMQ](https://github.com/pgmq/pgmq) client for Elixir.

## Installation

This package can be installed by adding `:ecto_pgmq` to your list of
dependencies in `mix.exs`:

```elixir
defp deps do
  [
    # Should already be present if you're using Ecto
    {:ecto_sql, "~> 3.11"},
    {:postgrex, ">= 0.0.0"},
    # Required to use the EctoPGMQ.Producer module
    {:broadway, "~> 1.0"},
    {:ecto_pgmq, "~> 1.0"}
  ]
end
```

## Documentation

For additional documentation, see
[HexDocs](https://hexdocs.pm/ecto_pgmq/readme.html).

## Usage

`EctoPGMQ` is designed to be used seamlessly with your application repo. Some
common operations are showcased below but the full feature-space can be found in
the [Documentation](#documentation).

### Installing the PGMQ Extension

The most common way to install the PGMQ extension is in a database migration. We
can create a database migration with the following command:

```bash
mix ecto.gen.migration create_pgmq
```

We can then implement the migration accordingly:

```elixir
defmodule MyApp.Repo.Migrations.CreatePgmq do
  use Ecto.Migration

  alias EctoPGMQ.Migrations

  def change, do: Migrations.create_extension()
end
```

For more information about installing PGMQ, see
[PGMQ Installation](`m:EctoPGMQ#pgmq-installation`).

### Creating a Queue

The most common way to create a queue is in a database migration. We can create
a database migration with the following command:

```bash
mix ecto.gen.migration create_my_queue
```

We can then implement the migration accordingly:

```elixir
defmodule MyApp.Repo.Migrations.CreateMyQueue do
  use Ecto.Migration

  alias EctoPGMQ.Migrations

  def change, do: Migrations.create_queue("my_queue")
end
```

### Sending Messages

Messages can be sent to a queue with the following function:

```elixir
EctoPGMQ.send_messages(MyApp.Repo, "my_queue", [%{"foo" => 1}, %{"bar" => 2}])
```

### Reading Messages

Messages can be read from a queue with the following function:

```elixir
messages = EctoPGMQ.read_messages(MyApp.Repo, "my_queue", 300, 2)
```

Alternatively, messages can also be consumed in a `Broadway` pipeline with the
`EctoPGMQ.Producer` module.

### Dropping a Queue

The most common way to drop a queue is in a database migration. We can create a
database migration with the following command:

```bash
mix ecto.gen.migration drop_my_queue
```

We can then implement the migration accordingly:

```elixir
defmodule MyApp.Repo.Migrations.DropMyQueue do
  use Ecto.Migration

  alias EctoPGMQ.Migrations

  def change, do: Migrations.drop_queue("my_queue")
end
```
