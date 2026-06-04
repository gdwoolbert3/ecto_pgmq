# PGMQ Installation

This guide covers the installation and management of the PGMQ extension.

## Installation Methods

There are two ways to install the PGMQ extension:

### Extension Installation

The preferred way to install PGMQ is to treat it as a traditional Postgres
extension. This installation method requires one of the following:

  * The ability to add the necessary
    [extension files](https://www.postgresql.org/docs/current/extend-extensions.html#EXTEND-EXTENSIONS-FILES)
    to the Postgres instance file system

  * Pre-installed PGMQ on the Postgres instance (ie
    [Supabase](https://supabase.com/docs/guides/queues/pgmq))

If necessary files are available, the PGMQ extension can then be created,
updated, or dropped using the "Extension Installation API" functions in the
`EctoPGMQ.Migrations` module.

If neither of the above requirements can be met, PGMQ must be installed using
the [SQL Installation](#sql-installation) method.

### SQL Installation

Because PGMQ is entirely made up of SQL objects, it is possible to install it by
manually defining the `pgmq` schema and its members. In practice, the best way
to do this is by running the `.sql` scripts that can be found in the
[source code](https://github.com/pgmq/pgmq/tree/main/pgmq-extension/sql).

If the scripts are availble to the _application_, the `pgmq` schema can be
created, updated, or dropped using the "SQL Installation API" functions in the
`EctoPGMQ.Migrations` module:

```elixir
defmodule MyApp.Repo.Migrations.CreatePgmq do
  use Ecto.Migration

  alias EctoPGMQ.Migrations

  def change do
    :my_app
    |> :code.priv_dir()
    |> Path.join("repo/extensions/pgmq--1.11.2.sql")
    |> Migrations.import_schema()
  end
end
```

## Additional Information

For more information about managing the PGMQ extension, see the PGMQ
[Installation](https://github.com/pgmq/pgmq/blob/main/INSTALLATION.md) and
[Updating](https://github.com/pgmq/pgmq/blob/main/pgmq-extension/UPDATING.md)
guides.
