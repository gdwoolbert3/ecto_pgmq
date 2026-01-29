# Contributing

Contributions are always welcome!

---

## Setup

This section contains instructions for creating a local development environment
for the `EctoPGMQ` application.

### Environment Variables

In order to run the project we must first setup some environment variables.
These environment variables should be stored in a file named `.envrc`.

The necessary environment variables aren't actually secrets and can just be
copied from `.envrc.example`:

```bash
cp .envrc.example .envrc
```

Below are a few tools that be used to source environment variables:

<!-- tabs-open -->

### Mise (Preferred)

If you have [mise](https://mise.jdx.dev/) installed, environment variables can
be sourced with the following command:

```bash
mise trust
```

### direnv

If you have [direnv](https://direnv.net/) installed, environment variables can
be sourced with the following command:

```bash
direnv allow .
```

Updates to the `.envrc` file will not take effect until the above command is run
again.

<!-- tabs-close -->

### External Services

As one would expect, this project requires a
[PostgreSQL](https://www.postgresql.org/) instance with certain extensions
installed. By default, we create a custom docker image and run a containerized
version of this service to make local development more ergonomic. The
configuration for this container can be found in the `docker-compose.yml` file.

Below are a few tools that can be used to run containers:

<!-- tabs-open -->

### Docker

If you have [Docker](https://docs.docker.com/desktop/) installed, the containers
can be run with the following command:

```bash
docker compose up
```

### Podman

If you have [Podman](https://podman.io/) installed, the containers can be run
with the following command:

```bash
podman compose up
```

### OrbStack

If you have [OrbStack](https://docs.orbstack.dev/) installed, the containers can
be run with the following command:

```bash
docker compose up
```

<!-- tabs-close -->

Regardless of what tool you use to run the container, you should see a message
like this when everything is up and running:

```text
All services started and healthy!
```

Alternatively, while the project is set up to work with the aforementioned
docker image, any Postgres instance can be used so long as the following
extensions are available:

- [pg_partman](https://github.com/pgpartman/pg_partman)
- [pgmq](https://github.com/pgmq/pgmq)

The `ECTO_PGMQ_POSTGRES_URL` environment variable will just need to be updated
accordingly.

### Erlang and Elixir

This project is written almost entirely in [Elixir](https://elixir-lang.org/)
but, since Elixir is built on top of [Erlang](https://www.erlang.org/), we need
both languages installed.

The specific language versions used by the project can be found in the
`.tool-versions` file.

Below are a few tools that can be used to manage language versions:

<!-- tabs-open -->

### Mise (Preferred)

If you have [mise](https://mise.jdx.dev/) installed, the language versions from
the `.tool-versions` file can be installed with the following commands:

```bash
mise trust
mise install
```

### asdf

If you're using [asdf](https://asdf-vm.com/), you first need to install the
language plugins:

```bash
asdf plugin add erlang
asdf plugin add elixir
```

Once you have the plugins installed, the language versions from the
`.tool-versions` file can be installed with the following command:

```bash
asdf install
```

If the above command fails to install a language version, you may need to
troubleshoot using the corresponding plugin's documentation:

- [Erlang](https://github.com/asdf-vm/asdf-erlang) plugin
- [Elixir](https://github.com/asdf-vm/asdf-elixir) plugin

<!-- tabs-close -->

Once Elixir and Erlang are installed, we can install their respective package
managers if they aren't already available:

```bash
mix local.hex --if-missing --force
mix local.rebar --if-missing --force
```

### Application

Once our environment variables are set, Postgres is running, and the necessary
languages are installed, we can use the following command to install the project
dependencies:

```bash
mix deps.get
```

From there, the application can be started in "REPL" mode with the following
command:

```bash
iex -S mix
```

---

## Testing

Tests can be run locally with the following command:

```bash
mix test
```

The CI workflow runs tests but also includes a few additional checks. This
process can be run locally with the following command:

```bash
mix ci
```

---

## Documentation

This project's documentation can be generated locally with the following
command:

```bash
mix docs
```

The generated documentation can be viewed in a browser with the following
command:

```bash
open doc/index.html
```

---

## Debugging

You can optionally interact with the PostgreSQL container using `psql`. This
tool is available if you've
[installed PostgreSQL](https://www.postgresql.org/download/).

Using `psql`, you can open a SQL terminal with the following command:

```bash
# Password is "ecto_pgmq"
psql -h localhost -p 5432 -U ecto_pgmq -d ecto_pgmq
```

Alternatively, if `psql` is not available, and you're running a Postgres
container, you can execute the above command _inside_ of the container to open a
SQL terminal. The exact command is dependent on the tool you're using to run the
containers, but here is an example:

```bash
# Find the Postgres container name/ID
docker ps

# Replace "ecto_pgmq-postgres" with the container name/ID
docker exec -it ecto_pgmq-postgres psql -U ecto_pgmq -d ecto_pgmq
```

> #### Tip {: .tip}
>
> All tests that interact with the Postgres container remove any inserted data
> upon completion, so don't be surprised if the database is empty after a test
> run.

---

## Pull Request Process

Before submitting a pull request, make sure that:

1. The CI process is passing locally
1. Any relevant documentation is updated
1. The pull request description contains adequate context for a reviewer

---

## Potential Future Enhancements

Itching to contribute but not sure where to start? Here's a list of potential
enhancements that could benefit the application:

- Investigate adding a notion of a worker and leveraging it to limit the number
  of active consumers

- Investigate adding a scheduled job framework (maybe on top of
  [pg_cron](https://github.com/citusdata/pg_cron))

- Update the PGMQ extension to fix SQL-based installation failures

---

## Code of Conduct

### Our Pledge

In the interest of fostering an open and welcoming environment, we as
contributors and maintainers pledge to making participation in our project and
our community a harassment-free experience for everyone, regardless of age, body
size, disability, ethnicity, gender identity and expression, level of experience,
nationality, personal appearance, race, religion, or sexual identity and
orientation.

### Our Standards

Examples of behavior that contributes to creating a positive environment
include:

* Using welcoming and inclusive language
* Being respectful of differing viewpoints and experiences
* Gracefully accepting constructive criticism
* Focusing on what is best for the community
* Showing empathy towards other community members

Examples of unacceptable behavior by participants include:

* The use of sexualized language or imagery and unwelcome sexual attention or
advances
* Trolling, insulting/derogatory comments, and personal or political attacks
* Public or private harassment
* Publishing others' private information, such as a physical or electronic
  address, without explicit permission
* Other conduct which could reasonably be considered inappropriate in a
  professional setting

### Our Responsibilities

Project maintainers are responsible for clarifying the standards of acceptable
behavior and are expected to take appropriate and fair corrective action in
response to any instances of unacceptable behavior.

Project maintainers have the right and responsibility to remove, edit, or
reject comments, commits, code, wiki edits, issues, and other contributions
that are not aligned to this Code of Conduct, or to ban temporarily or
permanently any contributor for other behaviors that they deem inappropriate,
threatening, offensive, or harmful.

### Scope

This Code of Conduct applies both within project spaces and in public spaces
when an individual is representing the project or its community. Examples of
representing a project or community include using an official project e-mail
address, posting via an official social media account, or acting as an appointed
representative at an online or offline event. Representation of a project may be
further defined and clarified by project maintainers.

### Enforcement

Instances of abusive, harassing, or otherwise unacceptable behavior may be
reported by contacting the project owner at `gordonwoolbert3@gmail.com`. All
complaints will be reviewed and investigated and will result in a response that
is deemed necessary and appropriate to the circumstances. The project team is
obligated to maintain confidentiality with regard to the reporter of an incident.
Further details of specific enforcement policies may be posted separately.

Project maintainers who do not follow or enforce the Code of Conduct in good
faith may face temporary or permanent repercussions as determined by other
members of the project's leadership.

### Attribution

This Code of Conduct is adapted from the [Contributor Covenant][homepage],
version 1.4, available at [http://contributor-covenant.org/version/1/4][version]
