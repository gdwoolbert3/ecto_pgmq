defmodule EctoPGMQ.TestPipeline do
  @moduledoc """
  A simple `Broadway` pipeline to be used for `EctoPGMQ` unit tests.

  This pipeline requires all processed messages to have an `"encoded_pid"` key
  in their payload that contains the `t:encoded_pid/0` of the test process (see
  `get_encoded_pid/0`). This value is used to send a message of the following
  shape back to the test process:

  ```elixir
  {:success | :failure, EctoPGMQ.Message.id()}
  ```

  This pipeline also supports two optional payload keys:

    * `"fail"` - A `t:boolean/0` denoting whether or not to mark the message as
      failed. Effectively defaults to `false`.

    * `"archive"` - A `t:boolean/0` denoting whether or not to override the
      default acknowledgement for the message and archive it. Effectively
      defaults to `false`. For more information about acknowledgements, see
      [Acknowledgements](`m:EctoPGMQ.Producer#acknowledgements`).
  """

  use Broadway

  alias Broadway.Message
  alias EctoPGMQ.Producer
  alias EctoPGMQ.TestRepo

  ################################
  # Types
  ################################

  @typedoc "An encoded `t:pid/0`."
  @type encoded_pid :: String.t()

  ################################
  # Public API
  ################################

  @doc """
  Starts the `Broadway` pipeline process.

  ## Options

  This function supports any of the `EctoPGMQ.Producer`
  [options](`m:EctoPGMQ.Producer#options`) but only requires the `:queue` option
  to be specified. This function also applies the following defaults:

    * `:repo` - Defaults to `EctoPGMQ.TestRepo`.

    * `:visibility_timeout` - Defaults to `300` (5 minutes).

    * `:read_interval` - Defaults to `1_000` (1 second).

    * `:listener` - Defaults to the configuration for the specified repo with
      the name `:test_pipeline_listener`.
  """
  @spec start_link :: Broadway.on_start()
  @spec start_link(keyword()) :: Broadway.on_start()
  def start_link(opts \\ []) do
    repo = Keyword.get(opts, :repo, TestRepo)
    listener = Keyword.put(repo.config(), :name, :test_pipeline_listener)

    producer_opts =
      Keyword.merge(
        [
          repo: repo,
          visibility_timeout: 300,
          read_interval: 1_000,
          listener: listener
        ],
        opts
      )

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {Producer, producer_opts},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 1]
      ]
    )
  end

  @doc """
  Returns the `t:encoded_pid/0` for the current process.
  """
  @spec get_encoded_pid :: encoded_pid()
  def get_encoded_pid do
    self()
    |> :erlang.term_to_binary()
    |> Base.encode64()
  end

  ################################
  # Broadway Callbacks
  ################################

  @doc false
  @impl Broadway
  @spec handle_message(:default, Message.t(), :context_not_set) :: Message.t()
  def handle_message(:default, message, :context_not_set) do
    status = get_status(message)

    message.data
    |> Map.fetch!("encoded_pid")
    |> Base.decode64!()
    |> :erlang.binary_to_term()
    |> send({status, message.metadata.id})

    message
    |> maybe_configure_ack()
    |> maybe_fail(status)
  end

  ################################
  # Private API
  ################################

  defp get_status(%{data: %{"fail" => true}}), do: :failure
  defp get_status(_), do: :success

  defp maybe_configure_ack(%{data: %{"archive" => true}} = message) do
    Message.configure_ack(message, ack_action: :archive)
  end

  defp maybe_configure_ack(message), do: message

  defp maybe_fail(message, :failure), do: Message.failed(message, :failure)
  defp maybe_fail(message, :success), do: message
end
