defmodule EctoPGMQ.TestHelpers do
  @moduledoc """
  Helper functions for `EctoPGMQ` unit tests.
  """

  import Ecto.Query

  alias Ecto.Repo
  alias EctoPGMQ.Message
  alias EctoPGMQ.PGMQ
  alias EctoPGMQ.Queue

  ################################
  # EctoPGMQ Helpers
  ################################

  @doc """
  Returns all messages in the archive of the given queue ordered by ID.
  """
  @spec all_archive_messages(Repo.t(), Queue.name()) :: [Message.t()]
  def all_archive_messages(repo, queue) do
    queue
    |> Message.archive_query()
    |> order_by(:id)
    |> repo.all()
  end

  @doc """
  Returns all messages in the given queue ordered by ID.
  """
  @spec all_queue_messages(Repo.t(), Queue.name()) :: [Message.t()]
  @spec all_queue_messages(Repo.t(), Queue.name(), Message.payload_type()) :: [Message.t()]
  def all_queue_messages(repo, queue, payload_type \\ :map) do
    queue
    |> Message.queue_query(payload_type: payload_type)
    |> order_by(:id)
    |> repo.all()
  end

  @doc """
  Returns whether or not the archive for the given queue is empty.
  """
  @spec archive_empty?(Repo.t(), Queue.name()) :: boolean()
  def archive_empty?(repo, queue) do
    queue
    |> Message.archive_query()
    |> empty?(repo)
  end

  @doc """
  Returns whether or not the given queue has a FIFO message group index.
  """
  @spec fifo_index?(Repo.t(), Queue.name()) :: boolean()
  def fifo_index?(repo, queue) do
    queue_table = PGMQ.queue_table_name(queue)

    "pg_indexes"
    |> put_query_prefix("pg_catalog")
    |> where([i], i.schemaname == ^PGMQ.schema())
    |> where([i], i.tablename == ^queue_table)
    |> where([i], i.indexname == ^"#{queue_table}_fifo_idx")
    |> repo.exists?()
  end

  @doc """
  Returns whether or not the given queue is empty.
  """
  @spec queue_empty?(Repo.t(), Queue.name()) :: boolean()
  def queue_empty?(repo, queue) do
    queue
    |> Message.queue_query()
    |> empty?(repo)
  end

  @doc """
  Returns whether or not the given sent messages were read with the given
  visibility timeout.

  This function assumes that the given sent messages and the given read messages
  are in the same order.
  """
  @spec read_messages?([Message.t()], [Message.t()], PGMQ.visibility_timeout(), PGMQ.quantity()) :: boolean()
  def read_messages?(sent, read, vt, q) when length(sent) == q and length(read) == q do
    sent
    |> Enum.zip(read)
    |> Enum.all?(fn
      {%Message{id: id} = old, %Message{id: id} = new} ->
        DateTime.diff(new.visible_at, old.visible_at) == vt and old.reads + 1 == new.reads

      _ ->
        false
    end)
  end

  def read_messages?(_, _, _, _), do: false

  @doc """
  Returns whether or not the given messages are aligned with the given message
  specifications and IDs.

  This function assumes that the given messages, message IDs and message
  specifications are all in the same order.
  """
  @spec same_messages?([Message.t()], [Message.id()], [Message.specification()]) :: boolean()
  def same_messages?(messages, ids, _) when length(messages) != length(ids), do: false
  def same_messages?(messages, _, specs) when length(messages) != length(specs), do: false

  def same_messages?(messages, ids, specs) do
    [ids, specs, messages]
    |> Enum.zip()
    |> Enum.all?(fn {id, {:spec, payload, group, headers}, message} ->
      match?(
        %Message{
          id: ^id,
          payload: ^payload,
          headers: ^headers,
          group: ^group
        },
        message
      )
    end)
  end

  @doc """
  Returns whether or not the given updated messages were updated with the given
  visibility timeout.

  This function assumes that the given sent messages and the given updated
  messages are in the same order.
  """
  @spec updated_messages?([Message.t()], [Message.t()], PGMQ.visibility_timeout(), PGMQ.quantity()) :: boolean()
  def updated_messages?(updated, sent, vt, q) when length(sent) == q and length(updated) == q do
    sent
    |> Enum.zip(updated)
    |> Enum.all?(fn
      {%Message{id: id, reads: reads} = old, %Message{id: id, reads: reads} = new} ->
        DateTime.diff(new.visible_at, old.visible_at) == vt

      _ ->
        false
    end)
  end

  def updated_messages?(_, _, _, _), do: false

  ################################
  # Generic Helpers
  ################################

  @doc """
  Returns whether or not the given enumerables contain the same elements.
  """
  @spec same_elements?(Enumerable.t(), Enumerable.t()) :: boolean()
  def same_elements?(enum_1, enum_2) do
    Enum.sort(enum_1) == Enum.sort(enum_2)
  end

  ################################
  # Private API
  ################################

  defp empty?(query, repo), do: not repo.exists?(query)
end
