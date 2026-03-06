defmodule EctoPGMQ.BindingTest do
  use EctoPGMQ.TestCase, async: true

  import Ecto.Query

  alias EctoPGMQ.Binding

  doctest Binding, import: true

  @moduletag queue_attributes: %{bindings: ["#"]}

  describe "query/0" do
    @tag queue: false
    test "will return a query for queue bindings" do
      queue_1 = EctoPGMQ.create_queue(Repo, "my_queue_1", %{bindings: ["foo.*", "bar.*"]})
      queue_2 = EctoPGMQ.create_queue(Repo, "my_queue_2", %{bindings: ["bar.*", "baz.*"]})
      bindings = Repo.all(Binding.query())

      # Validate that all queue bindings are returned by the query
      # Note that direct regex comparison will often fail so we instead compare
      # the regex sources. For more information, see
      # https://hexdocs.pm/elixir/Regex.html.
      assert [queue_1, queue_2]
             |> Enum.flat_map(fn queue -> queue.bindings end)
             |> Enum.map(fn b -> Map.update!(b, :regex, &Regex.source/1) end)
             |> same_elements?(Enum.map(bindings, fn b -> Map.update!(b, :regex, &Regex.source/1) end))
    end

    test "will allow filtering on regex field with a Regex struct" do
      # Validate that Regex struct can be used in query
      assert Binding.query()
             |> where([b], b.regex == ^~r/^.*$/)
             |> Repo.exists?()
    end

    test "will allow filtering on regex field with a Regex source" do
      # Validate that Regex source can be used in query
      assert Binding.query()
             |> where([b], b.regex == "^.*$")
             |> Repo.exists?()
    end

    test "will allow filtering based on a regex match" do
      assert Binding.query()
             |> where([b], fragment("? ~ ?", "my.routing.key", b.regex))
             |> Repo.exists?()
    end
  end
end
