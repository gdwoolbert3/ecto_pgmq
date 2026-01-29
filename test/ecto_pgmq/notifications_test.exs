defmodule EctoPGMQ.NotificationsTest do
  use EctoPGMQ.TestCase

  alias EctoPGMQ.Notifications

  @moduletag default_queue_attributes: %{notifications: 250}

  setup_all do
    listener = EctoPGMQ.Listener

    # Start a shared listener process for tests in this module
    start_supervised!(%{
      id: listener,
      start: {Notifications, :start_link, [[{:name, listener} | Repo.config()]]}
    })

    %{listener: listener}
  end

  describe "subscribe/3" do
    test "will subscribe to queue insert notifications", ctx do
      # Validate that subscription was successful
      assert {:ok, subscription, channel} = Notifications.subscribe(ctx.listener, ctx.queue)

      payloads = [%{"id" => 1}, %{"id" => 2}, %{"id" => 3}]
      EctoPGMQ.send_messages(Repo, ctx.queue, payloads)

      # Validate that a notification is received
      assert_receive {:notification, _, ^subscription, ^channel, ""}
    end
  end

  describe "unsubscribe/3" do
    test "will unsubscribe from queue insert notifications", ctx do
      # Validate that subscription was successful
      assert {:ok, subscription, channel} = Notifications.subscribe(ctx.listener, ctx.queue)

      # Validate that unsubscription was successful
      assert Notifications.unsubscribe(ctx.listener, subscription) == :ok

      payloads = [%{"id" => 1}, %{"id" => 2}, %{"id" => 3}]
      EctoPGMQ.send_messages(Repo, ctx.queue, payloads)

      # Validate that a notification is NOT received
      refute_receive {:notification, _, ^subscription, ^channel, ""}
    end
  end
end
