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

      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

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

      message_specs = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue, message_specs)

      # Validate that a notification is NOT received
      refute_receive {:notification, _, ^subscription, ^channel, ""}
    end
  end
end
