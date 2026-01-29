# Start the test repo
{:ok, _} = EctoPGMQ.TestRepo.start_link()

# Configure default message timeout
ExUnit.configure(assert_receive_timeout: 5_000)

# Start the test application
ExUnit.start()
