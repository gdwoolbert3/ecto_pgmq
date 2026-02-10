defmodule EctoPGMQ.ThrottleTest do
  use EctoPGMQ.TestCase, async: true

  alias EctoPGMQ.Throttle

  doctest Throttle
end
