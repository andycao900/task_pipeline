defmodule TaskPipeline.Monitoring.MetricsTrackerTest do
  # Secure full multi-threaded performance by decoupling name dependencies completely
  use TaskPipeline.DataCase, async: true

  alias TaskPipeline.Monitoring.MetricsTracker

  setup do
    # Boot a dedicated instance under the test supervisor explicitly suppressing global name registry
    pid = start_supervised!({MetricsTracker, []})

    # Pass the unique process identifier straight into the test context frame
    {:ok, tracker: pid}
  end

  describe "MetricsTracker Core State Mutations" do
    test "initializes with a clean, empty zero-filled matrix state", %{tracker: tracker} do
      stats = GenServer.call(tracker, :get_stats)

      assert stats == %{
               processed_count: 0,
               failure_count: 0,
               average_duration_ms: 0.0
             }
    end

    test "log_success/1 increments processed counters and accurately aggregates average latency",
         %{tracker: tracker} do
      # Dispatch multiple asynchronous tracking casts straight to our sandboxed PID
      GenServer.cast(tracker, {:log_success, 100})
      GenServer.cast(tracker, {:log_success, 200})
      GenServer.cast(tracker, {:log_success, 300})

      # Request a synchronous snapshot capture
      stats = GenServer.call(tracker, :get_stats)

      assert stats.processed_count == 3
      assert stats.failure_count == 0
      assert stats.average_duration_ms == 200.0
    end

    test "log_failure/0 increments unrecoverable boundary system counters", %{tracker: tracker} do
      GenServer.cast(tracker, :log_failure)
      GenServer.cast(tracker, :log_failure)

      stats = GenServer.call(tracker, :get_stats)

      assert stats.processed_count == 0
      assert stats.failure_count == 2
      assert stats.average_duration_ms == 0.0
    end
  end

  describe "MetricsTracker Edge Cases & Mathematical Stability" do
    test "deflects divide-by-zero errors gracefully when metrics are polled before any success logs exist",
         %{tracker: tracker} do
      # Hit the process exclusively with failure counters to keep processed_count at 0
      GenServer.cast(tracker, :log_failure)

      stats = GenServer.call(tracker, :get_stats)

      # The internal callback must safely yield 0.0 instead of throwing an ArithmeticError crash
      assert stats.processed_count == 0
      assert stats.failure_count == 1
      assert stats.average_duration_ms == 0.0
    end
  end
end
