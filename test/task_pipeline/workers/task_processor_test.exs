defmodule TaskPipeline.Workers.TaskProcessorTest do
  # Enforce async: false to secure state isolation when manipulating global application environments
  use TaskPipeline.DataCase, async: false
  use Oban.Testing, repo: TaskPipeline.Repo

  alias TaskPipeline.Tasks
  alias TaskPipeline.Tasks.Task
  alias TaskPipeline.Workers.TaskProcessor
  alias TaskPipeline.Monitoring.MetricsTracker

  @task_attrs %{
    "title" => "Telemetry Calculation",
    "type" => "report",
    "priority" => "normal",
    "payload" => %{"month" => "2026-06"}
  }

  setup do
    # Clear out structural mock interceptors to guarantee clean initial environments
    Application.delete_env(:task_pipeline, :mock_error_type)
    Application.put_env(:task_pipeline, :task_failure_threshold, 0)

    # 💡 Senior Resilience Guard: Check if the global tracker is alive.
    # If missing (due to application topology in test), boot a sandboxed instance dynamically.
    case GenServer.whereis(MetricsTracker) do
      nil ->
        # Boot a sandboxed telemetry tracker registered locally under the global name for test continuity
        start_supervised!({MetricsTracker, [name: MetricsTracker]})

      pid when is_pid(pid) ->
        # If already globally active, gracefully flush its internal memory matrix state
        MetricsTracker.reset_counters()
        # Barrier synchronization to block test frame until the async cast completes safely
        MetricsTracker.get_stats()
    end

    :ok
  end

  describe "perform/1 success flow" do
    test "success: processes task successfully, transitions state to completed, and dispatches metrics" do
      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)

      mock_job = %Oban.Job{args: %{"task_id" => task.id}, attempt: 1}
      assert :ok = TaskProcessor.perform(mock_job)

      %Task{} = updated_task = Tasks.get_task(task.id)
      assert updated_task.status == :completed

      assert length(updated_task.attempts) == 2
      assert List.last(updated_task.attempts)["message"] == "completed execution successfully"

      # Verify metrics telemetry pipeline aggregates the success non-blockingly
      stats = MetricsTracker.get_stats()
      assert stats.processed_count == 1
      assert stats.failure_count == 0
    end
  end

  describe "perform/1 transient fault-tolerance and self-healing" do
    # Capture log tag suppresses standard error logging outputs during infrastructure failures
    @describetag :capture_log

    test "error: handles transient failure step and rolls custom database state back to queued" do
      # Force a transient network rate limit fault via dynamic application environment
      Application.put_env(:task_pipeline, :mock_error_type, :rate_limited)

      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)
      mock_job = %Oban.Job{args: %{"task_id" => task.id}, attempt: 1}

      assert {:error, "Transient failure: " <> _} = TaskProcessor.perform(mock_job)

      %Task{} = updated_task = Tasks.get_task(task.id)
      assert updated_task.status == :queued
      assert length(updated_task.attempts) == 2

      # Assert against the unified message stream to verify the semantic error trace safely
      last_log_message = List.last(updated_task.attempts)["message"]
      assert String.contains?(last_log_message, "429 Rate Limited")
    end

    test "error: marks task as failed permanently and logs system failure when maximum attempts are exhausted" do
      # Force a transient network timeout fault
      Application.put_env(:task_pipeline, :mock_error_type, :timeout)

      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)
      mock_job = %Oban.Job{args: %{"task_id" => task.id}, attempt: task.max_attempts}

      assert {:error, "Max attempts reached: " <> _} = TaskProcessor.perform(mock_job)

      %Task{} = updated_task = Tasks.get_task(task.id)
      assert updated_task.status == :failed
      assert length(updated_task.attempts) == 2

      last_log_message = List.last(updated_task.attempts)["message"]
      assert String.contains?(last_log_message, "Retry limits exhausted")

      # Terminal retries must increment failure metrics inside the tracker
      stats = MetricsTracker.get_stats()
      assert stats.failure_count == 1
      assert stats.processed_count == 0
    end
  end

  describe "perform/1 fatal failure and fail-fast optimization" do
    @describetag :capture_log

    test "error: terminates unrecoverable business bugs immediately, bypasses retry loops, and logs metrics" do
      # Force a fatal bad request schema mismatch vector
      Application.put_env(:task_pipeline, :mock_error_type, :bad_request)

      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)
      mock_job = %Oban.Job{args: %{"task_id" => task.id}, attempt: 1}

      assert :ok = TaskProcessor.perform(mock_job)

      %Task{} = updated_task = Tasks.get_task(task.id)
      assert updated_task.status == :failed
      assert length(updated_task.attempts) == 2

      last_log_message = List.last(updated_task.attempts)["message"]
      assert String.contains?(last_log_message, "400 Bad Request")

      # Fail-fast unrecoverable blocks must increment system failure counters asynchronously
      stats = MetricsTracker.get_stats()
      assert stats.failure_count == 1
      assert stats.processed_count == 0
    end
  end

  describe "perform/1 defense and operational protection" do
    test "idempotency: gracefully releases non-queued items by returning ok to Oban scheduler" do
      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)

      assert {:ok, processed_task} = Tasks.update_task_status(task, :processing, %{})
      assert {:ok, completed_task} = Tasks.update_task_status(processed_task, :completed, %{})

      mock_job = %Oban.Job{args: %{"task_id" => completed_task.id}, attempt: 1}
      assert :ok = TaskProcessor.perform(mock_job)

      # Ensure no duplicate telemetry states populate the tracker on idempotency blocks
      stats = MetricsTracker.get_stats()
      assert stats.processed_count == 0
    end

    @tag :capture_log
    test "operational: handles orphaned tasks gracefully by skipping execution and dropping job safely" do
      # Generates a structurally valid UUID that is guaranteed to not exist in the DB tree
      non_existent_uuid = Ecto.UUID.generate()
      mock_job = %Oban.Job{args: %{"task_id" => non_existent_uuid}, attempt: 1}

      assert :ok = TaskProcessor.perform(mock_job)
    end
  end

  describe "Supervision Tree — Fault Tolerance Boundary Isolation" do
    @describetag :capture_log

    test "blast radius: crashing the telemetry tracking core preserves worker core database transaction capabilities" do
      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)
      mock_job = %Oban.Job{args: %{"task_id" => task.id}, attempt: 1}

      # Intentionally terminate the active telemetry process to mimic mailbox saturation or extreme crashes
      pid = GenServer.whereis(MetricsTracker)
      assert is_pid(pid)

      Process.exit(pid, :kill)

      # Yield execution context fractionally to let the Sub-Supervisor handle dynamic reactivation
      Process.sleep(10)

      # Verify process identifier healing took place under the supervision tree
      new_pid = GenServer.whereis(MetricsTracker)
      assert is_pid(new_pid)
      assert pid != new_pid

      # The task processing framework must run flawlessly without throwing cascading faults to the consumer thread
      assert :ok == TaskProcessor.perform(mock_job)

      # 💡 修正点：将 get_task! 改为 get_task
      updated_task = Tasks.get_task(task.id)
      assert updated_task.status == :completed
    end
  end
end
