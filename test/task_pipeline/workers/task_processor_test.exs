defmodule TaskPipeline.Workers.TaskProcessorTest do
  # Enforce async: false to secure state isolation when manipulating global application environments
  use TaskPipeline.DataCase, async: false
  use Oban.Testing, repo: TaskPipeline.Repo

  alias TaskPipeline.Tasks
  alias TaskPipeline.Tasks.Task
  alias TaskPipeline.Workers.TaskProcessor

  @task_attrs %{
    "title" => "Telemetry Calculation",
    "type" => "report",
    "priority" => "normal",
    "payload" => %{"month" => "2026-06"}
  }

  describe "perform/1 success flow" do
    test "success: processes task successfully and transitions state to completed" do
      Application.delete_env(:task_pipeline, :mock_error_type)
      Application.put_env(:task_pipeline, :task_failure_threshold, 0)

      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)

      mock_job = %Oban.Job{args: %{"task_id" => task.id}, attempt: 1}
      assert :ok = TaskProcessor.perform(mock_job)

      %Task{} = updated_task = Tasks.get_task(task.id)
      assert updated_task.status == :completed

      assert length(updated_task.attempts) == 2
      assert List.last(updated_task.attempts)["message"] == "completed execution successfully"
    end
  end

  describe "perform/1 transient fault-tolerance and self-healing" do
    # Capture log tag suppresses standard error logging outputs during infrastructure failures
    @describetag :capture_log

    test "error: handles transient failure step and rolls custom database state back to queued" do
      # Force a transient network rate limit fault via dynamic application environment
      Application.put_env(:task_pipeline, :mock_error_type, :rate_limited)
      on_exit(fn -> Application.delete_env(:task_pipeline, :mock_error_type) end)

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

    test "error: marks task as failed permanently when maximum attempts are exhausted" do
      # Force a transient network timeout fault
      Application.put_env(:task_pipeline, :mock_error_type, :timeout)
      on_exit(fn -> Application.delete_env(:task_pipeline, :mock_error_type) end)

      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)
      mock_job = %Oban.Job{args: %{"task_id" => task.id}, attempt: task.max_attempts}

      assert {:error, "Max attempts reached: " <> _} = TaskProcessor.perform(mock_job)

      %Task{} = updated_task = Tasks.get_task(task.id)
      assert updated_task.status == :failed
      assert length(updated_task.attempts) == 2

      last_log_message = List.last(updated_task.attempts)["message"]
      assert String.contains?(last_log_message, "Retry limits exhausted")
    end
  end

  describe "perform/1 fatal failure and fail-fast optimization" do
    @describetag :capture_log

    test "error: terminates unrecoverable business bugs immediately without triggering retry loops" do
      # Force a fatal bad request schema mismatch vector
      Application.put_env(:task_pipeline, :mock_error_type, :bad_request)
      on_exit(fn -> Application.delete_env(:task_pipeline, :mock_error_type) end)

      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)
      mock_job = %Oban.Job{args: %{"task_id" => task.id}, attempt: 1}

      assert :ok = TaskProcessor.perform(mock_job)

      %Task{} = updated_task = Tasks.get_task(task.id)
      assert updated_task.status == :failed
      assert length(updated_task.attempts) == 2

      last_log_message = List.last(updated_task.attempts)["message"]
      assert String.contains?(last_log_message, "400 Bad Request")
    end
  end

  describe "perform/1 defense and operational protection" do
    test "idempotency: gracefully releases non-queued items by returning ok to Oban scheduler" do
      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)

      assert {:ok, processed_task} = Tasks.update_task_status(task, :processing, %{})
      assert {:ok, completed_task} = Tasks.update_task_status(processed_task, :completed, %{})

      mock_job = %Oban.Job{args: %{"task_id" => completed_task.id}, attempt: 1}
      assert :ok = TaskProcessor.perform(mock_job)
    end

    @tag :capture_log
    test "operational: handles orphaned tasks gracefully by skipping execution and dropping job safely" do
      # Generates a structurally valid UUID that is guaranteed to not exist in the DB tree
      non_existent_uuid = Ecto.UUID.generate()
      mock_job = %Oban.Job{args: %{"task_id" => non_existent_uuid}, attempt: 1}

      assert :ok = TaskProcessor.perform(mock_job)
    end
  end
end
