defmodule TaskPipeline.Workers.TaskProcessorTest do
  use TaskPipeline.DataCase, async: true

  alias TaskPipeline.Tasks
  alias TaskPipeline.Tasks.Task
  alias TaskPipeline.Workers.TaskProcessor

  @task_attrs %{
    "title" => "Telemetry Calculation",
    "type" => "report",
    "priority" => "normal",
    "payload" => %{"month" => "2026-06"}
  }

  describe "perform/1" do
    test "success: processes task successfully and transitions state to completed" do
      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)

      # 💡 Senior Control: Force failure threshold to 0% to guarantee success path
      Application.put_env(:task_pipeline, :task_failure_threshold, 0)

      mock_job = %Oban.Job{args: %{"task_id" => task.id}, attempt: 1}
      assert :ok = TaskProcessor.perform(mock_job)

      %Task{} = updated_task = Tasks.get_task(task.id)
      assert updated_task.status == :completed
      assert length(updated_task.attempts) == 2
    end

    test "error: handles failure step and drops task back to queued when retries remain" do
      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)

      # 💡 Senior Control: Force failure threshold to 100% to guarantee failure path
      Application.put_env(:task_pipeline, :task_failure_threshold, 100)

      mock_job = %Oban.Job{args: %{"task_id" => task.id}, attempt: 1}

      assert {:error, "Random execution failure encountered (20% simulation gate)"} =
               TaskProcessor.perform(mock_job)

      %Task{} = updated_task = Tasks.get_task(task.id)
      assert updated_task.status == :queued
      assert length(updated_task.attempts) == 2
    end

    test "error: marks task as failed permanently when maximum attempts are exhausted" do
      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)

      # 💡 Senior Control: Force failure threshold to 100% to guarantee failure path
      Application.put_env(:task_pipeline, :task_failure_threshold, 100)

      mock_job = %Oban.Job{args: %{"task_id" => task.id}, attempt: task.max_attempts}

      assert {:error, "Random execution failure encountered (20% simulation gate)"} =
               TaskProcessor.perform(mock_job)

      %Task{} = updated_task = Tasks.get_task(task.id)
      assert updated_task.status == :failed
      assert length(updated_task.attempts) == 2
    end

    test "error: executes pre-flight gating to enforce idempotency when task is not queued" do
      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@task_attrs)

      assert {:ok, %Task{} = processed_task} = Tasks.update_task_status(task, :processing, %{})
      assert {:ok, _} = Tasks.update_task_status(processed_task, :completed, %{})

      mock_job = %Oban.Job{args: %{"task_id" => task.id}, attempt: 1}
      assert {:error, "Idempotency block: Task " <> _} = TaskProcessor.perform(mock_job)
    end
  end
end
