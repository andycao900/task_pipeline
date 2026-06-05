defmodule TaskPipeline.TasksTest do
  use TaskPipeline.DataCase, async: true
  use Oban.Testing, repo: TaskPipeline.Repo

  alias TaskPipeline.Tasks
  alias TaskPipeline.Tasks.Task

  @valid_payload %{
    "title" => "Bulk Export Pipeline",
    "type" => "export",
    "priority" => "critical",
    "payload" => %{"target_format" => "parquet"}
  }

  describe "create_task/1 (Transactional Enqueueing & Traffic Staggering)" do
    test "success: inserts critical task with maximum weight and zero schedule delays" do
      assert {:ok, %{task: %Task{} = task, job: %Oban.Job{} = job}} =
               Tasks.create_task(@valid_payload)

      assert task.status == :queued
      assert task.title == "Bulk Export Pipeline"
      assert task.lock_version == 1

      # Staff Optimization: Enforce strict verification of queue placement and numeric weights
      assert_enqueued(
        worker: TaskPipeline.Workers.TaskProcessor,
        args: %{"task_id" => task.id}
      )

      assert job.priority == 3

      # Critical task should execute instantly (scheduled_at should be roughly equal to now)
      assert DateTime.diff(DateTime.utc_now(), job.scheduled_at, :second) <= 1
    end

    test "success: staggers low priority task execution by 30 seconds to protect infrastructure" do
      low_payload = Map.put(@valid_payload, "priority", "low")
      assert {:ok, %{job: %Oban.Job{} = job}} = Tasks.create_task(low_payload)

      assert job.priority == 0

      # Flaky Guard: Calculate precise future offsets to shield the test from CPU scheduling latency spikes
      diff_in_seconds = DateTime.diff(job.scheduled_at, DateTime.utc_now(), :second)
      assert diff_in_seconds >= 28 and diff_in_seconds <= 30
    end

    test "error: transactional rollback occurs when changeset is invalid" do
      invalid_payload = Map.drop(@valid_payload, ["title"])

      assert {:error, :task, %Ecto.Changeset{}, _steps} = Tasks.create_task(invalid_payload)
      refute_enqueued(worker: TaskPipeline.Workers.TaskProcessor)
    end
  end

  describe "update_task_status/3 (Optimistic Locking & Concurrency)" do
    test "success: advances status and updates lock version safely" do
      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@valid_payload)

      assert {:ok, %Task{} = updated_task} =
               Tasks.update_task_status(task, :processing, %{"event" => "test"})

      assert updated_task.status == :processing
      assert updated_task.lock_version == 2
      assert length(updated_task.attempts) == 1
    end

    test "error: catches stale concurrent mutations gracefully via optimistic locking" do
      assert {:ok, %{task: %Task{} = task}} = Tasks.create_task(@valid_payload)

      # Simulate an out-of-band concurrent update directly in the database
      {1, _} =
        Repo.update_all(
          from(t in Task, where: t.id == ^task.id),
          set: [lock_version: 2]
        )

      # Attempting to transition with stale version 1 will trigger an Ecto.StaleEntryError.
      # This is caught by our context rescue block and transformed into a clean changeset error.
      assert {:error, %Ecto.Changeset{} = changeset} = Tasks.update_task_status(task, :processing)

      assert {:status, {"Stale concurrency mutation block. Database version changed.", []}} in changeset.errors
    end
  end

  describe "list_tasks/1 & get_tasks_summary/0 (Query API Contracts)" do
    test "success: lists and filters collections with structured priority sorting hierarchy" do
      # critical
      assert {:ok, %{task: task1}} = Tasks.create_task(@valid_payload)
      assert {:ok, %{task: task2}} = Tasks.create_task(Map.put(@valid_payload, "priority", "low"))

      # Validate critical items sort first under default parameters due to our priority composite indices
      all_tasks = Tasks.list_tasks()
      assert Enum.map(all_tasks, & &1.id) == [task1.id, task2.id]

      # Validate specific isolated filtering maps
      low_tasks = Tasks.list_tasks(%{"priority" => "low"})
      assert length(low_tasks) == 1
      assert hd(low_tasks).id == task2.id
    end

    test "success: calculates real-time summary statistics maps" do
      assert {:ok, %{task: _}} = Tasks.create_task(@valid_payload)
      assert {:ok, %{task: _}} = Tasks.create_task(@valid_payload)

      summary = Tasks.get_tasks_summary()
      assert summary == %{"queued" => 2}
    end
  end
end
