defmodule TaskPipeline.Task.TaskTest do
  use TaskPipeline.DataCase, async: true
  alias TaskPipeline.Tasks.Task

  @valid_attrs %{
    title: "Data Ingestion Task",
    type: "import",
    priority: "high",
    payload: %{"source" => "s3://bucket/data.csv"}
  }

  describe "changeset/2 (Ingestion Validations)" do
    test "success: returns valid changeset with complete attributes" do
      changeset = Task.changeset(%Task{}, @valid_attrs)
      assert changeset.valid?
      assert get_field(changeset, :status) == :queued
    end

    test "error: invalid when missing required fields" do
      invalid_attrs = %{title: nil, type: nil, payload: nil}
      changeset = Task.changeset(%Task{}, invalid_attrs)

      refute changeset.valid?
      assert keyword_has_error?(changeset.errors, :title, "can't be blank")
      assert keyword_has_error?(changeset.errors, :type, "can't be blank")
      assert keyword_has_error?(changeset.errors, :payload, "can't be blank")
    end

    test "error: invalid when max_attempts is zero or negative" do
      invalid_attrs = Map.put(@valid_attrs, :max_attempts, 0)
      changeset = Task.changeset(%Task{}, invalid_attrs)
      refute changeset.valid?
      assert keyword_has_error?(changeset.errors, :max_attempts, "must be greater than %{number}")
    end
  end

  describe "status_changeset/3 (State Machine Transitions)" do
    setup do
      # Mock a persisted/existing struct in the :queued state
      {:ok, task: %Task{status: :queued, attempts: []}}
    end

    test "success: transitions from queued to processing and logs attempts", %{task: task} do
      attempt_log = %{
        "timestamp" => DateTime.utc_now() |> DateTime.to_string(),
        "status" => "started"
      }

      changeset = Task.status_changeset(task, :processing, attempt_log)

      assert changeset.valid?
      assert get_change(changeset, :status) == :processing
      assert get_change(changeset, :attempts) == [attempt_log]
    end

    test "success: allows valid terminal jumps from processing", %{task: %Task{} = task} do
      # Simulate a task currently processing
      processing_task = %Task{task | status: :processing}

      assert Task.status_changeset(processing_task, :completed).valid?
      assert Task.status_changeset(processing_task, :failed).valid?
      assert Task.status_changeset(processing_task, :queued).valid?
    end

    test "error: blocks illegal state jumps (e.g., queued directly to completed)", %{task: task} do
      changeset = Task.status_changeset(task, :completed)
      refute changeset.valid?

      assert keyword_has_error?(
               changeset.errors,
               :status,
               "Invalid status transition from queued to completed"
             )
    end
  end

  # Helper helper to simplify error tuple keyword list checking
  defp keyword_has_error?(errors, field, expected_msg) do
    Enum.any?(errors, fn {f, {msg, _}} -> f == field and msg == expected_msg end)
  end
end
