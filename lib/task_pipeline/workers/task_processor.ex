defmodule TaskPipeline.Workers.TaskProcessor do
  @moduledoc """
  High-throughput Oban execution worker. Features defensive lifecycle error handling,
  priority latency simulations, and isolated random error simulation blocks.
  """
  use Oban.Worker, queue: :tasks

  alias TaskPipeline.Tasks
  alias TaskPipeline.Tasks.Task

  @type status_info :: %{required(String.t()) => any()}

  @impl Oban.Worker
  @spec perform(Oban.Job.t()) :: :ok | {:error, any()}
  def perform(%Oban.Job{args: %{"task_id" => task_id}} = job) do
    case Tasks.get_task(task_id) do
      nil ->
        {:error, "Operational failure: Task reference #{task_id} missing from relational tier."}

      %Task{} = task ->
        # Defensive Execution Guard: If another process or previous error finalized or processed this task,
        # skip it immediately to guarantee processing idempotency.
        if task.status != :queued do
          {:error, "Idempotency block: Task #{task_id} is already in a '#{task.status}' state."}
        else
          execute_pipeline(task, job)
        end
    end
  end

  # --- Internal Pipeline Segments ---
  defp execute_pipeline(%Task{} = task, %Oban.Job{} = job) do
    # 1. Promote state smoothly to processing with clear execution logs
    case Tasks.update_task_status(
           task,
           :processing,
           log_attempt("started execution processing loop")
         ) do
      {:ok, processing_task} ->
        # simulate job execution
        Process.sleep(100 + :rand.uniform(101) - 1)

        # This shifts the evaluation from compile-time to runtime, allowing dynamic testing injections.
        failure_threshold = Application.get_env(:task_pipeline, :task_failure_threshold, 20)

        # 3. Inject fault vectors based on the dynamic runtime threshold
        if :rand.uniform(100) <= failure_threshold do
          handle_failure(processing_task, job)
        else
          {:ok, _} =
            Tasks.update_task_status(
              processing_task,
              :completed,
              log_attempt("completed execution successfully")
            )

          :ok
        end

      {:error, _stale_changeset} ->
        # If the state update failed due to Optimistic Locking, abort gracefully to preserve the winner's data
        {:error, "Aborted: Task state changed concurrently by another consumer process."}
    end
  end

  defp handle_failure(%Task{} = task, %Oban.Job{} = job) do
    error_msg = "Random execution failure encountered (20% simulation gate)"
    current_attempt = job.attempt

    if current_attempt >= task.max_attempts do
      {:ok, _} =
        Tasks.update_task_status(
          task,
          :failed,
          log_attempt("terminal error: #{error_msg}. Allocation limit reached.")
        )

      {:error, error_msg}
    else
      # Re-queue task back into the structural pool for subsequent evaluation schedules
      {:ok, _} =
        Tasks.update_task_status(
          task,
          :queued,
          log_attempt(
            "transient retry: re-queued due to execution faults (Attempt #{current_attempt}/#{task.max_attempts})"
          )
        )

      {:error, error_msg}
    end
  end

  @spec log_attempt(String.t()) :: status_info()
  defp log_attempt(message) do
    %{
      "timestamp" => DateTime.utc_now() |> DateTime.to_string(),
      "message" => message
    }
  end
end
