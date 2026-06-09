defmodule TaskPipeline.Workers.TaskProcessor do
  @moduledoc """
  High-throughput Oban execution worker featuring a tri-tier idempotency shield,
  domain-driven fault taxonomy classification, and real-time telemetry metrics aggregation.

  ## Supervision & Fault Tolerance Topology

  To enforce strict production availability SLAs under a 10,000 tasks/min throughput background,
  this module operates alongside a dedicated diagnostic sub-supervision tree:

      [TaskPipeline.Supervisor] (Strategy: :one_for_one)
                 │
                 ├── [TaskPipeline.Monitoring.Supervisor] (Strategy: :one_for_one)
                 │         │
                 │         └── [TaskPipeline.Monitoring.MetricsTracker] (GenServer)
                 │
                 └── [Oban] (Background Job Engine)

  ### Telemetry Isolation and State Recovery
  1. **Blast Radius Control**: All runtime tracking states are sequestered inside the `MetricsTracker`
     GenServer, which sits under the isolated `Monitoring.Supervisor`. If a severe mailbox overflow
     or telemetry anomaly crashes the tracker, its fault boundary is completely contained.
  2. **Non-Blocking Telemetry Ingestion**: Workers update performance metrics asynchronously via
     one-way `GenServer.cast/2` signals (`log_success` and `log_failure`). This ensures that diagnostic
     crashes or latency spikes never throttle or cascade into active core database transactions.
  3. **In-Flight Task Recovery (OOM/Crash Mitigation)**: If a worker node encounters a sudden
     hardware OOM or connection severance mid-execution, Oban's localized heartbeat timeout leases
     will expire. The job is automatically re-queued. Upon picking up the duplicate token, the subsequent
     consumer thread relies on the relational `lock_version` to safely deflect out-of-order writes.

  ## Status Lifecycle Strategy
  queued → processing → completed
  ↓
  (on failure)
  ↓
  queued (retry) ← re-enqueued if attempts remain
  ↓
  failed ← when max_attempts exhausted
  Design Node Matrix:
  Node 3 (Optimistic Locking Shield),
  Node 5 (Domain-Driven Fault Taxonomy),
  Node 6 (Jittered Backoff),
  Node 7 & 12 (Tri-Tier Idempotency Shield).

  [ High-Throughput Task Stream (10,000 tasks/min) ]
                                       │
                                       ▼
  ┌───────────────────────────────────────────────────────────────────────────┐
  │  Tier 1: Scheduler Ingestion Gate (Oban Unique Engine)                    │
  │  - Filters redundant payloads via transactional unique indexes in PG.     │
  └───────────────────────────────────┬───────────────────────────────────────┘
                                     │ (Passes Unique Token Check)
                                     ▼
  ┌───────────────────────────────────────────────────────────────────────────┐
  │  Tier 2: Worker Pre-flight Validation Guard                               │
  │  - Asserts domain status strictly matches :queued / :processing.          │
  └───────────────────────────────────┬───────────────────────────────────────┘
                                     │ (Passes State Validation)
                                     ▼
  ┌───────────────────────────────────────────────────────────────────────────┐
  │  Tier 3: Relational Optimistic Locking Shield                             │
  │  - Atomic DB Verification: UPDATE tasks SET ... WHERE lock_version = X    │
  └───────────────────────────────────┬───────────────────────────────────────┘
                                     │
               ┌─────────────────────┴─────────────────────┐
               ▼ (Success: Lock Version Matched)           ▼ (Conflict Detected)
  ┌───────────────────────────────────────────┐ ┌───────────────────────────┐
  │          Execute Task Payload             │ │  Atomic Invariant Failure │
  ├───────────────────────────────────────────┤ ├───────────────────────────┤
  │ - Simulates dynamic delay (Jitter Timer)  │ │ - Stale write deflected   │
  │ - Appends execution logs into JSONB       │ │ - Returns safe :ok tuple  │
  └───────────────────────────────────────────┘ └───────────────────────────┘
  """
  use Oban.Worker,
    queue: :tasks,
    max_attempts: 5,
    # Distributed Ingestion Guard: Deduplicates high-frequency client retry loops at the database index tier
    unique: [
      period: 60,
      fields: [:args, :queue],
      states: :incomplete
    ]

  require Logger

  alias TaskPipeline.Tasks
  alias TaskPipeline.Tasks.Task

  @type status_info :: %{required(String.t()) => any()}
  @type business_error_types :: :rate_limited | :timeout | :bad_request | :payload_too_large

  @impl Oban.Worker
  @spec perform(Oban.Job.t()) :: :ok | {:error, any()}
  def perform(%Oban.Job{args: %{"task_id" => task_id}} = job) do
    # Capture monotonic clock at the absolute threshold entry point to track genuine latency pipelines
    start_time = System.monotonic_time(:millisecond)

    case Tasks.get_task(task_id) do
      nil ->
        # Task record is fundamentally missing from the database.
        # Return :ok to let Oban mark the job as completed, avoiding useless retries.
        Logger.warning(
          "Task record missing from database. Aborting Oban job gracefully. task_id=#{task_id}"
        )

        :ok

      %Task{} = task ->
        # Idempotency Guard
        if task.status not in [:queued, :processing] do
          Logger.debug(
            "Idempotency block: Task already handled by another worker. task_id=#{task.id} current_status=#{task.status}"
          )

          :ok
        else
          execute_pipeline(task, job, start_time)
        end
    end
  end

  # --- Configurable Exponential Backoff with Jitter ---

  @doc """
  Calculates the retry delay using exponential backoff supplemented with a randomized jitter.
  Prevents thundering herd problems when thousands of jobs retry simultaneously.

  Calculation formula: (attempt^4) + 2 + (random_jitter_up_to_30_seconds)
  """
  @impl Oban.Worker
  @spec backoff(Oban.Job.t()) :: integer()
  def backoff(%Oban.Job{attempt: attempt}) do
    # Base exponential growth factor: 1st retry = 1s, 2nd = 16s, 3rd = 81s...
    exponential_base = :math.pow(attempt, 4) |> trunc()

    # Inject a randomized jitter up to 30 seconds to break synchronicity across workers
    jitter = :rand.uniform(30)

    # Secure a structural minimum padding of 2 seconds
    exponential_base + 2 + jitter
  end

  # --- Internal Pipeline Segments ---

  @spec execute_pipeline(Task.t(), Oban.Job.t(), integer()) :: :ok | {:error, String.t()}
  defp execute_pipeline(%Task{} = task, %Oban.Job{} = job, start_time) do
    Logger.debug("Transitioning task to processing. task_id=#{task.id}")

    case Tasks.update_task_status(
           task,
           :processing,
           log_attempt("started execution processing loop")
         ) do
      {:ok, processing_task} ->
        simulate_priority_processing(processing_task.priority)

        # Evaluate execution logic and route through explicit semantic error matchers
        case run_business_logic(processing_task) do
          :ok ->
            finalize_success(processing_task, start_time)

          # Transient Errors (Rate limiting / Network hiccups) -> Safe to retry
          {:error, :rate_limited} ->
            handle_transient_failure(
              processing_task,
              "HTTP 429 Rate Limited - Downstream cluster congestion",
              job
            )

          # Transient Errors (Timeouts) -> Safe to retry
          {:error, :timeout} ->
            handle_transient_failure(
              processing_task,
              "HTTP 504 Gateway Timeout - Network packet loss across upstream edge",
              job
            )

          # Fatal Errors (Data corruption / Client breach) -> Do NOT retry
          {:error, :bad_request} ->
            handle_fatal_failure(
              processing_task,
              "HTTP 400 Bad Request - Schema mutation violates static constraints"
            )

          # Fatal Errors (Payload Overflow) -> Do NOT retry
          {:error, :payload_too_large} ->
            handle_fatal_failure(
              processing_task,
              "HTTP 413 Payload Too Large - Binary payload overflows allocated relational memory boundaries"
            )
        end

      {:error, _stale_changeset} ->
        # Concurrency collision captured via optimistic locking
        Logger.warning(
          "Optimistic locking collision hit during processing transition. task_id=#{task.id}"
        )

        :ok
    end
  end

  # --- Failure Filter and Alignment Actions ---

  @spec handle_transient_failure(Task.t(), String.t(), Oban.Job.t()) :: {:error, String.t()}
  defp handle_transient_failure(%Task{} = task, reason, %Oban.Job{} = job) do
    current_attempt = job.attempt

    if current_attempt >= task.max_attempts or length(task.attempts || []) >= task.max_attempts do
      Logger.error(
        "Transient error exhausted max retry limits. Moving to terminal failed state. task_id=#{task.id} reason=#{reason}"
      )

      # Asynchronously increment systemic failure counters upon hitting unrecoverable processing boundaries
      TaskPipeline.Monitoring.MetricsTracker.log_failure()

      {:ok, _} =
        Tasks.update_task_status(
          task,
          :failed,
          log_attempt("terminal error: #{reason}. Retry limits exhausted.")
        )

      {:error, "Max attempts reached: #{reason}"}
    else
      Logger.warning(
        "Transient error encountered. Re-queueing task state to queued. task_id=#{task.id} attempt=#{current_attempt}"
      )

      # Align database state to :queued before releasing to Oban's retry scheduler
      {:ok, _} =
        Tasks.update_task_status(
          task,
          :queued,
          log_attempt(
            "transient retry: re-queued due to: #{reason} (Attempt #{current_attempt}/#{task.max_attempts})"
          )
        )

      # Return error tuple to instruct Oban to transition oban_jobs.state to 'retryable'
      {:error, "Transient failure: #{reason}"}
    end
  end

  @spec handle_fatal_failure(Task.t(), String.t()) :: :ok
  defp handle_fatal_failure(%Task{} = task, reason) do
    Logger.error(
      "Fatal business error encountered. Terminating execution immediately. task_id=#{task.id} reason=#{reason}"
    )

    # Asynchronously increment systemic failure counters upon hitting unrecoverable processing boundaries
    TaskPipeline.Monitoring.MetricsTracker.log_failure()

    {:ok, _} =
      Tasks.update_task_status(
        task,
        :failed,
        log_attempt("fatal structural error: #{reason}. Execution terminated.")
      )

    # Return :ok to Oban because retry attempts are useless for fatal logic mismatches
    :ok
  end

  @spec finalize_success(Task.t(), integer()) :: :ok
  defp finalize_success(%Task{} = task, start_time) do
    Logger.debug("Task executed successfully. task_id=#{task.id}")

    # Calculate execution latency using monotonic time to guard against system clock adjustments
    duration_ms = System.monotonic_time(:millisecond) - start_time

    # Asynchronously dispatch metrics tracking to keep the core execution thread non-blocking
    TaskPipeline.Monitoring.MetricsTracker.log_success(duration_ms)

    {:ok, _} =
      Tasks.update_task_status(
        task,
        :completed,
        log_attempt("completed execution successfully")
      )

    :ok
  end

  # --- Advanced Error Matrix Simulation ---

  @spec run_business_logic(Task.t()) :: :ok | {:error, business_error_types()}
  defp run_business_logic(_task) do
    # Shift from pure randomness to test-injected configuration flags to ensure 100% testability
    case Application.get_env(:task_pipeline, :mock_error_type) do
      :rate_limited ->
        {:error, :rate_limited}

      :timeout ->
        {:error, :timeout}

      :bad_request ->
        {:error, :bad_request}

      :payload_too_large ->
        {:error, :payload_too_large}

      nil ->
        # Fallback to standard random simulation mode for dev/prod environments
        failure_threshold = Application.get_env(:task_pipeline, :task_failure_threshold, 20)

        if :rand.uniform(100) <= failure_threshold do
          case :rand.uniform(4) do
            1 -> {:error, :rate_limited}
            2 -> {:error, :timeout}
            3 -> {:error, :bad_request}
            4 -> {:error, :payload_too_large}
          end
        else
          :ok
        end
    end
  end

  # --- Configurable Dynamic Staggering Engine ---

  # Simulates processing durations by pulling jitter limits from application environment.
  # Default fallback baseline is 1000ms if parameters are missing.

  @spec simulate_priority_processing(atom() | String.t()) :: :ok
  defp simulate_priority_processing(priority) do
    key = try_atomize(priority)
    config = Application.get_env(:task_pipeline, :priority_durations, [])

    case Keyword.get(config, key) do
      [base: base, rand: rand] ->
        Process.sleep(base + :rand.uniform(rand) - 1)

      nil ->
        Logger.warning(
          "Missing priority sleep configuration for key=#{key}. Using default fallback timeline."
        )

        Process.sleep(1000)
    end
  end

  @spec try_atomize(atom() | String.t()) :: atom()
  defp try_atomize(priority) when is_atom(priority), do: priority

  defp try_atomize(priority) when is_binary(priority) do
    try do
      String.to_existing_atom(priority)
    rescue
      ArgumentError -> :normal
    end
  end

  @spec log_attempt(String.t()) :: status_info()
  defp log_attempt(message) do
    %{
      "timestamp" => NaiveDateTime.utc_now() |> NaiveDateTime.to_iso8601(),
      "message" => message
    }
  end
end
