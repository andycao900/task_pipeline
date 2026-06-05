defmodule TaskPipeline.Tasks do
  @moduledoc """
  The unified transactional entrypoint for the Tasks context boundary. Handles concurrent query scaling,
  real-time state metrics aggregations, and resilient distributed state modifications.
  """
  import Ecto.Query, warn: false
  alias Ecto.Multi
  alias TaskPipeline.Repo
  alias TaskPipeline.Tasks.Task
  alias TaskPipeline.Workers.TaskProcessor

  @type task_attrs :: %{required(String.t() | Atom.t()) => any()}
  @type attempt_log :: %{required(String.t()) => any()}

  @doc """
  Lists tasks with composable filtering scopes and an explicit, performance-tuned sorting hierarchy.
  Leverages composite database indexing to satisfy aggressive SLA read deadlines under heavy loads.
  """
  @spec list_tasks(map()) :: [Task.t()]
  def list_tasks(filters \\ %{}) do
    Task
    |> scope_by_status(filters["status"])
    |> scope_by_type(filters["type"])
    |> scope_by_priority(filters["priority"])
    # System Constraint: Priority sorted descending (critical first), followed by newest creation timelines.
    |> order_by([t], desc: t.priority, desc: t.inserted_at)
    |> Repo.all()
  end

  @doc """
  Fetches an isolated task instance by its structural Binary ID. Returns nil if missing.
  """
  @spec get_task(binary()) :: Task.t() | nil
  def get_task(id), do: Repo.get(Task, id)

  @doc """
  Orchestrates dual-system data persistence. Wraps domain record creation and background queue
  provisioning inside an atomic Ecto.Multi transaction to guarantee structural isolation.
  """
  @spec create_task(task_attrs()) :: {:ok, %{task: Task.t(), job: Oban.Job.t()}} | {:error, any()}
  def create_task(attrs) do
    Multi.new()
    |> Multi.insert(:task, Task.changeset(%Task{}, attrs))
    |> Multi.insert(:job, fn %{task: task} ->
      # 💡 Senior Implementation: Calculate the delayed execution timeline based on business urgency
      scheduled_time = calculate_scheduled_at(task.priority)

      TaskProcessor.new(
        %{"task_id" => task.id},
        priority: transform_priority(task.priority),
        scheduled_at: scheduled_time
      )
    end)
    |> Repo.transaction()
  end

  @doc """
  Updates a task lifecycle status with defensive protection against distributed race conditions.
  Uses Ecto's native Optimistic Locking engine to catch out-of-order execution attempts gracefully.
  """
  @spec update_task_status(Task.t(), Task.status(), attempt_log() | nil) ::
          {:ok, Task.t()} | {:error, Ecto.Changeset.t() | :stale_entry}
  def update_task_status(%Task{} = task, new_status, additional_log \\ nil) do
    task
    |> Task.status_changeset(new_status, additional_log)
    |> Repo.update()
  rescue
    # Intercept race conditions gracefully when another process has advanced the row version concurrently
    Ecto.StaleEntryError ->
      stale_changeset =
        task
        |> Task.status_changeset(new_status)
        |> Ecto.Changeset.add_error(
          :status,
          "Stale concurrency mutation block. Database version changed."
        )

      {:error, stale_changeset}
  end

  @doc """
  Aggregates real-time state metrics counts broken down by state machine status fields.
  Optimized via database group-by mechanics to mitigate unbounded memory allocation vectors.
  """
  @spec get_tasks_summary() :: map()
  def get_tasks_summary do
    from(t in Task, group_by: t.status, select: {t.status, count(t.id)})
    |> Repo.all()
    |> Map.new(fn {status, count} -> {to_string(status), count} end)
  end

  # --- Composable Internal Scopes ---
  defp scope_by_status(query, nil), do: query
  defp scope_by_status(query, status), do: where(query, [t], t.status == ^status)

  defp scope_by_type(query, nil), do: query
  defp scope_by_type(query, type), do: where(query, [t], t.type == ^type)

  defp scope_by_priority(query, nil), do: query
  defp scope_by_priority(query, priority), do: where(query, [t], t.priority == ^priority)

  defp transform_priority(:critical), do: 3
  defp transform_priority(:high), do: 2
  defp transform_priority(:normal), do: 1
  defp transform_priority(:low), do: 0

  # Critical tasks bypass any queues and execute immediately
  defp calculate_scheduled_at(:critical), do: DateTime.utc_now()
  # High priority tasks run instantly or within 1 second window
  defp calculate_scheduled_at(:high), do: DateTime.utc_now()
  # Normal tasks are gently staggered by 5 seconds to let critical traffic pass first
  defp calculate_scheduled_at(:normal), do: DateTime.utc_now() |> DateTime.add(5, :second)
  # Low priority tasks are aggressively deferred by 30 seconds to mitigate DB write spikes
  defp calculate_scheduled_at(:low), do: DateTime.utc_now() |> DateTime.add(30, :second)
end
