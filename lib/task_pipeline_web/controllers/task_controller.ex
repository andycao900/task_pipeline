defmodule TaskPipelineWeb.TaskController do
  @moduledoc """
  [ Client Request ] -> [ Phoenix Ingestion Controller ]
                  │
                  ▼
  ┌──────────────────────────────┐
  │    Ecto.Multi Transaction    │
  │                              │
  │  Step 1: Write Domain Task   │
  │  Step 2: Inject Oban Job     │
  └──────────────┬───────────────┘
                 │ (Guarantees Dual-Write Parity)
                 ▼
  ┌──────────────────────────────┐
  │     PostgreSQL Database      │
  │  - tasks: [status]=:queued   │
  │  - oban_jobs: available      │
  └──────────────────────────────┘
  """
  use TaskPipelineWeb, :controller

  alias TaskPipeline.Tasks
  alias TaskPipeline.Tasks.Task

  action_fallback TaskPipelineWeb.FallbackController

  @doc """
  Lists tasks using composable internal query maps with filtering.
  """
  @spec index(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def index(conn, params) do
    # TODO: To sustainably fulfill the 10,000 tasks/min concurrent SLA requirements,
    # traditional `OFFSET / LIMIT` pagination MUST BE REJECTED.
    # As the database row volume grows into millions, OFFSET forces PostgreSQL to perform
    # a linear sequential scan (O(N) cost) discarding N rows prior to returning results.
    #
    # Mitigation Implementation Strategy:
    # 1. We will extract a deterministic pagination cursor (e.g., `params["after_id"]` and `params["after_timestamp"]`).
    # 2. Slice the query utilizing an explicit range condition against our composite index:
    #    `where(query, [t], t.inserted_at < ^cursor_timestamp or (t.inserted_at == ^cursor_timestamp and t.id < ^cursor_id))`
    # 3. This locks the database query execution planner into an O(log N) constant index-seek timeline,
    #    neutralizing persistent response degradation regardless of relational pool depths.

    tasks = Tasks.list_tasks(params)
    render(conn, :index, tasks: tasks)
  end

  @doc """
  Retrieves a discrete task structural instance by its binary ID signature.
  """
  @spec show(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def show(conn, %{"id" => id}) do
    # Forward guard check to filter out non-UUID garbage injections before database ingestion
    case Ecto.UUID.cast(id) do
      {:ok, valid_uuid} ->
        case Tasks.get_task(valid_uuid) do
          nil ->
            conn
            |> put_status(:not_found)
            |> put_view(html: TaskPipelineWeb.ErrorHTML, json: TaskPipelineWeb.ErrorJSON)
            |> render(:"404")

          %Task{} = task ->
            render(conn, :show, task: task)
        end

      :error ->
        # Smoothly reject invalid malformed strings with a clean 404/400 without crashing the node
        conn
        |> put_status(:not_found)
        |> put_view(html: TaskPipelineWeb.ErrorHTML, json: TaskPipelineWeb.ErrorJSON)
        |> render(:"404")
    end
  end

  @doc """
  Aggregates status statistics broken down by state machine parameters.
  """
  @spec summary(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def summary(conn, _params) do
    summary = Tasks.get_tasks_summary()
    render(conn, :summary, summary: summary)
  end

  @doc """
  Ingests raw task payloads, orchestrating transactional dual-system enqueueing operations.
  """
  @spec create(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def create(conn, task_params) do
    case Tasks.create_task(task_params) do
      {:ok, %{task: %Task{} = task}} ->
        conn
        |> put_status(:created)
        |> render(:show, task: task)

      {:error, :task, %Ecto.Changeset{} = changeset, _steps} ->
        # Hijack validation anomalies and pipe them smoothly down into the error presentation filter
        {:error, changeset}
    end
  end
end
