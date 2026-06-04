defmodule TaskPipeline.Tasks.Task do
  @moduledoc """
  Defines the core Schema, type definitions, and state machine constraints
  for background operational tasks.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @statuses [:queued, :processing, :completed, :failed]
  @types [:import, :export, :report, :cleanup]
  @priorities [:low, :normal, :high, :critical]

  # Domain Typespecs for Static Analysis (Dialyzer)
  @type id :: binary()
  @type status :: :queued | :processing | :completed | :failed
  @type task_type :: :import | :export | :report | :cleanup
  @type priority :: :low | :normal | :high | :critical
  @type attempt_log :: %{required(String.t()) => any()}

  @type t :: %__MODULE__{
          id: id() | nil,
          title: String.t() | nil,
          type: task_type() | nil,
          priority: priority() | nil,
          payload: map(),
          max_attempts: integer(),
          status: status(),
          attempts: [attempt_log()],
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  schema "tasks" do
    field :title, :string
    field :payload, :map, default: %{}
    field :max_attempts, :integer, default: 3

    # Map database-level native enums using Ecto.Enum
    field :status, Ecto.Enum, values: @statuses, default: :queued
    field :type, Ecto.Enum, values: @types
    field :priority, Ecto.Enum, values: @priorities, default: :normal

    # Embedded list of maps tracking history of job runs
    field :attempts, {:array, :map}, default: []

    timestamps()
  end

  @doc """
  Generates an execution changeset for validating incoming payload ingestion.

  Ensures all mandatory fields are present and data formats adhere to structural constraints.
  """
  @spec changeset(t() | Ecto.Schema.t(), map()) :: Ecto.Changeset.t()
  def changeset(task, attrs) do
    task
    |> cast(attrs, [:title, :type, :priority, :payload, :max_attempts, :status, :attempts])
    |> validate_required([:title, :type, :payload])
    |> validate_number(:max_attempts, greater_than: 0)
  end

  @doc """
  Executes deterministic state transitions enforced by the system lifecycle state machine.

  Guards against race conditions, out-of-order execution, and invalid state jumps.
  Optionally appends an absolute telemetry attempt log payload to the historical log.
  """
  @spec status_changeset(t(), status(), attempt_log() | nil) :: Ecto.Changeset.t()
  def status_changeset(task, new_status, additional_attempt \\ nil) do
    current_status = task.status

    if valid_transition?(current_status, new_status) do
      attempts =
        if additional_attempt,
          do: task.attempts ++ [additional_attempt],
          else: task.attempts

      change(task, %{status: new_status, attempts: attempts})
    else
      change(task)
      |> add_error(:status, "Invalid status transition from #{current_status} to #{new_status}")
    end
  end

  # State machine guard rails
  @spec valid_transition?(status(), status()) :: boolean()
  defp valid_transition?(:queued, :processing), do: true
  defp valid_transition?(:processing, :completed), do: true
  # Re-queued for retry
  defp valid_transition?(:processing, :queued), do: true
  # Max attempts exhausted
  defp valid_transition?(:processing, :failed), do: true
  defp valid_transition?(_from, _to), do: false
end
