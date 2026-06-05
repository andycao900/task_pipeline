defmodule TaskPipelineWeb.TaskJSON do
  @moduledoc """
  Optimized API presentation layer for the Tasks boundary context.
  Maintains clean, flat serialization matrices with minimal CPU allocation signatures.
  """
  alias TaskPipeline.Tasks.Task

  @doc """
  Renders a collection of processed domain tasks.
  """
  def index(%{tasks: tasks}) do
    %{data: render_many(tasks, &data/1)}
  end

  @doc """
  Renders an isolated singular task record along with its JSONB attempt logs.
  """
  def show(%{task: task}) do
    %{data: data(task)}
  end

  @doc """
  Renders a real-time aggregated summary dictionary map.
  """
  def summary(%{summary: summary}) do
    %{data: summary}
  end

  # Helper serializer mapper to keep memory foot-print lean.
  defp data(%Task{} = task) do
    %{
      id: task.id,
      title: task.title,
      type: to_string(task.type),
      status: to_string(task.status),
      priority: to_string(task.priority),
      payload: task.payload,
      max_attempts: task.max_attempts,
      lock_version: task.lock_version,
      attempts: task.attempts || [],
      inserted_at: NaiveDateTime.to_iso8601(task.inserted_at),
      updated_at: NaiveDateTime.to_iso8601(task.updated_at)
    }
  end

  defp render_many(collection, mapper) do
    Enum.map(collection, mapper)
  end
end
