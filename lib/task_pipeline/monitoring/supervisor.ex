defmodule TaskPipeline.Monitoring.Supervisor do
  use Supervisor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = [
      # If MetricsTracker crashes, the Supervisor will restart it individually without bleeding faults upstream
      {TaskPipeline.Monitoring.MetricsTracker, []}
    ]

    # :one_for_one guarantees precise isolation for this diagnostic boundary
    Supervisor.init(children, strategy: :one_for_one)
  end
end
