defmodule TaskPipeline.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      TaskPipelineWeb.Telemetry,
      TaskPipeline.Repo,
      {DNSCluster, query: Application.get_env(:task_pipeline, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: TaskPipeline.PubSub},
      # Start a worker by calling: TaskPipeline.Worker.start_link(arg)
      # {TaskPipeline.Worker, arg},
      # Start to serve requests, typically the last entry
      TaskPipelineWeb.Endpoint
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: TaskPipeline.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    TaskPipelineWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
