defmodule TaskPipeline.Repo do
  use Ecto.Repo,
    otp_app: :task_pipeline,
    adapter: Ecto.Adapters.Postgres
end
