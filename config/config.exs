# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

config :task_pipeline,
  ecto_repos: [TaskPipeline.Repo],
  generators: [timestamp_type: :utc_datetime]

# Configure the endpoint
config :task_pipeline, TaskPipelineWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [json: TaskPipelineWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: TaskPipeline.PubSub,
  live_view: [signing_salt: "c/Xx74Ej"]

# Configure the mailer
#
# By default it uses the "Local" adapter which stores the emails
# locally. You can see the emails in your browser, at "/dev/mailbox".
#
# For production it's recommended to configure a different adapter
# at the `config/runtime.exs`.
config :task_pipeline, TaskPipeline.Mailer, adapter: Swoosh.Adapters.Local

# Configure Elixir's Logger
config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

config :task_pipeline, Oban,
  engine: Oban.Engines.Basic,
  repo: TaskPipeline.Repo,
  queues: [
    # Set the default task queue concurrency limit to 10
    tasks: [limit: 10]
  ],
  plugins: [
    # Configure automatic pruning to prevent the oban_jobs table from bloating the database under a 10k/min throughput
    # Retain 24 hours of history
    {Oban.Plugins.Pruner, max_age: 3600 * 24}
  ]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
