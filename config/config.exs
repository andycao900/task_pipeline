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

import Config

# --- Task Pipeline Core Engine Configuration ---

# Default random fault threshold (20% chance to trigger business logic failure simulation)
config :task_pipeline, :task_failure_threshold, 20

# Standard Production & Development Jitter Delay Profiles (in milliseconds)
# Formulates the traffic staggering matrices across concurrent workers
config :task_pipeline, :priority_durations,
  # 1-2 Seconds
  critical: [base: 1000, rand: 1001],
  # 2-4 Seconds
  high: [base: 2000, rand: 2001],
  # 4-6 Seconds
  normal: [base: 4000, rand: 2001],
  # 6-8 Seconds
  low: [base: 6000, rand: 2001]

# Standard Oban Open-Source Engine Core Layout
config :task_pipeline, Oban,
  engine: Oban.Engines.Basic,
  repo: TaskPipeline.Repo,
  # Configure automatic pruning to prevent the oban_jobs table from bloating the database under a 10k/min throughput
  # Retain 24 hours of history
  plugins: [{Oban.Plugins.Pruner, max_age: 3600 * 24}],
  # Allows up to 50 concurrent operational execution threads per node
  queues: [tasks: 50]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
