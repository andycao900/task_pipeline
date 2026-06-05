import Config

# Configure your database
#
# The MIX_TEST_PARTITION environment variable can be used
# to provide built-in test partitioning in CI environment.
# Run `mix help test` for more information.
config :task_pipeline, TaskPipeline.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "task_pipeline_test#{System.get_env("MIX_TEST_PARTITION")}",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: System.schedulers_online() * 2

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :task_pipeline, TaskPipelineWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "Ac9zhLisPsYvNZM1wDiKskVYoYBn74ib3p/qqOchXarixy/IlrY+X14nEpk/kJqx",
  server: false

# In test we don't send emails
config :task_pipeline, TaskPipeline.Mailer, adapter: Swoosh.Adapters.Test

# Disable swoosh api client as it is only required for production adapters
config :swoosh, :api_client, false

# Print only warnings and errors during test
config :logger, level: :warning

# Initialize plugs at runtime for faster test compilation
config :phoenix, :plug_init_mode, :runtime

# Sort query params output of verified routes for robust url comparisons
config :phoenix,
  sort_verified_routes_query_params: true

# Compress simulation processing latencies to 0ms to eliminate un-testable idle wait states
config :task_pipeline, :priority_durations,
  critical: [base: 0, rand: 1],
  high: [base: 0, rand: 1],
  normal: [base: 0, rand: 1],
  low: [base: 0, rand: 1]

# Set a predictable fault gate for random fallbacks if no precise mock is injected
config :task_pipeline, :task_failure_threshold, 20

config :task_pipeline, Oban,
  repo: TaskPipeline.Repo,
  # Enable manual testing mode (this automatically disables real background queue consumption and peer polling)
  testing: :manual,
  # Completely disable background plugins (e.g., Pruner) during testing to prevent unpredictable concurrent database access
  plugins: false,
  # Disable distributed cluster peer election within the test environment
  peer: false
