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

config :task_pipeline, Oban,
  repo: TaskPipeline.Repo,
  # 开启手动测试模式（这会自动禁用后台实际的队列消费和 Peer 轮询）
  testing: :manual,
  # 在测试中完全禁用后台插件（例如 Pruner），避免它们在测试期间无序访问数据库
  plugins: false,
  # 禁用 Peer 选举
  peer: false
