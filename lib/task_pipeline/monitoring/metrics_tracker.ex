defmodule TaskPipeline.Monitoring.MetricsTracker do
  use GenServer
  require Logger

  @type state :: %{
          processed_count: integer(),
          failure_count: integer(),
          total_duration_ms: integer()
        }

  # --- Client API ---

  @doc """
  Starts the MetricsTracker process.
  Supports optional name registration to enable decoupled integration testing.
  """
  def start_link(opts) do
    case Keyword.pop(opts, :name) do
      {nil, server_opts} ->
        GenServer.start_link(__MODULE__, server_opts)

      {name, server_opts} ->
        GenServer.start_link(__MODULE__, server_opts, name: name)
    end
  end

  @doc "Logs a successful task execution with its processing latency."
  @spec log_success(integer()) :: :ok
  def log_success(duration_ms) do
    GenServer.cast(__MODULE__, {:log_success, duration_ms})
  end

  @doc "Logs a task failure."
  @spec log_failure() :: :ok
  def log_failure do
    GenServer.cast(__MODULE__, :log_failure)
  end

  @doc "Retrieves the current cached metrics state snapshot."
  @spec get_stats() :: map()
  def get_stats do
    GenServer.call(__MODULE__, :get_stats)
  end

  @doc "Resets all runtime diagnostic counters to zero. Built exclusively for test isolation environments."
  @spec reset_counters() :: :ok
  def reset_counters do
    GenServer.cast(__MODULE__, :reset_counters)
  end

  # --- Server Callbacks ---

  @impl true
  def init(_init_arg) do
    Logger.info("MetricsTracker telemetry core initialized successfully.")
    {:ok, %{processed_count: 0, failure_count: 0, total_duration_ms: 0}}
  end

  @impl true
  def handle_cast({:log_success, duration_ms}, state) do
    new_state = %{
      state
      | processed_count: state.processed_count + 1,
        total_duration_ms: state.total_duration_ms + duration_ms
    }

    {:noreply, new_state}
  end

  @impl true
  def handle_cast(:log_failure, state) do
    new_state = %{state | failure_count: state.failure_count + 1}
    {:noreply, new_state}
  end

  @impl true
  def handle_cast(:reset_counters, _state) do
    {:noreply, %{processed_count: 0, failure_count: 0, total_duration_ms: 0}}
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    avg_duration =
      if state.processed_count > 0,
        do: Float.round(state.total_duration_ms / state.processed_count, 2),
        else: 0.0

    stats = %{
      processed_count: state.processed_count,
      failure_count: state.failure_count,
      average_duration_ms: avg_duration
    }

    {:reply, stats, state}
  end
end
