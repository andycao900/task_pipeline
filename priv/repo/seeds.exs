alias TaskPipeline.Repo
alias TaskPipeline.Tasks.Task
alias Ecto.UUID

# Configure dynamic logger to clean setup interfaces
Logger.configure(level: :info)
Mix.shell().info("=== Beginning Bulk Ingestion Sequence: 150 Domain Tasks ===")

# 1. Define static matrix blocks strictly limited to your schema's valid Ecto.Enum values
types = ["import", "export", "report", "cleanup"]
statuses = [:completed, :completed, :completed, :failed, :queued, :processing]

# 2. Clear out existing remnants to guarantee baseline repeatability
Repo.delete_all(Task)
Mix.shell().info("Database cleared of existing task records.")

# 3. Microsecond operational loop to stream 150 deterministic rows
tasks =
  Enum.map(1..150, fn index ->
    # Enforce deterministic distributions using index division remainders from legal types
    type = Enum.at(types, rem(index, length(types)))

    # Pareto Distribution Simulation: bias priorities heavily toward normal/low
    priority =
      cond do
        rem(index, 15) == 0 -> "critical"
        rem(index, 7) == 0 -> "high"
        rem(index, 2) == 0 -> "normal"
        true -> "low"
      end

    # Distribute statuses across the matrix block
    status = Enum.at(statuses, rem(index, length(statuses)))

    # Generate clean NaiveDateTime to perfectly match the underlying database schema column constraints
    inserted_at =
      NaiveDateTime.utc_now()
      |> NaiveDateTime.add(-index * 60, :second)
      |> NaiveDateTime.truncate(:second)

    updated_at = NaiveDateTime.truncate(NaiveDateTime.utc_now(), :second)

    # Synthesize polymorphic execution attempt metadata records for completed/failed tasks
    attempts =
      case status do
        :completed ->
          [
            %{
              "attempt" => 1,
              "message" => "initiated pipeline handshake",
              "recorded_at" => NaiveDateTime.to_string(NaiveDateTime.add(inserted_at, 2, :second))
            },
            %{
              "attempt" => 2,
              "message" => "completed execution successfully",
              "recorded_at" =>
                NaiveDateTime.to_string(NaiveDateTime.add(inserted_at, 12, :second))
            }
          ]

        :failed ->
          [
            %{
              "attempt" => 1,
              "message" => "Transient failure: 504 Gateway Timeout encountered",
              "recorded_at" => NaiveDateTime.to_string(NaiveDateTime.add(inserted_at, 5, :second))
            },
            %{
              "attempt" => 2,
              "message" => "Fatal business error: Retry limits exhausted",
              "recorded_at" =>
                NaiveDateTime.to_string(NaiveDateTime.add(inserted_at, 45, :second))
            }
          ]

        _ ->
          []
      end

    # Yield raw map data structure prepped for hyper-fast Ecto bulk insertion
    %{
      id: UUID.generate(),
      title: "Automated Operational Stream Job ##{index}",
      type: String.to_atom(type),
      priority: String.to_atom(priority),
      status: status,
      max_attempts: 5,
      lock_version: 1,
      payload: %{
        "batch_sequence" => index,
        "source_node" => "cluster_worker_#{rem(index, 4)}",
        "file_size_bytes" => index * 204_850
      },
      attempts: attempts,
      inserted_at: inserted_at,
      updated_at: updated_at
    }
  end)

# 4. Fire high-performance Repo.insert_all/3 to minimize network round-trips
{count, _} = Repo.insert_all(Task, tasks)

Mix.shell().info("=== Bulk Ingestion Completed: #{count} task rows injected successfully ===")
