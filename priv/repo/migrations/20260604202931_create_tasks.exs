defmodule TaskPipeline.Repo.Migrations.CreateTasks do
  use Ecto.Migration

  def up do
    # Create native PostgreSQL ENUM types for data integrity and storage efficiency
    execute("CREATE TYPE task_type AS ENUM ('import', 'export', 'report', 'cleanup')")
    execute("CREATE TYPE task_priority AS ENUM ('low', 'normal', 'high', 'critical')")
    execute("CREATE TYPE task_status AS ENUM ('queued', 'processing', 'completed', 'failed')")

    create table(:tasks, primary_key: false) do
      # UUID/BinaryID for distributed system compliance
      add :id, :binary_id, primary_key: true
      add :title, :string, null: false
      add :type, :task_type, null: false
      add :priority, :task_priority, null: false, default: "normal"
      add :payload, :map, null: false, default: "{}"
      add :max_attempts, :integer, null: false, default: 3
      add :status, :task_status, null: false, default: "queued"
      # Embedded JSONB for O(1) attempt log writes
      add :attempts, :jsonb, null: false, default: "[]"

      timestamps()
    end

    # Composite index for API listing, filtering, and optimized sorting (Priority DESC, Creation DESC)
    create index(:tasks, [:status, :type, :priority, :inserted_at])

    # Partial index to drastically reduce index size and memory footprint under 10k/min throughput
    # Optimizes frequent operational lookups for active workloads while ignoring historical bloat
    execute("""
    CREATE INDEX tasks_active_status_index
    ON tasks (status, priority DESC, inserted_at DESC)
    WHERE status IN ('queued', 'processing');
    """)
  end

  def down do
    drop table(:tasks)
    execute("DROP TYPE task_status")
    execute("DROP TYPE task_priority")
    execute("DROP TYPE task_type")
  end
end
