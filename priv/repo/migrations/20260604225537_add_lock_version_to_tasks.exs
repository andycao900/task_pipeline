defmodule TaskPipeline.Repo.Migrations.AddLockVersionToTasks do
  use Ecto.Migration

  def change do
    alter table(:tasks) do
      # Defaults to 1, cannot be null. Ecto will auto-increment this on every update.
      add :lock_version, :integer, default: 1, null: false
    end
  end
end
