defmodule TaskPipeline.Repo.Migrations.AddObanJobs do
  use Ecto.Migration

  def up do
    Oban.Migration.up(version: 14)
  end

  def down do
    Oban.Migration.down(version: 0)
  end
end
