defmodule TaskPipelineWeb.PageController do
  use TaskPipelineWeb, :controller

  def index(conn, _params) do
    json(conn, %{status: "ok", message: "Task Pipeline API is running"})
  end
end
