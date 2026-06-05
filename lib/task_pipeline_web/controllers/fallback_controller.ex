defmodule TaskPipelineWeb.FallbackController do
  @moduledoc """
  Translates domain and infrastructure error signatures into clean, semantic JSON responses.
  """
  use TaskPipelineWeb, :controller

  # Translates Ecto changeset validation errors into standard 422 Unprocessable Entity payloads.
  def call(conn, {:error, %Ecto.Changeset{} = changeset}) do
    conn
    |> put_status(:unprocessable_entity)
    |> put_view(json: TaskPipelineWeb.ChangesetJSON)
    |> render(:error, changeset: changeset)
  end

  # Translates missing entity lookup failures into semantic 404 responses.
  def call(conn, {:error, :not_found}) do
    conn
    |> put_status(:not_found)
    |> put_view(json: TaskPipelineWeb.ErrorJSON)
    |> render("404.json")
  end
end
