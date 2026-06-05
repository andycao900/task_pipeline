defmodule TaskPipelineWeb.FallbackControllerTest do
  use TaskPipelineWeb.ConnCase, async: true

  alias TaskPipelineWeb.FallbackController
  alias TaskPipeline.Tasks.Task

  describe "call/2 -> {:error, %Ecto.Changeset{}}" do
    test "directly intercepts invalid changesets and transforms into a clean 422 payload", %{
      conn: conn
    } do
      changeset = Task.changeset(%Task{}, %{})

      assert {:error, %Ecto.Changeset{} = invalid_changeset} =
               Ecto.Changeset.apply_action(changeset, :insert)

      # 💡 Senior Fix: Force register the target serialization format inside the connection's private metadata
      prepared_conn =
        conn
        |> Plug.Conn.fetch_query_params()
        |> Phoenix.Controller.put_format("json")

      result_conn = FallbackController.call(prepared_conn, {:error, invalid_changeset})

      assert result_conn.status == 422
      assert response = json_response(result_conn, :unprocessable_entity)
      assert Map.has_key?(response["errors"], "title")
    end
  end

  describe "call/2 -> {:error, :not_found}" do
    test "directly catches lookup failures and formats a semantic 404 JSON object", %{conn: conn} do
      # 💡 Senior Fix: Apply the same configuration formatting to the 404 path
      prepared_conn =
        conn
        |> Plug.Conn.fetch_query_params()
        |> Phoenix.Controller.put_format("json")

      result_conn = FallbackController.call(prepared_conn, {:error, :not_found})

      assert result_conn.status == 404
      assert response = json_response(result_conn, :not_found)
      assert response == %{"errors" => %{"detail" => "Not Found"}}
    end
  end
end
