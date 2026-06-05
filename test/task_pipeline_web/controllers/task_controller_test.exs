defmodule TaskPipelineWeb.TaskControllerTest do
  use TaskPipelineWeb.ConnCase, async: true
  use Oban.Testing, repo: TaskPipeline.Repo

  alias TaskPipeline.Tasks

  @valid_attrs %{
    "title" => "On-Demand Transcoding",
    "type" => "export",
    "priority" => "critical",
    "payload" => %{"resolution" => "4k"}
  }

  setup %{conn: conn} do
    # Senior Protocol Guard: Guarantee all requests specify JSON accept headers
    {:ok, conn: put_req_header(conn, "accept", "application/json")}
  end

  describe "POST /api/tasks (Ingestion Gateway)" do
    test "renders flat task JSON and verifies transactional Oban enqueueing", %{conn: conn} do
      response_conn = post(conn, ~p"/api/tasks", @valid_attrs)

      assert %{"data" => json} = json_response(response_conn, :created)
      assert json["title"] == "On-Demand Transcoding"
      assert json["status"] == "queued"
      assert json["priority"] == "critical"
      assert is_list(json["attempts"])

      # Validate compliance with NaiveDateTime ISO8601 formatting strings
      assert {:ok, %DateTime{} = _parsed_dt, _offset} =
               DateTime.from_iso8601(json["inserted_at"] <> "Z")

      # Verify Async Side-Effect: Oban job must be committed atomically within the transaction
      assert_enqueued(
        worker: TaskPipeline.Workers.TaskProcessor,
        args: %{"task_id" => json["id"]}
      )
    end

    test "intercepts validation failures and routes cleanly through FallbackController to 422", %{
      conn: conn
    } do
      # Pass an invalid enum value to trigger our protected ChangesetJSON encoder boundary
      invalid_attrs = Map.put(@valid_attrs, "type", "invalid_enum_value")
      response_conn = post(conn, ~p"/api/tasks", invalid_attrs)

      assert response = json_response(response_conn, :unprocessable_entity)
      assert %{"errors" => %{"type" => ["is invalid"]}} = response
    end
  end

  describe "GET /api/tasks/:id (Resource Extraction)" do
    test "renders single task details along with empty telemetry arrays", %{conn: conn} do
      assert {:ok, %{task: task}} = Tasks.create_task(@valid_attrs)

      response_conn = get(conn, ~p"/api/tasks/#{task.id}")
      assert %{"data" => json} = json_response(response_conn, :ok)

      assert json["id"] == task.id
      assert json["title"] == "On-Demand Transcoding"
      assert json["attempts"] == []
    end

    test "returns clean 404 response when missing binary UUID is queried", %{conn: conn} do
      missing_uuid = Ecto.UUID.generate()
      response_conn = get(conn, ~p"/api/tasks/#{missing_uuid}")

      assert json_response(response_conn, :not_found)["errors"] == %{"detail" => "Not Found"}
    end
  end

  describe "GET /api/tasks/summary (Metric Aggregations)" do
    test "calculates state machine counts using optimized group_by matrix maps", %{conn: conn} do
      assert {:ok, _} = Tasks.create_task(@valid_attrs)
      assert {:ok, _} = Tasks.create_task(@valid_attrs)

      response_conn = get(conn, ~p"/api/tasks/summary")
      assert %{"data" => summary} = json_response(response_conn, :ok)

      assert summary == %{"queued" => 2}
    end
  end

  describe "GET /api/tasks (Collection Ingestion & Filtering)" do
    test "applies composite sorting priorities and matches isolated filtering scopes", %{
      conn: conn
    } do
      assert {:ok, %{task: critical_task}} = Tasks.create_task(@valid_attrs)

      low_attrs = Map.put(@valid_attrs, "priority", "low")
      assert {:ok, %{task: low_task}} = Tasks.create_task(low_attrs)

      # 1. Test Base Order: Critical priority items must float to the top of the collection index
      index_conn = get(conn, ~p"/api/tasks")
      assert %{"data" => [first, second]} = json_response(index_conn, :ok)
      assert first["id"] == critical_task.id
      assert second["id"] == low_task.id

      # 2. Test Parameter Filtering
      filtered_conn = get(conn, ~p"/api/tasks", %{"priority" => "low"})
      assert %{"data" => [matched_task]} = json_response(filtered_conn, :ok)
      assert matched_task["id"] == low_task.id
    end
  end
end
