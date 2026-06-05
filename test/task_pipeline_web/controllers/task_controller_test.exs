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

    test "error: returns 404 block gracefully when a structurally corrupted non-UUID identifier is sent",
         %{conn: conn} do
      # Pass a plain string that violates binary_id format completely to verify parsing stability
      response_conn = get(conn, ~p"/api/tasks/not-a-valid-uuid")

      assert json_response(response_conn, :not_found)["errors"] == %{"detail" => "Not Found"}
    end
  end

  describe "GET /api/tasks/summary (Metric Aggregations)" do
    test "calculates state machine counts across a matrix of concurrent enterprise states", %{
      conn: conn
    } do
      # 1. Batch create 20 tasks to build our metrics baseline
      tasks =
        for i <- 1..20 do
          attrs = Map.put(@valid_attrs, "title", "Bulk Task #{i}")
          {:ok, %{task: task}} = Tasks.create_task(attrs)
          task
        end

      # Splitting tasks array into dedicated pools for explicit status transitions
      {failed_pool, remaining} = Enum.split(tasks, 1)
      {processing_pool, remaining} = Enum.split(remaining, 2)
      {completed_pool, _queued_pool} = Enum.split(remaining, 12)

      # 2. Advance states through the structural domain lifecycle context
      for task <- failed_pool do
        {:ok, p_task} = Tasks.update_task_status(task, :processing, %{})

        {:ok, _} =
          Tasks.update_task_status(p_task, :failed, %{"message" => "forced terminal failure"})
      end

      for task <- processing_pool do
        {:ok, _} =
          Tasks.update_task_status(task, :processing, %{"message" => "active execution block"})
      end

      for task <- completed_pool do
        {:ok, p_task} = Tasks.update_task_status(task, :processing, %{})

        {:ok, _} =
          Tasks.update_task_status(p_task, :completed, %{"message" => "successful completion"})
      end

      # 3. Trigger the presentation tier gateway endpoint
      response_conn = get(conn, ~p"/api/tasks/summary")
      assert summary = json_response(response_conn, :ok)["data"]

      # 4. Strict assertion against the full multi-tenant aggregate metrics contract
      assert summary == %{
               "queued" => 5,
               "processing" => 2,
               "completed" => 12,
               "failed" => 1
             }
    end

    test "calculates summary perfectly by providing complete fallback zeroes when active states are missing",
         %{conn: conn} do
      # Insert only 1 single queued item to assert data structure zero-filling logic
      assert {:ok, _} = Tasks.create_task(@valid_attrs)

      response_conn = get(conn, ~p"/api/tasks/summary")
      assert summary = json_response(response_conn, :ok)["data"]

      # Empty or missing status buckets must never be skipped from the JSON schema contract
      assert summary == %{
               "queued" => 1,
               "processing" => 0,
               "completed" => 0,
               "failed" => 0
             }
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

    test "applies scope isolation filters perfectly and secures clean empty arrays under no-match queries",
         %{conn: conn} do
      assert {:ok, _} = Tasks.create_task(@valid_attrs)

      # Query exclusively for 'low' items when only 'critical' items populate the datastore
      response_conn = get(conn, ~p"/api/tasks", %{"priority" => "low"})
      assert json_response(response_conn, :ok)["data"] == []
    end
  end
end
