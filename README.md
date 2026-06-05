# Task Processing Pipeline (Senior Elixir Take-Home Project)

A high-concurrency asynchronous task processing pipeline built with Elixir, Phoenix, and Oban.

---

## Architecture Design

The core system architecture is engineered for predictable latency, strict lifecycle tracking, and failure resilience under high throughput.

### Key Architectural Decisions

1. **Transactional Dual-Write Engine**: Task ingestion couples domain record generation with background scheduling using an isolated `Ecto.Multi` block. This prevents orphaned processing records and guarantees transactional parity across business states and Oban execution queues.
2. **Concurrency Shield via Optimistic Locking**: To handle high-volume row modifications across horizontal nodes without locking database rows (`SELECT FOR UPDATE`), the `tasks` schema uses an atomic `lock_version` mechanism. Concurrent stale write attempts during state machine mutations are deflected safely at the database layer, preventing data corruption.
3. **Advanced Priority Scheduling & Traffic Staggering**: The pipeline utilizes a dual-tier mechanism to process high-priority tasks first without causing queue starvation or consumer locks:
   * **Oban Priority Weights**: Maps domain priorities directly to Oban numerical weights (`critical` -> `3`, `low` -> `0`). Oban's poll engine reads these values to order jobs natively via database index scans (`ORDER BY priority DESC`).
   * **Staggered Queue Ingestion (`scheduled_at`)**: To protect infrastructure from spikes under a $10,000 \text{ tasks/min}$ throughput, low and normal tasks are automatically injected with an engineered scheduling offset (`+5s` for normal, `+30s` for low). Critical paths completely bypass this delay, ensuring immediate execution.
4. **Idempotent Job Processing**: Workers execute a non-blocking pre-flight state validation guard asserting that the processing task is strictly in a `:queued` state before firing any code. This guarantees execution idempotency if a worker drops or redelivers a job signature under load.
5. **Denormalized Telemetry Array**: Execution step details and retry histories are stored within an embedded JSONB `attempts` column. This structural pattern avoids cross-table write bottlenecks during high-concurrency loops and keeps historical audit logs localized to a single row read ($O(1)$).
6. **Locality-of-Reference Web Colocation**: Adhering to modern Phoenix standards, `task_json.ex` and `task_controller.ex` are colocated inside the same `controllers/` directory rather than split across arbitrary technical layer boundaries. This enforces tight encapsulation of the API presentation contract, reduces cognitive file-hopping overhead, and minimizes the blast radius during future API versioning or deprecations.
7. **Database Bloat Mitigation**: To sustainably handle heavy traffic profiles (e.g., 10k tasks/min), `Oban.Plugins.Pruner` is configured to automatically purge historical jobs after 24 hours. This eliminates table bloat and prevents database index degradation under persistent write amplification.
8. **Database Indexing & Query Optimization**: 
   * **Native PG Enums**: Enforced at the database tier to minimize storage consumption and guarantee structural validity.
   * **Composite Index**: Tailored directly for the `GET /api/tasks` endpoint requirements, sorting high-priority and newer tasks first.
   * **Partial Indexing**: Under a 10k/min scale, finished tasks dominate storage. We isolate hot-reads by maintaining a dedicated index `WHERE status IN ('queued', 'processing')`, securing sub-millisecond query execution.

---

### Future Scalability Considerations

* **Batch Ingestion vs. Atomic Processing**: While batch database insertions (`Repo.insert_all`) are highly effective at the ingestion layer (`POST /api/tasks`) to minimize network round-trips during bulk task creation, they are explicitly avoided within the background processing execution layer (`Oban.Worker`). Processing tasks individually preserves strict blast-radius isolation (preventing a single task failure from rolling back an entire batch) and maintains the integrity of the atomic `lock_version` state-machine guards.
* **Cursor-Based Pagination Strategy**: To sustainably fulfill the query latency SLAs under millions of rows, the `GET /api/tasks` endpoint is architected around a cursor-based pagination strategy (`TODO`). Traditional `OFFSET / LIMIT` pagination is rejected as it forces PostgreSQL to perform expensive $O(N)$ linear sequential scans over older rows. Utilizing an opaque cursor (`inserted_at` paired with `id`) locks query evaluation into an $O(\log N)$ index-seek timeline, neutralizing database response degradation as data layers deepen.

---
## System Prerequisites

This project enforces strict version targeting. Ensure you have `asdf` or a compatible version manager installed to read the `.tool-versions` file.

* **Erlang/OTP:** 28.3
* **Elixir:** 1.19.5-otp-28
* **PostgreSQL:** 15+

---

## Getting Started

Follow these steps sequentially to provision the database, compile dependencies, and run the server.

### 1. Environment Setup
Ensure your local language runtime matches the project specifications:
```bash
# If using asdf
asdf plugin add erlang
asdf plugin add elixir
asdf install