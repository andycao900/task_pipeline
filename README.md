# Task Processing Pipeline (Senior Elixir Take-Home Project)

A high-concurrency asynchronous task processing pipeline built with Elixir, Phoenix, and Oban.

---

## Architecture Design

The core system architecture is engineered for predictable latency, strict lifecycle tracking, and failure resilience under high throughput.

```mermaid
graph TD
    App[TaskPipeline.Application]
    Repo[TaskPipeline.Repo <br/> Connection Pool]
    Oban[Oban <br/> Queue Processing]
    Endpoint[TaskPipelineWeb.Endpoint <br/> HTTP Server]

    App --> Repo
    Repo --> Oban
    Oban --> Endpoint

    style App fill:#f9f,stroke:#333,stroke-width:2px
    style Repo fill:#bbf,stroke:#333,stroke-width:1px
    style Oban fill:#bbf,stroke:#333,stroke-width:1px
    style Endpoint fill:#bbf,stroke:#333,stroke-width:1px

### Key Architectural Decisions

1. **Supervision Tree Orchestration**: `Oban` is strictly supervised *after* `TaskPipeline.Repo` and *before* `TaskPipelineWeb.Endpoint`. This guarantees the PostgreSQL connection pool is fully warmed up before Oban boots, and ensures background workers are ready to consume workloads before the HTTP server begins routing live traffic.
2. **Transactional Enqueueing**: Task record persistence and Oban job insertions are wrapped within an `Ecto.Multi` transaction. This enforces strict atomicity—preventing phantom jobs and ensuring data consistency.
3. **Database Bloat Mitigation**: To sustainably handle heavy traffic profiles (e.g., 10k tasks/min), `Oban.Plugins.Pruner` is configured to automatically purge historical jobs after 24 hours. This eliminates table bloat and prevents database index degradation under persistent write amplification.

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