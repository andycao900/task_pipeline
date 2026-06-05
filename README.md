# Task Processing Pipeline (Senior Elixir Take-Home Project)

A high-concurrency asynchronous task processing pipeline built with Elixir, Phoenix, and Oban.

---

## Architecture Overview

The system architecture is engineered for predictable sub-millisecond response latencies, absolute transactional integrity, and resilient self-healing capabilities under a high-throughput footprint of $10,000 \text{ tasks/min}$.

Detailed breakdowns of our core engineering solutions—including dual-state alignment, optimistic concurrency shields, jittered backoff systems, error taxonomy management, and long-term caching blueprints—are isolated inside our technical ledger module:

👉 **Detailed System Engineering Ledgers and Tradeoffs can be found in [NODES.md](./NODES.md).**

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

## Local Development Setup & Provisioning

Follow these steps sequentially to configure your local runtime environment, install locked compiler dependencies, and initialize the PostgreSQL relational tables.

### 1. Dependency Management & Database Initialization
Fetch and compile external Elixir packages, configure your local PostgreSQL credentials, and run migrations along with core system seed records.

Before initializing, ensure your target PostgreSQL instance is running and its access permissions align with your environment variables (configured inside `config/dev.exs` and `config/test.exs`).

```bash
# Fetch and compile mix dependencies locked in mix.lock
mix deps.get

# Compile dependencies and your core application
mix deps.compile

# Automated script to create databases, run Ecto migrations, and load relational mock seed states
# this step populates your datastore using the module located at priv/repo/seeds.exs
mix setup

#testing
mix test 
or postman with endpoint url
http://localhost:4000/api/tasks/
http://localhost:4000/api/tasks/summary