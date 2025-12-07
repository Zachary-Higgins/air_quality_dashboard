# Copilot Instructions

1. Keep the Grafana dashboard JSON in sync with MySQL schema changes so provisioned dashboards load cleanly.
2. Honor `.env` variables when scripting or documenting anything involving the worker container.
3. When provisioning fails, check container logs before modifying agent configs (see the agent roles in `AGENTS.md`).

| Name | Role | Description |
| --- | --- | --- |
| Grafana | Visualization | Renders the Air Quality Overview dashboard from provisioned JSON and MySQL data. |
| Worker | Ingestion | Polls Airthings API, enriches readings, and stores telemetry rows in MySQL. |
| MySQL | Storage | Persists telemetry and serves Grafana queries via the MySQL data source. |