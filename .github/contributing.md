# Contributing Guidelines

1. **Discuss first** – open an issue before large changes so we can agree on scope.
2. **Environment** – copy `.env.template` → `.env`, fill in secrets, and never commit the `.env` file.
3. **Code style** – favor concise Python (PEP 8) and keep Grafana JSON UTF‑8 without BOM.
4. **Testing** – run `docker compose build` locally before pushing; make sure the GitHub Action `PR Compose Build` is green on your PR.
5. **Docs** – update `README.md`, `TESTED_DEVICES.md`, or other docs whenever behavior or hardware support changes.
6. **Security** – don’t include real credentials, tokens, or volume data in commits.
7. **Reviews** – fill out the PR template (summary, testing, checklist) so reviewers can merge quickly.
