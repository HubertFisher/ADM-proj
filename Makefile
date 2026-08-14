PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

.PHONY: install format lint test check compose-config cluster-up cluster-status cluster-verify cluster-down ingest

install:
	$(PYTHON) -m pip install -e '.[dev]'

format:
	$(PYTHON) -m ruff format .
	$(PYTHON) -m ruff check --fix .

lint:
	$(PYTHON) -m ruff format --check .
	$(PYTHON) -m ruff check .
	$(PYTHON) -m mypy src

test:
	$(PYTHON) -m pytest --cov --cov-report=term-missing

compose-config:
	docker compose config --quiet

cluster-up:
	docker compose up -d --wait

cluster-status:
	docker compose exec -T mongos mongosh --quiet --file /scripts/inspect-cluster.js

cluster-verify:
	docker compose exec -T mongos mongosh --quiet --file /scripts/verify-cluster.js

cluster-down:
	docker compose down

ingest:
	docker compose --profile ingest run --build --rm ingest

check: lint test compose-config
