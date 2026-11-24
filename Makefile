# SkyWatch Automation Makefile
# Targets: test, lint, coverage, smoke, demo, clean, frontend-check, frontend-build, e2e, drill

PROFILE ?= modern
PYTHON ?= python3

.PHONY: all test lint coverage smoke smoke-simulation demo demo-simulation clean help frontend-check frontend-build e2e drill

all: lint test

help:
	@echo "Available targets:"
	@echo "  make lint               - Run Ruff code linting"
	@echo "  make test               - Run unit, contract, and integration tests"
	@echo "  make coverage           - Run tests with NFR-17 coverage gate (80% core modules)"
	@echo "  make smoke [PROFILE=..] - Run smoke acceptance gate (fails if real engine missing)"
	@echo "  make smoke-simulation   - Run smoke acceptance gate allowing simulation fallback"
	@echo "  make demo [PROFILE=..]  - Run demo on sample data (fails if real engine missing)"
	@echo "  make demo-simulation    - Run demo on sample data allowing simulation fallback"
	@echo "  make frontend-check     - Run TypeScript static typecheck on Next.js frontend"
	@echo "  make frontend-build     - Build Next.js 15 production dashboard"
	@echo "  make e2e                - Run full-stack backend-to-frontend end-to-end tests"
	@echo "  make drill              - Run dual-profile rollback verification drill"
	@echo "  make clean              - Remove temporary build, smoke, and test artifacts"

lint:
	$(PYTHON) -m ruff check .

test:
	$(PYTHON) -m pytest -v --tb=short

coverage:
	$(PYTHON) -m pytest --cov=skywatch.data.validation \
		--cov=skywatch.data.normalization \
		--cov=skywatch.data.deduplication \
		--cov=skywatch.analytics_core.cpcb \
		--cov=skywatch.analytics_core.freshness \
		--cov-report=term-missing \
		--cov-report=xml \
		--cov-fail-under=80

smoke:
	SKYWATCH_REQUIRE_REAL_ENGINE=1 bash scripts/smoke.sh $(PROFILE)

smoke-simulation:
	ALLOW_SIMULATION=1 bash scripts/smoke.sh $(PROFILE) --allow-simulation

demo:
	SKYWATCH_REQUIRE_REAL_ENGINE=1 bash scripts/demo.sh $(PROFILE)

demo-simulation:
	ALLOW_SIMULATION=1 bash scripts/demo.sh $(PROFILE) --allow-simulation

frontend-check:
	pnpm --dir frontend exec tsc --noEmit

frontend-build:
	pnpm --dir frontend build

e2e:
	$(PYTHON) -m pytest tests/integration/test_full_stack_e2e.py -v
	pnpm --dir frontend exec tsc --noEmit
	pnpm --dir frontend build

drill:
	bash scripts/rollback_drill.sh --allow-simulation

clean:
	rm -rf data/smoke output/demo data/benchmarks output/clean_air_quality output/published data/drill output/drill_modern output/drill_lab .pytest_cache .coverage coverage.xml
