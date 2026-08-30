# aiops lakehouse — developer entrypoints.
#
# SOURCE selects the data backend for gen/load/detect/chat: local | iceberg.
#   make detect SOURCE=iceberg
# Extra flags pass through via ARGS:
#   make chat ARGS='"why is patient onboarding slow?" --backend ollama'

PYTHON ?= python3
CONFIG ?= config.ini
ALIAS  ?= aiops
SOURCE ?= local
ARGS   ?=

.DEFAULT_GOAL := help

.PHONY: help setup gen load detect chat test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-8s\033[0m %s\n", $$1, $$2}'

setup: ## Wire up AIStor: mc alias, raw bucket, Iceberg warehouse (idempotent)
	./bin/01_setup.sh --config $(CONFIG) --alias $(ALIAS) $(ARGS)

gen: ## Generate synthetic 2,000-VM telemetry (M1)
	$(PYTHON) bin/02_generate.py $(ARGS)

load: ## Load raw parquet into Iceberg tables (M2). Defaults to s3; ARGS='--source local' to load local chunks
	$(PYTHON) bin/03_load_iceberg.py $(ARGS)

lake-info: ## Show Iceberg tables, row counts, snapshots, time-travel (M2)
	$(PYTHON) bin/lake_info.py $(ARGS)

detect: ## Run detection engine, write alerts (M3)
	$(PYTHON) bin/04_detect.py --source $(SOURCE) $(ARGS)

replay: ## Heap-leak early-warning replay timeline (M3 money-shot)
	./bin/replay_demo.sh --source $(SOURCE) $(ARGS)

chat: ## Ask the SRE copilot a question (M4)
	$(PYTHON) bin/05_copilot.py --source $(SOURCE) $(ARGS)

test: ## Run the test suite
	$(PYTHON) -m pytest -q
