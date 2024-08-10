DEFAULT_PYTHON_VERSION = 3.12.2
DIRS = $(shell ls -d tests/multi_repo/* )

dev:
	brew install duckdb; \
	for dir in $(DIRS); do \
		cd $$dir; \
			if [ -f .python-version ]; then \
				PYTHON_VERSION=`cat .python-version`; \
			else \
				PYTHON_VERSION=$(DEFAULT_PYTHON_VERSION); \
				printf "$(DEFAULT_PYTHON_VERSION)" > .python-version; \
			fi; \
			pyenv local $$PYTHON_VERSION; \
			poetry env use $$PYTHON_VERSION; \
			poetry install --no-root; \
			poetry run dbt deps; \
			poetry run dbt build; \
			rm -rf dbt_packages/dbt_utils/tests; \
			cd ../../../; \
	done

clean:
	for dir in $(DIRS); do \
		rm -rf $$dir/*.duckdb; \
		rm -rf $$dir/target; \
		rm -rf $$dir/.venv; \
		rm -rf $$dir/poetry.lock; \
	done
