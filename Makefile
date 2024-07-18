DEFAULT_PYTHON_VERSION = 3.12.2

dev:
	brew install duckdb; \
	cd tests/multi_repo/jaffle_shop; \
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
		cd ..; \

clean:
	rm -rf tests/multi_repo/jaffle_shop/jaffle_shop.duckdb; \
	rm -rf tests/multi_repo/jaffle_shop/.venv; \
	rm -rf tests/multi_repo/jaffle_shop/poetry.lock;
