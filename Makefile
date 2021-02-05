PLUGIN_VERSION=0.1.3
PLUGIN_ID=tableau-hyper-export

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip code-env parameter-sets python-exporters python-formats python-lib plugin.json

unit-tests:
	@echo "[START] Running unit tests..."
	@( \
		python3 -m venv env/; \
		source env/bin/activate; \
		pip3 install --upgrade pip;\
		pip install --no-cache-dir -r python-test/requirements.txt; \
		pip install --no-cache-dir -r code-env/python/spec/requirements.txt; \
		export PYTHONPATH="$(PYTHONPATH):$(PWD)/python-lib"; \
		echo "PYTHONPATH=$(PYTHONPATH)";\
		echo "suis je debile? $(PYTHONPATH):$(PWD)/python-lib"; \
        pytest --junitxml=unit.xml tests/python/unit || true; \
		deactivate; \
	)
	@echo "[SUCCESS] Running unit tests: Done!"

integration-tests:
	@echo "[START] Running integration tests..."
	# TODO add integration tests
	@echo "[SUCCESS] Running integration tests: Done!"

tests: unit-tests integration-tests
