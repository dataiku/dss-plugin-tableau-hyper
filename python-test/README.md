# Test procedure

Running the tests in console using pytest.

Add the following into the Makefile:
```bash
unit-tests:
	@echo "[START] Running unit tests..."
	@( \
		python3 -m venv env/; \
		source env/bin/activate; \
		pip3 install --upgrade pip;\
		pip install --no-cache-dir -r tests/python/requirements.txt; \
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
```

## Testing scope

Index of the tests:
- test_null_values.py
- test_schema_conversion.py
- test_type_conversion.py

test_null_values.py
- test the export to hyper on a file with lot of nan values
- run analysis on the output file with the standard API of the Hyper file

test_geopoints.py
