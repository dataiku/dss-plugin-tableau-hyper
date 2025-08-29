# Makefile variables set automatically
plugin_id=`cat plugin.json | python -c "import sys, json; print(str(json.load(sys.stdin)['id']).replace('/',''))"`
plugin_version=`cat plugin.json | python -c "import sys, json; print(str(json.load(sys.stdin)['version']).replace('/',''))"`
archive_file_name="dss-plugin-${plugin_id}-${plugin_version}.zip"
remote_url=`git config --get remote.origin.url`
last_commit_id=`git rev-parse HEAD`


HYPER_API_ZIP_URL := https://downloads.tableau.com/tssoftware//tableauhyperapi-java-linux-x86_64-release-main.0.0.22502.r99d1cc31.zip
HYPER_API_ZIP_FILE := tableauhyperapi.zip
HYPERD_TARGET_BINARY := java-lib/hyper/hyperd

$(HYPERD_TARGET_BINARY):
	@echo "[DEPENDENCY] Downloading Tableau Hyper API binary..."
	@mkdir -p $(dir $(HYPERD_TARGET_BINARY))
	@curl -L -o $(HYPER_API_ZIP_FILE) $(HYPER_API_ZIP_URL)
	@mkdir -p tmp_unzip
	@unzip -q $(HYPER_API_ZIP_FILE) -d tmp_unzip
	@mv tmp_unzip/*/lib/hyper/hyperd $(HYPERD_TARGET_BINARY)
	@chmod +x $(HYPERD_TARGET_BINARY)
	@rm $(HYPER_API_ZIP_FILE)
	@rm -rf tmp_unzip
	@echo "[DEPENDENCY] Tableau Hyper API binary is ready at $(HYPERD_TARGET_BINARY)"

download-deps: $(HYPERD_TARGET_BINARY)

build:
	@ant clean
	@ant

plugin: $(HYPERD_TARGET_BINARY) build
	@echo "[START] Archiving plugin to dist/ folder..."
	@rm -rf dist
	@mkdir dist
	@echo "{\"remote_url\":\"${remote_url}\",\"last_commit_id\":\"${last_commit_id}\"}" > release_info.json
	
	@zip -r -9 dist/$(archive_file_name) . -x ".git/*" "dist/*" "env/*" "tmp_unzip/*" "tests/*" "data/*" "*.zip" ".idea/*" "*.DS_Store" "Makefile" "build.xml" "Jenkinsfile" ".gitignore"
	
	@zip -g -j dist/$(archive_file_name) release_info.json
	
	@rm release_info.json
	@echo "[SUCCESS] Archiving plugin to dist/ folder: Done!"

unit-tests: java-unit-tests python-unit-tests
	@echo "[SUCCESS] All unit tests completed!"

java-unit-tests:
	@echo "[START] Running Java unit tests..."
	@mkdir -p tests/java-build
	@mkdir -p tests/allure_report
	@javac -cp "$$DKU_INSTALL_DIR/lib/ivy/backend-run/*:$$DKU_INSTALL_DIR/lib/ivy/common-run/*:$$DKU_INSTALL_DIR/lib/shadelib/*:$$DKU_INSTALL_DIR/dist/dataiku-core.jar:$$DKU_INSTALL_DIR/dist/dataiku-app-platform.jar:$$DKU_INSTALL_DIR/dist/dataiku-dss-core.jar:$$DKU_INSTALL_DIR/dist/dataiku-scoring.jar:$$DKU_INSTALL_DIR/dist/dataiku-dip.jar:java-lib/*" \
		-d tests/java-build \
		java-src/com/dataiku/dss/export/tableau/*.java tests/java/unit/*.java
	@java -cp "tests/java-build:$$DKU_INSTALL_DIR/lib/ivy/backend-run/*:$$DKU_INSTALL_DIR/lib/ivy/common-run/*:$$DKU_INSTALL_DIR/lib/shadelib/*:$$DKU_INSTALL_DIR/dist/dataiku-core.jar:$$DKU_INSTALL_DIR/dist/dataiku-app-platform.jar:$$DKU_INSTALL_DIR/dist/dataiku-dss-core.jar:$$DKU_INSTALL_DIR/dist/dataiku-scoring.jar:$$DKU_INSTALL_DIR/dist/dataiku-dip.jar:java-lib/*" \
		org.junit.runner.JUnitCore com.dataiku.dss.export.tableau.TableauExporterTest
	@echo "[SUCCESS] Java unit tests: Done!"

python-unit-tests:
	@echo "[START] Running Python unit tests..."
	@( \
		PYTHON_VERSION=`python3 -V 2>&1 | sed 's/[^0-9]*//g' | cut -c 1,2`; \
		PYTHON_VERSION_IS_CORRECT=`cat code-env/python/desc.json | python3 -c "import sys, json; print(str($$PYTHON_VERSION) in [x[-2:] for x in json.load(sys.stdin)['acceptedPythonInterpreters']]);"`; \
		if [ $$PYTHON_VERSION_IS_CORRECT == "False" ]; then echo "Python version $$PYTHON_VERSION is not in acceptedPythonInterpreters"; exit 1; else echo "Python version $$PYTHON_VERSION is in acceptedPythonInterpreters"; fi; \
	)
	@( \
		python3 -m venv env/; \
		source env/bin/activate; \
		pip3 install --upgrade pip; \
		pip install --no-cache-dir -r tests/python/unit/requirements.txt; \
		pip install --no-cache-dir -r code-env/python/spec/requirements.txt; \
		export PYTHONPATH="$(PYTHONPATH):$(PWD)/python-lib"; \
		pytest tests/python/unit --alluredir=tests/allure_report || ret=$$?; exit $$ret \
	)
	@echo "[SUCCESS] Python unit tests: Done!"

integration-tests:
	@echo "Running integration tests..."
	@( \
		rm -rf ./env/; \
		python3 -m venv env/; \
		source env/bin/activate; \
		pip3 install --upgrade pip;\
		pip install --no-cache-dir -r tests/python/integration/requirements.txt; \
		pytest tests/python/integration --alluredir=tests/allure_report || ret=$$?; exit $$ret \
	)

tests: unit-tests integration-tests

dist-clean:
	rm -rf dist

clean: dist-clean
	rm -rf java-lib/hyper