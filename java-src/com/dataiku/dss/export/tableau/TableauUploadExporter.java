package com.dataiku.dss.export.tableau;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dataiku.dip.DSSTempUtils;
import com.dataiku.dip.code.CodeEnvSelector;
import com.dataiku.dip.code.ICodeEnvResolutionService;
import com.dataiku.dip.coremodel.Schema;
import com.dataiku.dip.coremodel.SchemaColumn;
import com.dataiku.dip.datalayer.Column;
import com.dataiku.dip.datalayer.ColumnFactory;
import com.dataiku.dip.datalayer.Row;
import com.dataiku.dip.datalayer.RowInputStream;
import com.dataiku.dip.datasets.Type;
import com.dataiku.dip.export.CustomPythonExportersService;
import com.dataiku.dip.io.KernelUtils;
import com.dataiku.dip.plugin.CustomExporter;
import com.dataiku.dip.plugins.IPluginsRegistryService;
import com.dataiku.dip.server.SpringUtils;
import com.dataiku.dip.util.AutoDelete;
import com.dataiku.dip.utils.DKUFileUtils;
import com.dataiku.dip.utils.DKULogger;
import com.dataiku.dip.utils.DKUtils;
import com.dataiku.dip.utils.DKUtils.ByteCollectingSubscription;
import com.dataiku.dip.utils.DKUtils.ExecBuilder;
import com.dataiku.dip.utils.DKUtils.LoggingLineSubscription;
import com.dataiku.dip.utils.DKUtils.SimpleExceptionExecCompletionHandler;
import com.dataiku.dip.utils.JSON;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import com.google.gson.JsonObject;
import com.tableau.hyperapi.Connection;
import com.tableau.hyperapi.CreateMode;
import com.tableau.hyperapi.HyperProcess;
import com.tableau.hyperapi.Inserter;
import com.tableau.hyperapi.Nullability;
import com.tableau.hyperapi.SqlType;
import com.tableau.hyperapi.TableDefinition;
import com.tableau.hyperapi.TableName;
import com.tableau.hyperapi.Telemetry;

public class TableauUploadExporter extends TableauExporter  {
    private static final DKULogger logger = DKULogger.getLogger("dku.export.tableau.server");
    
    private CustomPythonExportersService customPythonExporterService;
    private IPluginsRegistryService pluginsService;
    private ICodeEnvResolutionService codeEnvService;
    private File pluginDir;
    private JsonObject config;
    private JsonObject pluginSettings;
    private File destinationFile;
    private AutoDelete tmpDir;
    private String envName;

    @Override
    public void initialize(JsonObject config, JsonObject pluginSettings, Schema schema, ColumnFactory cf, File destinationFile) throws Exception {
        tmpDir = DSSTempUtils.getTempFolder("tableau-upload");

        String outputTableName = config.get("output_table") == null ? "DSS_extract" : config.get("output_table").getAsString();
        outputTableName = outputTableName.replaceAll("[^A-Za-z0-9_]*","");
        destinationFile = DKUFileUtils.getWithin(tmpDir, outputTableName + ".hyper");

        super.initialize(config, pluginSettings, schema, cf, destinationFile);

        initializeServices();
        
        this.config = config;
        this.pluginSettings = pluginSettings;
        this.destinationFile = destinationFile;
        this.envName = new CodeEnvSelector().selectForCustomPythonRecipe("tableau-hyper-export");
    }

    private void initializeServices() throws Exception {
        try {
            customPythonExporterService = SpringUtils.getBean(CustomPythonExportersService.class);
            pluginsService = SpringUtils.getBean(IPluginsRegistryService.class);
            codeEnvService = SpringUtils.getBean(ICodeEnvResolutionService.class);
            pluginDir = pluginsService.getActualPluginFolder("tableau-hyper-export");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize required services", e);
        }
    }

    @Override
    public void close() {
        if (tmpDir != null) {
            try {
                tmpDir.close();
            } catch (Exception e) {
                logger.warn("Failed to cleanup temporary directory", e);
            }
        }
    }

    public void stream(RowInputStream stream) throws Exception {
        try {
            processDataStream(stream);
            createUploaderConfig();
            executePythonUploader();
            logger.info("Upload to Tableau completed successfully");
        } catch (IOException e) {
            throw new RuntimeException("IO error during Tableau upload", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Upload process was interrupted", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload to Tableau: " + e.getMessage(), e);
        }
    }

    private void processDataStream(RowInputStream stream) throws Exception {
        super.stream(stream);
        super.close();
    }

    private void createUploaderConfig() throws IOException {
        JsonObject uploaderConfig = new JsonObject();
        uploaderConfig.add("config", config);
        uploaderConfig.addProperty("hyperFilePath", destinationFile.getAbsolutePath());
        JSON.prettyToFile(uploaderConfig, DKUFileUtils.getWithin(tmpDir, "uploader_config.json"));
    }

    private void executePythonUploader() throws Exception {
        ProcessBuilder pb = createProcessBuilder();
        logger.info("Executing Python uploader");
        logProcessInfo(pb);
        DKUtils.execAndLogThrows(pb);
    }

    private ProcessBuilder createProcessBuilder() throws IOException {
        List<String> pyCmd = codeEnvService.getPythonCmd(envName, "N/A", Lists.newArrayList(
                DKUFileUtils.getWithin(pluginDir, "java-driven-uploader", "uploader.py").getAbsolutePath()
        ));

        ProcessBuilder pb = new ProcessBuilder();
        pb.command(pyCmd);
        pb.directory(tmpDir);

        KernelUtils.handlePythonAndRPath(null, null, false,
                ImmutableMap.of("plugin-lib", pluginsService.getPluginPythonlibFolder("tableau-hyper-export").getAbsolutePath()),
                tmpDir, false, pb);

        return pb;
    }

    private void logProcessInfo(ProcessBuilder pb) {
        logger.info("Python command: " + pb.command().get(0) + " <script>");
        logger.info("Working directory: " + pb.directory().getAbsolutePath());
        logger.info("Environment name: " + envName);
    }
}