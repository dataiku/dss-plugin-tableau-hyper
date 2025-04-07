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
    CustomPythonExportersService customPythonExporterService;
    IPluginsRegistryService pluginsService;
    File pluginDir;
    JsonObject config;
    JsonObject pluginSettings;
    File destinationFile;

    AutoDelete tmpDir;
    @Override
    public void initialize(JsonObject config, JsonObject pluginSettings, Schema schema, ColumnFactory cf, File destinationFile) throws Exception {
        tmpDir = DSSTempUtils.getTempFolder("tableau-upload");

        String outputTableName = config.get("output_table") == null ? "DSS_extract" : config.get("output_table").getAsString();

        outputTableName = outputTableName.replaceAll("[^A-Za-z0-9_]*","");

        destinationFile = DKUFileUtils.getWithin(tmpDir, outputTableName + ".hyper");

        super.initialize(config, pluginSettings, schema, cf, destinationFile);

        customPythonExporterService = SpringUtils.getBean(CustomPythonExportersService.class);
        pluginsService = SpringUtils.getBean(IPluginsRegistryService.class);
        pluginDir = pluginsService.getActualPluginFolder("tableau-hyper-export");

        this.config = config;
        this.pluginSettings = pluginSettings;
        this.destinationFile = destinationFile;

    }
    @Override
    public void close() {
    }

    public void stream(RowInputStream stream) throws Exception {
        super.stream(stream);

        super.close();

        try {
            JsonObject uploaderConfig = new JsonObject();

            uploaderConfig.add("config", config);

            uploaderConfig.addProperty("hyperFilePath", destinationFile.getAbsolutePath());

            JSON.prettyToFile(uploaderConfig, DKUFileUtils.getWithin(tmpDir, "uploader_config.json"));

            String envName = new CodeEnvSelector().selectForCustomPythonRecipe("tableau-hyper-export");

            logger.info("envName= " + envName);

            ProcessBuilder pb = new ProcessBuilder();


            List<String> pyCmd =  SpringUtils.getBean(ICodeEnvResolutionService.class).
                    getPythonCmd(envName, "N/A", Lists.newArrayList(
                            DKUFileUtils.getWithin(pluginDir, "java-driven-uploader", "uploader.py").getAbsolutePath()
                    ));

            pb.command(pyCmd);

            KernelUtils.handlePythonAndRPath(null, null, false,
                    ImmutableMap.of("plugin-lib", pluginsService.getPluginPythonlibFolder("tableau-hyper-export").getAbsolutePath()),
                    tmpDir, false, pb);

            pb.directory(tmpDir);

            //Map<String, String> env = new HashMap<>();

            logger.info("Executing pyCmd: " + JSON.json(pyCmd));
            logger.info("ENV: " + JSON.json(pb.environment()));

            DKUtils.execAndLogThrows(pb);

            logger.info("Execution complete!");

        } catch (Exception e) {
            throw new RuntimeException("Failed to upload to Tableau", e);
        }

    }
    private static DKULogger logger = DKULogger.getLogger("dku.export.tableau.server");
}
