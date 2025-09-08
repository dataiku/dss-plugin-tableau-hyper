package com.dataiku.dss.export.tableau;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.dataiku.dip.coremodel.Schema;
import com.dataiku.dip.coremodel.SchemaColumn;
import com.dataiku.dip.datalayer.Column;
import com.dataiku.dip.datalayer.ColumnFactory;
import com.dataiku.dip.datalayer.Row;
import com.dataiku.dip.datalayer.RowInputStream;
import com.dataiku.dip.datasets.Type;
import com.dataiku.dip.logging.LimitedLogContext;
import com.dataiku.dip.logging.LimitedLogFactory;
import com.dataiku.dip.plugin.CustomExporter;
import com.dataiku.dip.plugins.IPluginsRegistryService;
import com.dataiku.dip.server.SpringUtils;
import com.dataiku.dip.utils.DKULogger;
import com.dataiku.dip.utils.ExceptionUtils;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import com.google.gson.JsonObject;
import com.tableau.hyperapi.Connection;
import com.tableau.hyperapi.CreateMode;
import com.tableau.hyperapi.HyperProcess;
import com.tableau.hyperapi.Inserter;
import com.tableau.hyperapi.Nullability;
import com.tableau.hyperapi.SchemaName;
import com.tableau.hyperapi.SqlType;
import com.tableau.hyperapi.TableDefinition;
import com.tableau.hyperapi.TableName;
import com.tableau.hyperapi.Telemetry;

public class TableauExporter implements CustomExporter  {
    private static DKULogger logger = DKULogger.getLogger("dku.export.tableau");
    private static LimitedLogContext llc = LimitedLogFactory.get(logger, "dku.export.tableau.errors");

    private ColumnFactory cf;
    private Schema schema;
    private HyperProcess process;
    private Connection connection;
    private TableDefinition tableauTable;
    private TableDefinition tableauTempTable;
    private boolean isGeoTable = false;
    private List<Column> columns;
    private List<Type> types;


    private final DateTimeFormatter[] dateTimeNoTZFormatters = new DateTimeFormatter[]{
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    };

    private final DateTimeFormatter[] offsetDateTimeFormatters = new DateTimeFormatter[]{
            DateTimeFormatter.ISO_OFFSET_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXX")
    };

    public SqlType getTableauType(SchemaColumn dssColumn) {
        switch (dssColumn.getType()) {
        case STRING:
        case ARRAY:
        case MAP:
        case OBJECT:
            return SqlType.text();
        case DATE:
            return SqlType.timestampTz();
        case DATETIMENOTZ:
            return SqlType.timestamp();
        case DATEONLY:
            return SqlType.date();
        case GEOPOINT:
        case GEOMETRY:
            return SqlType.geography();
        case DOUBLE:
        case FLOAT:
            return SqlType.doublePrecision();
        case BOOLEAN:
            return SqlType.bool();
        case BIGINT:
            return SqlType.bigInt();
        case INT:
            return SqlType.integer();
        case SMALLINT:
        case TINYINT:
            return SqlType.smallInt();
        }
        throw new IllegalStateException("Unsupported DSS column type: " + dssColumn.getType());
    }

    @Override
    public void initialize(JsonObject config, JsonObject pluginSettings, Schema schema, ColumnFactory cf, File destinationFile) throws Exception {
        this.cf = cf;
        this.schema = schema;
        this.isGeoTable = schema.getColumns().stream().anyMatch(col -> col.getType() == Type.GEOPOINT || col.getType() == Type.GEOMETRY);

        Thread.currentThread().setContextClassLoader(TableauExporter.class.getClassLoader());

        String tableauTableName = config.get("table_name") == null ? "Extract" : config.get("table_name").getAsString();
        String tableauSchemaName = config.get("schema_name") == null ? "Extract" : config.get("schema_name").getAsString();

        this.tableauTable = new TableDefinition(
                new TableName(tableauSchemaName, tableauTableName),
                schema.getColumns().stream()
                        .map(dssColumn -> new TableDefinition.Column(dssColumn.getName(), getTableauType(dssColumn), Nullability.NULLABLE))
                        .collect(Collectors.toList())
        );

        if (this.isGeoTable) {
            List<TableDefinition.Column> tempColumns = schema.getColumns().stream()
                    .map(dssColumn -> {
                        SqlType type = (dssColumn.getType() == Type.GEOPOINT || dssColumn.getType() == Type.GEOMETRY) ? SqlType.text() : getTableauType(dssColumn);
                        return new TableDefinition.Column(dssColumn.getName(), type, Nullability.NULLABLE);
                    })
                    .collect(Collectors.toList());
            this.tableauTempTable = new TableDefinition(new TableName(tableauSchemaName, "tmp_" + tableauTableName), tempColumns);
        }

        Map<String, String> processParameters = new HashMap<>();
        processParameters.put("log_file_max_count", "2");
        processParameters.put("log_file_size_limit", "100M");
        processParameters.put("log_config", "");


        IPluginsRegistryService pluginsService = SpringUtils.getBean(IPluginsRegistryService.class);
        new File(pluginsService.getPluginJavalibFolder("tableau-hyper-export"), "hyper/hyperd").setExecutable(true);

        this.process = new HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "dataiku", processParameters);

        try {
            Map<String, String> connectionParameters = new HashMap<>();

            this.connection = new Connection(this.process.getEndpoint(),
                    destinationFile.getAbsolutePath(),
                    CreateMode.CREATE_AND_REPLACE,
                    connectionParameters);

            connection.getCatalog().createSchema(new SchemaName(tableauSchemaName));
            connection.getCatalog().createTable(this.tableauTable);
            if (this.isGeoTable) {
                connection.getCatalog().createTable(this.tableauTempTable);
            }
        } catch (Exception e) {
            if (this.process != null) {
                this.process.close();
                this.process = null;
            }
            throw e;
        }
    }

    public static <T> T parseWithMultipleFormatters(
            String input,
            DateTimeFormatter[] formatters,
            BiFunction<String, DateTimeFormatter, T> parser) {

        for (DateTimeFormatter formatter : formatters) {
            try {
                return parser.apply(input, formatter);
            } catch (DateTimeParseException e) {
                logger.debug("Failed to parse '" + input + "' with formatter " + formatter.toString() + ": " + e.getMessage());
            }
        }
        throw new IllegalArgumentException("Could not parse input: " + input);
    }

    @Override
    public void stream(RowInputStream stream) throws Exception {
        this.columns = new ArrayList<>();
        this.types = new ArrayList<>();
        for (SchemaColumn sc : this.schema.getColumns()) {
            this.columns.add(this.cf.getColumn(sc.getName()));
            this.types.add(sc.getType());
        }

        if (this.isGeoTable) {
            streamWithGeoProcessing(stream);
        } else {
            streamDirect(stream);
        }
    }

    private void streamDirect(RowInputStream stream) throws Exception {
        try (Inserter inserter = new Inserter(connection, this.tableauTable)) {
            insertRows(inserter, stream);
        }
    }

    private void streamWithGeoProcessing(RowInputStream stream) throws Exception {
        try (Inserter inserter = new Inserter(connection, this.tableauTempTable)) {
            insertRows(inserter, stream);
        }
        
        transferFromTempToFinalTable();
    }

    private void insertRows(Inserter inserter, RowInputStream stream) throws Exception {
        for (Row r; (r = stream.next()) != null; ) {
            addRowToInserter(inserter, r);
        }
        logger.info("Doing final execute");
        inserter.execute();
        logger.info("Done final execute");
    }

    private void transferFromTempToFinalTable() throws Exception {
        String sqlCommand = buildInsertFromTempTableSql();
        long rowsAffected = connection.executeCommand(sqlCommand).getAsLong();
        logger.info(rowsAffected + " rows transferred to the final table.");
    }

    private String buildInsertFromTempTableSql() {
        List<String> selectColumns = new ArrayList<>();
        
        for (SchemaColumn dssColumn : this.schema.getColumns()) {
            if (dssColumn.getType() == Type.GEOPOINT || dssColumn.getType() == Type.GEOMETRY) {
                selectColumns.add(String.format("CAST(\"%s\" AS TABLEAU.TABGEOGRAPHY)", dssColumn.getName()));
            } else {
                selectColumns.add(String.format("\"%s\"", dssColumn.getName()));
            }
        }
        
        return String.format("INSERT INTO %s SELECT %s FROM %s",
                this.tableauTable.getTableName(),
                String.join(", ", selectColumns),
                this.tableauTempTable.getTableName());
    }

    private void addRowToInserter(Inserter inserter, Row r) {
        for (int i = 0; i < columns.size(); i++) {
            String value = r.get(columns.get(i));
            if (value == null) {
                inserter.addNull();
            } else {
                try {
                    addValueToInserter(inserter, value, types.get(i));
                } catch (Exception e) {
                    llc.log("Failed to insert value in Tableau for col=" + columns.get(i).getName() + " v=" + value + " e=" +
                            ExceptionUtils.getMessageWithCauses(e));
                    inserter.addNull();
                }
            }
        }
        inserter.endRow();
    }

    public String convertGeoValueToLowercase(String value) {
        if (value == null) return null;
        return value.toLowerCase();
    }

    private void addValueToInserter(Inserter inserter, String value, Type type) {
        switch (type) {
            case GEOMETRY:
            case ARRAY:
            case MAP:
            case OBJECT:
            case STRING:
            case GEOPOINT:
                String geoValue = convertGeoValueToLowercase(value);
                inserter.add(geoValue);
                break;
            case DATE:
                OffsetDateTime offsetDateTime = parseWithMultipleFormatters(value, offsetDateTimeFormatters, OffsetDateTime::parse);
                inserter.add(offsetDateTime);
                break;
            case DATETIMENOTZ:
                LocalDateTime dateTime = parseWithMultipleFormatters(value, dateTimeNoTZFormatters, LocalDateTime::parse);
                inserter.add(dateTime);
                break;
            case DATEONLY:
                inserter.add(LocalDate.parse(value));
                break;
            case FLOAT:
            case DOUBLE:
                Double dv = Doubles.tryParse(value);
                if (dv != null) inserter.add(dv); else inserter.addNull();
                break;
            case BOOLEAN:
                inserter.add(Boolean.parseBoolean(value));
                break;
            case BIGINT:
                Long lvBig = Longs.tryParse(value);
                if (lvBig != null) inserter.add(lvBig); else inserter.addNull();
                break;
            case INT:
                Long lvInt = Longs.tryParse(value);
                if (lvInt != null) inserter.add(lvInt.intValue()); else inserter.addNull();
                break;
            case SMALLINT:
            case TINYINT:
                Long lvShort = Longs.tryParse(value);
                if (lvShort != null) inserter.add(lvShort.shortValue()); else inserter.addNull();
                break;
        }
    }

    @Override
    public long getWrittenBytes() throws Exception {
        return 0;
    }

    @Override
    public void close() {
        if (connection != null) {
            if (this.isGeoTable && this.tableauTempTable != null) {
                try {
                    connection.executeCommand("DROP TABLE " + this.tableauTempTable.getTableName());
                } catch (Exception e) {
                    logger.error("Failed to drop temporary geo table.", e);
                }
            }
            try {
                connection.close();
            } catch (Exception e) {
                logger.error("Failed to close connection.", e);
            }
        }
        if (process != null) {
            try {
                process.close();
            } catch (Exception e) {
                logger.error("Failed to close process.", e);
            }
        }
    }
}