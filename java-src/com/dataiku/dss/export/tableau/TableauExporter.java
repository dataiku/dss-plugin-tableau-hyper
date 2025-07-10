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
import com.tableau.hyperapi.impl.SharedLibraryProvider;
import java.util.ServiceLoader;

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

    private SqlType getTableauType(SchemaColumn dssColumn) {
        switch (dssColumn.getType()) {
        case STRING:
        case GEOMETRY:
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
        throw new Error("unreachable");
    }

    @Override
    public void initialize(JsonObject config, JsonObject pluginSettings, Schema schema, ColumnFactory cf, File destinationFile) throws Exception {
        this.cf = cf;
        this.schema = schema;
        this.isGeoTable = schema.getColumns().stream().anyMatch(col -> col.getType() == Type.GEOPOINT);
        this.columns = new ArrayList<>();
        this.types = new ArrayList<>();
        for (SchemaColumn sc : schema.getColumns()) {
            this.columns.add(cf.getColumn(sc.getName()));
            this.types.add(sc.getType());
        }

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
                        SqlType type = (dssColumn.getType() == Type.GEOPOINT) ? SqlType.text() : getTableauType(dssColumn);
                        return new TableDefinition.Column(dssColumn.getName(), type, Nullability.NULLABLE);
                    })
                    .collect(Collectors.toList());
            this.tableauTempTable = new TableDefinition(new TableName(tableauSchemaName, "tmp_" + tableauTableName), tempColumns);
        }

        Map<String, String> processParameters = new HashMap<>();
        processParameters.put("log_file_max_count", "2");
        processParameters.put("log_file_size_limit", "100M");
        processParameters.put("log_config", "");

        process = new HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "dataiku", processParameters);

        Map<String, String> connectionParameters = new HashMap<>();

        connection = new Connection(process.getEndpoint(),
                destinationFile.getAbsolutePath(),
                CreateMode.CREATE_AND_REPLACE,
                connectionParameters);

        connection.getCatalog().createSchema(new SchemaName(tableauSchemaName));
        connection.getCatalog().createTable(this.tableauTable);
        if (this.isGeoTable) {
            connection.getCatalog().createTable(this.tableauTempTable);
        }
    }

    private static <T> T parseWithMultipleFormatters(
            String input,
            DateTimeFormatter[] formatters,
            BiFunction<String, DateTimeFormatter, T> parser) {

        for (DateTimeFormatter formatter : formatters) {
            try {
                return parser.apply(input, formatter);
            } catch (DateTimeParseException e) {
                // Continue trying
            }
        }
        throw new IllegalArgumentException("Could not parse input: " + input);
    }

    @Override
    public void stream(RowInputStream stream) throws Exception {
        TableDefinition targetTableForInserter = this.isGeoTable ? this.tableauTempTable : this.tableauTable;

        try (Inserter inserter = new Inserter(connection, targetTableForInserter)) {
            for (Row r; (r = stream.next()) != null; ) {
                for (int colIdx = 0; colIdx < columns.size(); colIdx++) {
                    String v = r.get(columns.get(colIdx));
                    if (v == null) {
                        inserter.addNull();
                    } else {
                        try {
                            switch (types.get(colIdx)) {
                            case GEOMETRY:
                            case ARRAY:
                            case MAP:
                            case OBJECT:
                            case STRING:
                            case GEOPOINT: {
                                inserter.add(v);
                                break;
                            }
                            case DATE: {
                                inserter.add(OffsetDateTime.parse(v));
                                break;
                            }
                            case DATETIMENOTZ: {
                                inserter.add(parseWithMultipleFormatters(v,dateTimeNoTZFormatters, LocalDateTime::parse));
                                break;
                            }
                            case DATEONLY: {
                                inserter.add(LocalDate.parse(v));
                                break;
                            }
                            case FLOAT:
                            case DOUBLE: {
                                Double dv = Doubles.tryParse(v);
                                if (dv == null) {
                                    inserter.addNull();
                                } else {
                                    inserter.add(dv);
                                }
                                break;
                            }
                            case BOOLEAN: {
                                inserter.add(Boolean.parseBoolean(v));
                                break;
                            }
                            case BIGINT: {
                                Long lv = Longs.tryParse(v);
                                if (lv == null) {
                                    inserter.addNull();
                                } else {
                                    inserter.add(lv);
                                }
                                break;
                            }
                            case INT: {
                                Long lv = Longs.tryParse(v);
                                if (lv == null) {
                                    inserter.addNull();
                                } else {
                                    inserter.add(lv.intValue());
                                }
                                break;
                            }
                            case SMALLINT:
                            case TINYINT: {
                                Long lv = Longs.tryParse(v);
                                if (lv == null) {
                                    inserter.addNull();
                                } else {
                                    inserter.add(lv.shortValue());
                                }
                                break;
                            }
                            }
                        } catch (Exception e) {
                            llc.log("Failed to insert value in Tableau for col=" + columns.get(colIdx).getName() + " v=" + v + " e=" +
                                    ExceptionUtils.getMessageWithCauses(e));
                            inserter.addNull();
                        }
                    }
                }
                inserter.endRow();
            }
            logger.info("Doing final execute");
            inserter.execute();
            logger.info("Done final execute");
        }

        if (this.isGeoTable) {
            String sqlCommand = String.format("INSERT INTO %s SELECT * FROM %s",
                    this.tableauTable.getTableName(),
                    this.tableauTempTable.getTableName());
        long rowsAffected = connection.executeCommand(sqlCommand).getAsLong();
            logger.info(rowsAffected + " rows transferred to the final table.");
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
            connection.close();
        }
        if (process != null) {
            process.close();
        }
    }
}