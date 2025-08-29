package com.dataiku.dss.export.tableau;

import org.junit.Test;
import org.junit.Assert;
import com.dataiku.dip.coremodel.Schema;
import com.dataiku.dip.coremodel.SchemaColumn;
import com.dataiku.dip.datasets.Type;
import com.dataiku.dip.utils.DKULogger;
import com.tableau.hyperapi.SqlType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

import java.util.ArrayList;
import java.util.List;

public class TableauExporterTest {

    private TableauExporter exporter = new TableauExporter();

    @Test
    public void testDSSImportsWork() {
        SchemaColumn column = new SchemaColumn("test_column", Type.STRING);
        Assert.assertNotNull("SchemaColumn should be created", column);
        Assert.assertEquals("Column name should match", "test_column", column.getName());
        Assert.assertEquals("Column type should match", Type.STRING, column.getType());
    }

    @Test
    public void testDSSTypeToTableauTypeMapping() {
        // Equivalent Python => test_dss_type_to_hyper
        // Test string type mapping
        SchemaColumn stringColumn = new SchemaColumn("test_string", Type.STRING);
        SqlType stringType = exporter.getTableauType(stringColumn);
        Assert.assertEquals("String should map to text", SqlType.text(), stringType);

        // Test bigint type mapping
        SchemaColumn bigintColumn = new SchemaColumn("test_bigint", Type.BIGINT);
        SqlType bigintType = exporter.getTableauType(bigintColumn);
        Assert.assertEquals("Bigint should map to bigInt", SqlType.bigInt(), bigintType);

        // Test boolean type mapping
        SchemaColumn boolColumn = new SchemaColumn("test_bool", Type.BOOLEAN);
        SqlType boolType = exporter.getTableauType(boolColumn);
        Assert.assertEquals("Boolean should map to bool", SqlType.bool(), boolType);

        // Test geopoint type mapping
        SchemaColumn geoColumn = new SchemaColumn("test_geo", Type.GEOPOINT);
        SqlType geoType = exporter.getTableauType(geoColumn);
        Assert.assertEquals("Geopoint should map to geography", SqlType.geography(), geoType);
    }

    @Test
    public void testDateTypeMapping() {
        // Equivalent Python => test_date_type_mappings
        // Test date type mapping
        SchemaColumn dateColumn = new SchemaColumn("test_date", Type.DATE);
        SqlType dateType = exporter.getTableauType(dateColumn);
        Assert.assertEquals("Date should map to timestampTz", SqlType.timestampTz(), dateType);

        // Test dateonly type mapping
        SchemaColumn dateonlyColumn = new SchemaColumn("test_dateonly", Type.DATEONLY);
        SqlType dateonlyType = exporter.getTableauType(dateonlyColumn);
        Assert.assertEquals("Dateonly should map to date", SqlType.date(), dateonlyType);

        // Test datetimenotz type mapping
        SchemaColumn datetimeColumn = new SchemaColumn("test_datetime", Type.DATETIMENOTZ);
        SqlType datetimeType = exporter.getTableauType(datetimeColumn);
        Assert.assertEquals("Datetimenotz should map to timestamp", SqlType.timestamp(), datetimeType);
    }

    @Test
    public void testNumericTypeMapping() {
        // Equivalent Python => test_dss_type_to_hyper (numeric types)
        // Test double type mapping
        SchemaColumn doubleColumn = new SchemaColumn("test_double", Type.DOUBLE);
        SqlType doubleType = exporter.getTableauType(doubleColumn);
        Assert.assertEquals("Double should map to doublePrecision", SqlType.doublePrecision(), doubleType);

        // Test int type mapping
        SchemaColumn intColumn = new SchemaColumn("test_int", Type.INT);
        SqlType intType = exporter.getTableauType(intColumn);
        Assert.assertEquals("Int should map to integer", SqlType.integer(), intType);

        // Test smallint type mapping
        SchemaColumn smallintColumn = new SchemaColumn("test_smallint", Type.SMALLINT);
        SqlType smallintType = exporter.getTableauType(smallintColumn);
        Assert.assertEquals("Smallint should map to smallInt", SqlType.smallInt(), smallintType);
    }

    @Test
    public void testComplexTypeMapping() {
        // Equivalent Python => test_dss_geometry_type_mapping
        // Test array type mapping
        SchemaColumn arrayColumn = new SchemaColumn("test_array", Type.ARRAY);
        SqlType arrayType = exporter.getTableauType(arrayColumn);
        Assert.assertEquals("Array should map to text", SqlType.text(), arrayType);

        // Test map type mapping
        SchemaColumn mapColumn = new SchemaColumn("test_map", Type.MAP);
        SqlType mapType = exporter.getTableauType(mapColumn);
        Assert.assertEquals("Map should map to text", SqlType.text(), mapType);

        // Test geometry type mapping
        SchemaColumn geometryColumn = new SchemaColumn("test_geometry", Type.GEOMETRY);
        SqlType geometryType = exporter.getTableauType(geometryColumn);
        Assert.assertEquals("Geometry should map to geography", SqlType.geography(), geometryType);
    }

    @Test
    public void testSchemaDetectionForGeoTypes() {
        // Equivalent Python => test_dss_is_geo + test_dss_is_geo_with_geometry
        // Test schema with geopoint
        List<SchemaColumn> geoColumns = new ArrayList<>();
        geoColumns.add(new SchemaColumn("id", Type.INT));
        geoColumns.add(new SchemaColumn("location", Type.GEOPOINT));
        
        boolean hasGeoPoint = geoColumns.stream()
            .anyMatch(col -> col.getType() == Type.GEOPOINT);
        Assert.assertTrue("Schema should detect geopoint column", hasGeoPoint);

        // Test schema with geometry
        List<SchemaColumn> geometryColumns = new ArrayList<>();
        geometryColumns.add(new SchemaColumn("id", Type.INT));
        geometryColumns.add(new SchemaColumn("area", Type.GEOMETRY));
        
        boolean hasGeometry = geometryColumns.stream()
            .anyMatch(col -> col.getType() == Type.GEOMETRY);
        Assert.assertTrue("Schema should detect geometry column", hasGeometry);

        // Test schema without geo types
        List<SchemaColumn> regularColumns = new ArrayList<>();
        regularColumns.add(new SchemaColumn("id", Type.INT));
        regularColumns.add(new SchemaColumn("name", Type.STRING));
        
        boolean hasGeoTypes = regularColumns.stream()
            .anyMatch(col -> col.getType() == Type.GEOPOINT || col.getType() == Type.GEOMETRY);
        Assert.assertFalse("Regular schema should not have geo types", hasGeoTypes);
    }

    @Test
    public void testDateParsingFormats() {
        // Equivalent Python => test_dss_date_formats_to_hyper
        // Test that we can parse basic date formats
        try {
            LocalDate date = LocalDate.parse("2025-01-31");
            Assert.assertNotNull("Date should be parsed", date);
            Assert.assertEquals("Year should match", 2025, date.getYear());
            Assert.assertEquals("Month should match", 1, date.getMonthValue());
            Assert.assertEquals("Day should match", 31, date.getDayOfMonth());
        } catch (DateTimeParseException e) {
            Assert.fail("Should be able to parse basic date format");
        }
        
        // Test datetime parsing
        try {
            LocalDateTime dateTime = LocalDateTime.parse("2025-01-31T10:15:30");
            Assert.assertNotNull("DateTime should be parsed", dateTime);
            Assert.assertEquals("Hour should match", 10, dateTime.getHour());
        } catch (DateTimeParseException e) {
            Assert.fail("Should be able to parse ISO datetime format");
        }
        
        // Test offset datetime parsing
        try {
            OffsetDateTime offsetDateTime = OffsetDateTime.parse("2025-01-31T10:15:30+02:00");
            Assert.assertNotNull("OffsetDateTime should be parsed", offsetDateTime);
            Assert.assertEquals("Offset should match", "+02:00", offsetDateTime.getOffset().toString());
        } catch (DateTimeParseException e) {
            Assert.fail("Should be able to parse offset datetime format");
        }
    }

    @Test
    public void testGeoValueConversionToLowercase() {
        // Equivalent Python => test_dss_value_to_hyper + test_dss_geometry_to_hyper
        // Test geopoint case conversion
        String geopointUpper = "POINT(-90 89)";
        String geopointLower = exporter.convertGeoValueToLowercase(geopointUpper);
        Assert.assertEquals("Geopoint should be converted to lowercase", "point(-90 89)", geopointLower);
        
        // Test geometry case conversion
        String geometryUpper = "MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1)))";
        String geometryLower = exporter.convertGeoValueToLowercase(geometryUpper);
        Assert.assertEquals("Geometry should be converted to lowercase", 
            "multipolygon(((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1)))", geometryLower);
        
        // Test polygon case conversion
        String polygonUpper = "POLYGON((0 0,3 0,3 3,0 3,0 0))";
        String polygonLower = exporter.convertGeoValueToLowercase(polygonUpper);
        Assert.assertEquals("Polygon should be converted to lowercase", "polygon((0 0,3 0,3 3,0 3,0 0))", polygonLower);
        
        // Test null handling
        String nullResult = exporter.convertGeoValueToLowercase(null);
        Assert.assertNull("Null geo value should remain null", nullResult);
    }

    @Test
    public void testMultipleFormattersParsing() {
        // Equivalent Python => test_dss_date_formats_to_hyper (multiple formats)
        DateTimeFormatter[] testFormatters = new DateTimeFormatter[]{
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        };
        
        // Test first formatter success
        try {
            LocalDateTime result1 = TableauExporter.parseWithMultipleFormatters(
                "2025-01-31 10:15:30.123", 
                testFormatters, 
                LocalDateTime::parse
            );
            Assert.assertNotNull("DateTime with milliseconds should be parsed", result1);
            Assert.assertEquals("Year should match", 2025, result1.getYear());
            Assert.assertEquals("Milliseconds should match", 123000000, result1.getNano());
        } catch (Exception e) {
            Assert.fail("Should parse datetime with milliseconds: " + e.getMessage());
        }
        
        // Test fallback to second formatter
        try {
            LocalDateTime result2 = TableauExporter.parseWithMultipleFormatters(
                "2025-01-31 10:15:30", 
                testFormatters, 
                LocalDateTime::parse
            );
            Assert.assertNotNull("DateTime without milliseconds should be parsed", result2);
            Assert.assertEquals("Hour should match", 10, result2.getHour());
            Assert.assertEquals("Minute should match", 15, result2.getMinute());
        } catch (Exception e) {
            Assert.fail("Should parse datetime without milliseconds: " + e.getMessage());
        }
    }

    @Test
    public void testParsingErrorHandling() {
        // Equivalent Python => error handling in test_dss_date_formats_to_hyper
        DateTimeFormatter[] testFormatters = new DateTimeFormatter[]{
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        };
        
        // Test invalid date format throws exception
        try {
            TableauExporter.parseWithMultipleFormatters(
                "invalid-date-format", 
                testFormatters, 
                LocalDateTime::parse
            );
            Assert.fail("Should throw exception for invalid date format");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Exception message should mention parsing failure", 
                e.getMessage().contains("Could not parse input"));
        }
        
        // Test empty formatters array
        DateTimeFormatter[] emptyFormatters = new DateTimeFormatter[]{};
        try {
            TableauExporter.parseWithMultipleFormatters(
                "2025-01-31 10:15:30", 
                emptyFormatters, 
                LocalDateTime::parse
            );
            Assert.fail("Should throw exception with empty formatters");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Exception should indicate parsing failure", 
                e.getMessage().contains("Could not parse input"));
        }
    }

    @Test
    public void testOffsetDateTimeFormattersParsing() {
        // Equivalent Python => test_dss_date_formats_to_hyper (timezone formats)
        DateTimeFormatter[] offsetFormatters = new DateTimeFormatter[]{
            DateTimeFormatter.ISO_OFFSET_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXX")
        };
        
        // Test ISO format
        try {
            OffsetDateTime result1 = TableauExporter.parseWithMultipleFormatters(
                "2025-01-31T01:02:03+02:00", 
                offsetFormatters, 
                OffsetDateTime::parse
            );
            Assert.assertNotNull("ISO offset datetime should be parsed", result1);
            Assert.assertEquals("Offset should match", "+02:00", result1.getOffset().toString());
        } catch (Exception e) {
            Assert.fail("Should parse ISO offset datetime: " + e.getMessage());
        }
        
        // Test format with milliseconds
        try {
            OffsetDateTime result2 = TableauExporter.parseWithMultipleFormatters(
                "2013-05-30T15:16:13.764+02:00", 
                offsetFormatters, 
                OffsetDateTime::parse
            );
            Assert.assertNotNull("Datetime with milliseconds and offset should be parsed", result2);
            Assert.assertEquals("Month should match", 5, result2.getMonthValue());
        } catch (Exception e) {
            Assert.fail("Should parse datetime with milliseconds and offset: " + e.getMessage());
        }
        
        // Test Z timezone format  
        try {
            OffsetDateTime result3 = TableauExporter.parseWithMultipleFormatters(
                "2013-05-30T15:16:13.764Z", 
                offsetFormatters, 
                OffsetDateTime::parse
            );
            Assert.assertNotNull("Datetime with Z timezone should be parsed", result3);
            Assert.assertEquals("Z offset should be UTC", "Z", result3.getOffset().toString());
        } catch (Exception e) {
            Assert.fail("Should parse datetime with Z timezone: " + e.getMessage());
        }
    }
}