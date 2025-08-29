package com.dataiku.dss.export.tableau;

import org.junit.Test;
import org.junit.Assert;
import com.dataiku.dip.coremodel.Schema;
import com.dataiku.dip.coremodel.SchemaColumn;
import com.dataiku.dip.datasets.Type;
import com.dataiku.dip.utils.DKULogger;
import com.tableau.hyperapi.SqlType;

import java.util.ArrayList;
import java.util.List;

/**
 * Testing that we can test the java testing :)
 */
public class TableauExporterTest {

    @Test
    public void testDSSImportsWork() {
        // Test that we can create DSS objects without errors
        List<SchemaColumn> columns = new ArrayList<>();
        SchemaColumn column = new SchemaColumn("test_column", Type.STRING);
        columns.add(column);
        
        Schema schema = new Schema(columns);
        Assert.assertNotNull("Schema should be created", schema);
        Assert.assertEquals("Should have one column", 1, schema.getColumns().size());
        Assert.assertEquals("Column name should match", "test_column", schema.getColumns().get(0).getName());
    }
}