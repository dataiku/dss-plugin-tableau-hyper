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
        SchemaColumn column = new SchemaColumn("test_column", Type.STRING);
        Assert.assertNotNull("SchemaColumn should be created", column);
        Assert.assertEquals("Column name should match", "test_column", column.getName());
        Assert.assertEquals("Column type should match", Type.STRING, column.getType());
    }
}