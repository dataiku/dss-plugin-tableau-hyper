package com.dataiku.dss.export.tableau;

import org.junit.Test;
import org.junit.Assert;
import com.dataiku.dip.coremodel.Schema;
import com.dataiku.dip.coremodel.SchemaColumn;
import com.dataiku.dip.datasets.Type;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class TableauUploadExporterTest {

    @Test
    public void testTableauUploadExporterInstantiation() {
        TableauUploadExporter exporter = new TableauUploadExporter();
        Assert.assertNotNull("TableauUploadExporter should be instantiated", exporter);
    }
}