package com.scibrazeau.dbunitreporter;

import com.google.cloud.bigquery.*;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.ArrayList;

public class SchemaUpdates {
    private final BigQuery bq;
    private final String tableName;
    private final String dbName;

    /* package */ SchemaUpdates(BigQuery bq, String dbName, String tableName) {
        this.bq = bq;
        this.tableName = tableName;
        this.dbName = dbName;
    }

    private static boolean hasField(Schema existingSchema, String fieldName) {
        return existingSchema.getFields().stream().anyMatch(f -> f.getName().equals(fieldName));
    }

    public void synchSchema(Schema officialSchema) {
        var existingTable = this.bq.getTable(dbName, tableName);
        var existingSchema = existingTable.getDefinition().getSchema();
        Preconditions.checkNotNull(existingSchema);
        // Create a new schema adding the current fields, plus any new ones
        var fieldList = new ArrayList<>(existingSchema.getFields());
        var postStatements = new ArrayList<QueryJobConfiguration>();
        var doUpdate = false;
        for (Field officialField : officialSchema.getFields()) {
            if (!hasField(existingSchema, officialField.getName())) {
                doUpdate = true;
                if (StringUtils.isEmpty(officialField.getDefaultValueExpression())) {
                    fieldList.add(officialField);
                } else {
                    var newField = officialField.toBuilder()
                            .setDefaultValueExpression(null)
                            .build();
                    fieldList.add(newField);
                    postStatements.add(
                            QueryJobConfiguration.of(String.format(
                                    "ALTER TABLE %s.%s ALTER COLUMN %s SET DEFAULT %s",
                                    dbName, tableName,
                                    officialField.getName(),
                                    officialField.getDefaultValueExpression()
                            ))
                    );
                }
            }
        }
        if (!doUpdate) {
            return;
        }
        Schema newSchema = Schema.of(fieldList);
        Table updatedTable = existingTable.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build();
        updatedTable.update();
        postStatements.forEach(s -> {
            try {
                this.bq.query(s);
            } catch (InterruptedException e) {
                ExceptionUtils.rethrow(e);
            }
        });
    }
}
