/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);
  private static final String SYNC_ACTOR_FIELD = "_sync_actor";
  private static final String SYNC_ACTOR_MONGODB = "mongodb";
  private static final String SYNC_ACTOR_POSTGRES = "postgres";
  private static final String FIELD_NAME_INSERTED_TS = "_insertedTS";
  private static final String FIELD_NAME_MODIFIED_TS = "_modifiedTS";
  private static final String _ID_FIELD = "_id";
  private static final String ID_FIELD = "id";

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  final CachedConnectionProvider cachedConnectionProvider;

  JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    this.cachedConnectionProvider = connectionProvider(
        config.connectionAttempts,
        config.connectionBackoffMs
    );
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
      @Override
      protected void onConnect(final Connection connection) throws SQLException {
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:JavaNCSS"})
  void write(final Collection<SinkRecord> records)
          throws SQLException, TableAlterOrCreateException {
    final Connection connection = cachedConnectionProvider.getConnection();
    String schemaName = getSchemaSafe(connection).orElse(null);
    String catalogName = getCatalogSafe(connection).orElse(null);
    try {
      final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
      for (SinkRecord record : records) {
        Schema recordValueSchema = record.valueSchema();
        Struct recordValue = (Struct) record.value();

        if (isSkip(recordValue)) {
          continue;
        }

        switch (record.topic()) {
          case "syain": {
            String childFieldInMongo = "syain_busyos";
            String childTableInPostgres = "syain_busyo";
            String foreignKeyInPostgres = "syain_id";
            handleCustomTopic(record, recordValueSchema, recordValue, schemaName,
                    catalogName, bufferByTable, connection, childFieldInMongo,
                    childTableInPostgres, foreignKeyInPostgres, null);
            break;
          }
          case "shift": {
            String childFieldInMongo = "tasks";
            String childTableInPostgres = "task";
            String primaryKeyChildTableInPostgres = "task_id";
            handleCustomTopic(record, recordValueSchema, recordValue, schemaName,
                    catalogName, bufferByTable, connection, childFieldInMongo,
                    childTableInPostgres, null, primaryKeyChildTableInPostgres);
            break;
          }
          case "demands": {
            String childFieldInMongo = "workschedules";
            String childTableInPostgres = "workschedule";
            handleCustomTopic(record, recordValueSchema, recordValue, schemaName,
                    catalogName, bufferByTable, connection, childFieldInMongo,
                    childTableInPostgres, null, null);
            break;
          }
          case "sagyo": {
            String childFieldInMongo = "sagyo_wokmodel";
            String childTableInPostgres = "sagyo_wokmodel";
            String foreignKeyInPostgres = "sagyo_id";
            handleCustomTopic(record, recordValueSchema, recordValue, schemaName,
                    catalogName, bufferByTable, connection, childFieldInMongo,
                    childTableInPostgres, foreignKeyInPostgres, null);
            break;
          }
          default: {
            handleDefaultTopic(record, recordValueSchema, recordValue,
                    schemaName, catalogName, bufferByTable, connection);
            break;
          }
        }
      }
      for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
        TableId tableId = entry.getKey();
        BufferedRecords buffer = entry.getValue();
        log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
        buffer.flush();
        buffer.close();
      }
      log.trace("Committing transaction");
      connection.commit();
    } catch (SQLException | TableAlterOrCreateException e) {
      log.error("Error during write operation. Attempting rollback.", e);
      try {
        connection.rollback();
        log.info("Successfully rolled back transaction");
      } catch (SQLException sqle) {
        log.error("Failed to rollback transaction", sqle);
        e.addSuppressed(sqle);
      } finally {
        throw e;
      }
    }
    log.info("Completed write operation for {} records to the database", records.size());
  }

  private void handleDefaultTopic(SinkRecord record,
                                  Schema recordValueSchema,
                                  Struct recordValue,
                                  String schemaName,
                                  String catalogName,
                                  Map<TableId, BufferedRecords> bufferByTable,
                                  Connection connection)
          throws SQLException {
    SinkRecord newRecord = getNewDefaultRecord(record, recordValueSchema, recordValue);
    addBufferByTable(newRecord, schemaName, catalogName, bufferByTable, connection);
  }

  @SuppressWarnings("ParameterNumber")
  private void handleCustomTopic(SinkRecord record,
                                 Schema recordValueSchema,
                                 Struct recordValue,
                                 String schemaName,
                                 String catalogName,
                                 Map<TableId, BufferedRecords> bufferByTable,
                                 Connection connection,
                                 String childFieldInMongo,
                                 String childTableInPostgres,
                                 String foreignKeyInPostgres,
                                 String primaryKeyChildTableInPostgres)
          throws SQLException {
    Field isHaveChildFieldInMongo = recordValueSchema.field(childFieldInMongo);
    if (isHaveChildFieldInMongo != null) {
      SinkRecord newRecord = getNewParentRecord(record, recordValueSchema, recordValue,
              childFieldInMongo);

      addBufferByTable(newRecord, schemaName, catalogName, bufferByTable, connection);

      List<SinkRecord> listChildRecord = getNewChildRecord(record, recordValueSchema,
              recordValue, childFieldInMongo, childTableInPostgres,
              foreignKeyInPostgres, primaryKeyChildTableInPostgres);

      for (SinkRecord childRecord : listChildRecord) {
        addBufferByTable(childRecord, schemaName, catalogName, bufferByTable, connection);
      }
    }
  }

  private boolean isSkip(Struct recordValue) {
    if (recordValue.schema().field(SYNC_ACTOR_FIELD) != null) {
      String syncActor = String.valueOf(recordValue.get(SYNC_ACTOR_FIELD));
      return SYNC_ACTOR_POSTGRES.equals(syncActor);
    }
    return false;
  }

  @SuppressWarnings("VariableDeclarationUsageDistance")
  private void addBufferByTable(SinkRecord newRecord,
                                String schemaName,
                                String catalogName,
                                Map<TableId, BufferedRecords> bufferByTable,
                                Connection connection)
          throws SQLException {
    final TableId tableId = destinationTable(newRecord.topic(), schemaName, catalogName);
    BufferedRecords buffer = bufferByTable.get(tableId);
    if (buffer == null) {
      JdbcSinkConfig tableConfig;

      if (tableId.tableName().equals("task")) {
        Map<String, String> configMap = new HashMap<>(config.originalsStrings());
        tableConfig = new JdbcSinkConfig(configMap);
        tableConfig.pkFields = Collections.singletonList("task_id");
      } else {
        tableConfig = config;
      }

      buffer = new BufferedRecords(tableConfig, tableId, dbDialect, dbStructure, connection);
      bufferByTable.put(tableId, buffer);
    }

    buffer.add(newRecord);
  }

  private SinkRecord getNewParentRecord(SinkRecord record,
                                        Schema oldValueSchema,
                                        Struct oldValue,
                                        String childFieldInMongo) {
    Set<String> excludedFields = new HashSet<>(
            Arrays.asList(childFieldInMongo, FIELD_NAME_MODIFIED_TS, FIELD_NAME_INSERTED_TS));

    // build ValueSchema
    Schema newValueSchema = buildNewValueSchema(oldValueSchema, excludedFields);

    // build Value
    Struct newValue = buildNewValue(newValueSchema, oldValueSchema, excludedFields, oldValue);

    return new SinkRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            newValueSchema,
            newValue,
            record.kafkaOffset(), record.timestamp(),
            record.timestampType(), record.headers());
  }

  private SinkRecord getNewDefaultRecord(SinkRecord record,
                                         Schema oldValueSchema,
                                         Struct oldValue) {
    Set<String> excludedFields = new HashSet<>(
            Arrays.asList(FIELD_NAME_MODIFIED_TS, FIELD_NAME_INSERTED_TS));

    // build ValueSchema
    Schema newValueSchema = buildNewValueSchema(oldValueSchema, excludedFields);

    // build Value
    Struct newValue = buildNewValue(newValueSchema, oldValueSchema, excludedFields, oldValue);

    return new SinkRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            newValueSchema,
            newValue,
            record.kafkaOffset(), record.timestamp(),
            record.timestampType(), record.headers());
  }

  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
  private List<SinkRecord> getNewChildRecord(SinkRecord record,
                                             Schema oldValueSchema,
                                             Struct oldValue,
                                             String childFieldInMongo,
                                             String childTableInPostgres,
                                             String foreignKeyInPostgres,
                                             String primaryKeyChildTableInPostgres) {
    List<SinkRecord> listChildRecord = new ArrayList<>();

    Struct child1Value = ((Struct) oldValue.get(childFieldInMongo));

    Set<String> excludedFields = new HashSet<>(
            Arrays.asList(FIELD_NAME_MODIFIED_TS, FIELD_NAME_INSERTED_TS));

    child1Value.schema().schema().fields().forEach(field -> {
      Schema child2ValueSchema = field.schema();
      Struct child2Value = (Struct) child1Value.get(field.name());

      // build KeySchema
      Schema child3KeySchema;
      if (primaryKeyChildTableInPostgres != null) {
        child3KeySchema = SchemaBuilder.struct()
                .field(primaryKeyChildTableInPostgres,
                        new SchemaBuilder(Schema.Type.STRING).build())
                .build();
      } else {
        child3KeySchema = record.keySchema();
      }

      // build Key
      Struct child3Key = new Struct(child3KeySchema);
      if (primaryKeyChildTableInPostgres != null) {
        child3Key.put(primaryKeyChildTableInPostgres,
                child2Value.get(child2ValueSchema.field(primaryKeyChildTableInPostgres)));
      } else {
        child3Key.put(ID_FIELD, child2Value.get(child2ValueSchema.field(_ID_FIELD)));
      }

      // build ValueSchema
      SchemaBuilder child3ValueSchemaBuilder = SchemaBuilder.struct();
      for (Field field2 : child2ValueSchema.fields()) {
        if (excludedFields.contains(field.name())) {
          continue;
        }

        String fieldName = _ID_FIELD.equals(field2.name()) ? ID_FIELD : field2.name();
        child3ValueSchemaBuilder.field(fieldName, field2.schema());
      }

      if (foreignKeyInPostgres != null) {
        child3ValueSchemaBuilder.field(foreignKeyInPostgres,
                oldValueSchema.field(ID_FIELD).schema());
      }
      if (child2ValueSchema.field(SYNC_ACTOR_FIELD) == null) {
        child3ValueSchemaBuilder.field(
                SYNC_ACTOR_FIELD, new SchemaBuilder(Schema.Type.STRING).build());
      }
      Schema child3ValueSchema = child3ValueSchemaBuilder.build();

      // build Value
      Struct child3Value = new Struct(child3ValueSchema);
      for (Field field2 : child2ValueSchema.fields()) {
        if (excludedFields.contains(field.name())) {
          continue;
        }

        String fieldName = field2.name();
        if (_ID_FIELD.equals(fieldName)) {
          child3Value.put(ID_FIELD, child2Value.get(field2));
        } else if (SYNC_ACTOR_FIELD.equals(fieldName)) {
          child3Value.put(fieldName, SYNC_ACTOR_MONGODB);
        } else {
          child3Value.put(fieldName, child2Value.get(field2));
        }
      }
      if (foreignKeyInPostgres != null) {
        child3Value.put(foreignKeyInPostgres, oldValue.get(ID_FIELD));
      }
      if (child2ValueSchema.field(SYNC_ACTOR_FIELD) == null) {
        child3Value.put(SYNC_ACTOR_FIELD, SYNC_ACTOR_MONGODB);
      }

      SinkRecord childRecord = new SinkRecord(
              childTableInPostgres,
              record.kafkaPartition(),
              child3KeySchema,
              child3Key,
              child3ValueSchema,
              child3Value,
              record.kafkaOffset(), record.timestamp(),
              record.timestampType(), record.headers());

      listChildRecord.add(childRecord);
    });

    return listChildRecord;
  }

  private Struct buildNewValue(Schema newValueSchema,
                               Schema oldValueSchema,
                               Set<String> excludedFields,
                               Struct oldValue) {
    Struct newValue = new Struct(newValueSchema);
    for (Field field : oldValueSchema.fields()) {
      if (excludedFields.contains(field.name())) {
        continue;
      }

      if (SYNC_ACTOR_FIELD.equals(field.name())) {
        newValue.put(field.name(), SYNC_ACTOR_MONGODB);
      } else {
        newValue.put(field.name(), oldValue.get(field));
      }
    }

    if (oldValueSchema.field(SYNC_ACTOR_FIELD) == null) {
      newValue.put(SYNC_ACTOR_FIELD, SYNC_ACTOR_MONGODB);
    }
    return newValue;
  }

  private Schema buildNewValueSchema(Schema oldValueSchema,
                                     Set<String> excludedFields) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (Field field : oldValueSchema.fields()) {
      if (excludedFields.contains(field.name())) {
        continue;
      }
      schemaBuilder.field(field.name(), field.schema());
    }
    if (oldValueSchema.field(SYNC_ACTOR_FIELD) == null) {
      schemaBuilder.field(SYNC_ACTOR_FIELD, new SchemaBuilder(Schema.Type.STRING).build());
    }
    return schemaBuilder.build();
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
  }

  TableId destinationTable(String topic, String schemaName, String catalogName) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
          "Destination table name for topic '%s' is empty using the format string '%s'",
          topic,
          config.tableNameFormat
      ));
    }
    TableId parsedTableId = dbDialect.parseTableIdentifier(tableName);
    String finalCatalogName =
            (parsedTableId.catalogName() != null) ? parsedTableId.catalogName() : catalogName;
    String finalSchemaName =
            (parsedTableId.schemaName() != null) ? parsedTableId.schemaName() : schemaName;


    return new TableId(finalCatalogName, finalSchemaName, parsedTableId.tableName());
  }

  private Optional<String> getSchemaSafe(Connection connection) {
    try {
      return Optional.ofNullable(connection.getSchema());
    } catch (AbstractMethodError | SQLException e) {
      log.warn("Failed to get schema: {}", e.getMessage());
      return Optional.empty();
    }
  }

  private Optional<String> getCatalogSafe(Connection connection) {
    try {
      return Optional.ofNullable(connection.getCatalog());
    } catch (AbstractMethodError | SQLException e) {
      log.warn("Failed to get catalog: {}", e.getMessage());
      return Optional.empty();
    }
  }
}
