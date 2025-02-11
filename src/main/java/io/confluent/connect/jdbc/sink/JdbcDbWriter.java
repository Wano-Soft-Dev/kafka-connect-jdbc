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
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
                    childTableInPostgres, foreignKeyInPostgres, null, null);
            break;
          }
          case "shift": {
            String childFieldInMongo = "tasks";
            String childTableInPostgres = "task";
            String primaryKeyChildTableInPostgres = "task_id";
            handleCustomTopic(record, recordValueSchema, recordValue, schemaName,
                    catalogName, bufferByTable, connection, childFieldInMongo,
                    childTableInPostgres, null, null, primaryKeyChildTableInPostgres);
            break;
          }
          case "demands": {
            String childFieldInMongo = "workschedules";
            String childTableInPostgres = "workschedule";
            String primaryKeyParentTableInPostgres = "demand_id";
            String primaryKeyChildTableInPostgres = "workschedule_id";
            handleCustomTopic(record, recordValueSchema, recordValue, schemaName,
                    catalogName, bufferByTable, connection, childFieldInMongo,
                    childTableInPostgres, null, primaryKeyParentTableInPostgres,
                    primaryKeyChildTableInPostgres);
            break;
          }
          case "sagyo": {
            List<HashMap<String, String>> listFieldInChildRecord = new ArrayList<>();

            listFieldInChildRecord.add(createFieldInChildRecord(
                    "sagyo_wokmodel",
                    "sagyo_wokmodel",
                    "sagyo_id",
                    null));
            listFieldInChildRecord.add(createFieldInChildRecord(
                    "sagyobunrui_m",
                    "sagyobunrui_sagyo",
                    "sagyo_id",
                    null));

            handleCustomTopicForChildRecord(record, recordValueSchema, recordValue, schemaName,
                    catalogName, bufferByTable, connection, listFieldInChildRecord);
            break;
          }

          case "class": {
            String childFieldInMongo = "lower_classes";
            String childTableInPostgres = "class_tree";
            handleCustomTopic(record, recordValueSchema, recordValue, schemaName,
                    catalogName, bufferByTable, connection, childFieldInMongo,
                    childTableInPostgres, null, null, null);
            break;
          }
          case "class_group": {
            String childFieldInMongo = "lower_classgroups";
            String childTableInPostgres = "classgroup_rel";
            handleCustomTopic(record, recordValueSchema, recordValue, schemaName,
                    catalogName, bufferByTable, connection, childFieldInMongo,
                    childTableInPostgres, null, null, null);
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

  private HashMap<String, String> createFieldInChildRecord(String childFieldInMongo,
                                                           String childTableInPostgres,
                                                           String foreignKeyInPostgres,
                                                           String primaryKeyChildTableInPostgres) {
    HashMap<String, String> fieldInChildRecord = new HashMap<>();
    fieldInChildRecord.put("childFieldInMongo", childFieldInMongo);
    fieldInChildRecord.put("childTableInPostgres", childTableInPostgres);
    fieldInChildRecord.put("foreignKeyInPostgres", foreignKeyInPostgres);
    fieldInChildRecord.put("primaryKeyChildTableInPostgres", primaryKeyChildTableInPostgres);
    return fieldInChildRecord;
  }

  private void handleDefaultTopic(SinkRecord record,
                                  Schema recordValueSchema,
                                  Struct recordValue,
                                  String schemaName,
                                  String catalogName,
                                  Map<TableId, BufferedRecords> bufferByTable,
                                  Connection connection)
          throws SQLException {
    SinkRecord newRecord = getNewDefaultRecord(record, recordValueSchema,
            recordValue);
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
                                 String primaryKeyParentTableInPostgres,
                                 String primaryKeyChildTableInPostgres)
          throws SQLException {
    Field isHaveChildFieldInMongo = recordValueSchema.field(childFieldInMongo);
    if (isHaveChildFieldInMongo != null) {
      List<String> listChildFieldInMongo = Collections.singletonList(childFieldInMongo);

      SinkRecord newRecord = getNewParentRecord(record, recordValueSchema, recordValue,
              listChildFieldInMongo, primaryKeyParentTableInPostgres);

      addBufferByTable(newRecord, schemaName, catalogName, bufferByTable, connection);

      List<SinkRecord> listChildRecord = getNewChildRecord(record, recordValueSchema,
              recordValue, childFieldInMongo, childTableInPostgres,
              foreignKeyInPostgres, primaryKeyChildTableInPostgres);

      for (SinkRecord childRecord : listChildRecord) {
        addBufferByTable(childRecord, schemaName, catalogName, bufferByTable, connection);
      }
    }
  }

  private void handleCustomTopicForChildRecord(
          SinkRecord record,
          Schema recordValueSchema,
          Struct recordValue,
          String schemaName,
          String catalogName,
          Map<TableId, BufferedRecords> bufferByTable,
          Connection connection,
          List<HashMap<String, String>> listFieldInChildRecord)
          throws SQLException {
    List<String> listChildFieldInMongo = listFieldInChildRecord.stream()
            .map(fieldInChildRecord -> fieldInChildRecord.get("childFieldInMongo"))
            .filter(Objects::nonNull)
            .distinct()
            .collect(Collectors.toList());

    SinkRecord newRecord = getNewParentRecord(record, recordValueSchema, recordValue,
            listChildFieldInMongo, null);

    addBufferByTable(newRecord, schemaName, catalogName, bufferByTable, connection);

    for (HashMap<String, String> fieldInChildRecord : listFieldInChildRecord) {
      String childFieldInMongo = fieldInChildRecord.get("childFieldInMongo");
      String childTableInPostgres = fieldInChildRecord.get("childTableInPostgres");
      String foreignKeyInPostgres = fieldInChildRecord.get("foreignKeyInPostgres");
      String primaryKeyChildTableInPostgres = fieldInChildRecord.get(
              "primaryKeyChildTableInPostgres");

      Field isHaveChildFieldInMongo = recordValueSchema.field(childFieldInMongo);
      if (isHaveChildFieldInMongo != null) {
        List<SinkRecord> listChildRecord = getNewChildRecord(record, recordValueSchema,
                recordValue, childFieldInMongo, childTableInPostgres,
                foreignKeyInPostgres, primaryKeyChildTableInPostgres);

        for (SinkRecord childRecord : listChildRecord) {
          addBufferByTable(childRecord, schemaName, catalogName, bufferByTable, connection);
        }
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

      Map<String, String> configMap = new HashMap<>(config.originalsStrings());
      Map<String, String> pkFieldsMap = new HashMap<>();
      pkFieldsMap.put("task", "task_id");
      pkFieldsMap.put("demands", "demand_id");
      pkFieldsMap.put("workschedule", "workschedule_id");

      String tableName = tableId.tableName();

      if (pkFieldsMap.containsKey(tableName)) {
        tableConfig = new JdbcSinkConfig(configMap);
        tableConfig.pkFields = Collections.singletonList(pkFieldsMap.get(tableName));
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
                                        List<String> listChildFieldInMongo,
                                        String primaryKeyParentTableInPostgres) {
    Set<String> excludedFields = new HashSet<>(listChildFieldInMongo);
    excludedFields.add(FIELD_NAME_MODIFIED_TS);
    excludedFields.add(FIELD_NAME_INSERTED_TS);

    if ("demands".equals(record.topic())) {
      excludedFields.add(ID_FIELD);
    }

    // build KeySchema
    Schema parentKeySchema = buildNewKeySchema(record,
            primaryKeyParentTableInPostgres);

    // build Key
    Struct parentKey = buildNewKey(record,
            oldValue,
            primaryKeyParentTableInPostgres,
            parentKeySchema);

    // build ValueSchema
    Schema newValueSchema = buildNewValueSchema(oldValueSchema, excludedFields);

    // build Value
    Struct newValue = buildNewValue(newValueSchema, oldValueSchema, excludedFields, oldValue);

    return new SinkRecord(
            record.topic(),
            record.kafkaPartition(),
            parentKeySchema,
            parentKey,
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

  private Struct buildNewKey(SinkRecord record,
                             Struct oldValue,
                             String primaryKeyParentTableInPostgres,
                             Schema parentKeySchema) {
    Struct parentKey;
    if (primaryKeyParentTableInPostgres != null) {
      parentKey = new Struct(parentKeySchema);
      parentKey.put(primaryKeyParentTableInPostgres,
              oldValue.get(parentKeySchema.field(primaryKeyParentTableInPostgres)));
    } else {
      parentKey = (Struct) record.key();
    }
    return parentKey;
  }

  private Schema buildNewKeySchema(SinkRecord record,
                                   String primaryKeyParentTableInPostgres) {
    Schema parentKeySchema;
    if (primaryKeyParentTableInPostgres != null) {
      parentKeySchema = SchemaBuilder.struct()
              .field(primaryKeyParentTableInPostgres,
                      new SchemaBuilder(Schema.Type.STRING).build())
              .build();
    } else {
      parentKeySchema = record.keySchema();
    }
    return parentKeySchema;
  }

  private List<SinkRecord> getNewChildRecord(SinkRecord record,
                                             Schema oldValueSchema,
                                             Struct oldValue,
                                             String childFieldInMongo,
                                             String childTableInPostgres,
                                             String foreignKeyInPostgres,
                                             String primaryKeyChildTableInPostgres) {
    List<SinkRecord> listChildRecord = new ArrayList<>();
    Struct child1Value = ((Struct) oldValue.get(childFieldInMongo));
    Set<String> excludedFields = getExcludedFieldsChildRecord(record);

    child1Value.schema().schema().fields().forEach(field -> {
      Schema child2ValueSchema = field.schema();
      Struct child2Value = (Struct) child1Value.get(field.name());

      // build KeySchema
      Schema child3KeySchema = buildNewKeySchema(record, primaryKeyChildTableInPostgres);

      // build Key
      Struct child3Key = buildKeyChildRecord(primaryKeyChildTableInPostgres,
              child3KeySchema, child2Value, child2ValueSchema);

      // build ValueSchema
      Schema child3ValueSchema = buildValueSchemaChildRecord(record, oldValueSchema,
              childTableInPostgres, foreignKeyInPostgres, child2ValueSchema, excludedFields);

      // build Value
      Struct child3Value = buildValueChildRecord(record, oldValue,
              childTableInPostgres, foreignKeyInPostgres, child3ValueSchema,
              child2ValueSchema, excludedFields, child2Value);

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

  private Struct buildKeyChildRecord(String primaryKeyChildTableInPostgres,
                                     Schema child3KeySchema,
                                     Struct child2Value,
                                     Schema child2ValueSchema) {
    Struct child3Key = new Struct(child3KeySchema);
    if (primaryKeyChildTableInPostgres != null) {
      child3Key.put(primaryKeyChildTableInPostgres,
              child2Value.get(child2ValueSchema.field(primaryKeyChildTableInPostgres)));
    } else {
      child3Key.put(ID_FIELD, child2Value.get(child2ValueSchema.field(_ID_FIELD)));
    }
    return child3Key;
  }

  private Set<String> getExcludedFieldsChildRecord(SinkRecord record) {
    Set<String> excludedFields = new HashSet<>(
            Arrays.asList(FIELD_NAME_MODIFIED_TS, FIELD_NAME_INSERTED_TS));

    switch (record.topic()) {
      case "class":
        excludedFields.add("class_id");
        break;
      case "class_group":
        excludedFields.add("class_group_id");
        break;
      default:
        break;
    }
    return excludedFields;
  }

  private Schema buildValueSchemaChildRecord(SinkRecord record,
                                             Schema oldValueSchema,
                                             String childTableInPostgres,
                                             String foreignKeyInPostgres,
                                             Schema child2ValueSchema,
                                             Set<String> excludedFields) {
    SchemaBuilder child3ValueSchemaBuilder = SchemaBuilder.struct();

    // Bỏ qua Schema trong danh sách bỏ qua, thay thế Schema
    for (Field field2 : child2ValueSchema.fields()) {
      if (excludedFields.contains(field2.name())) {
        continue;
      }
      String fieldName = field2.name();
      switch (fieldName) {
        case "hiduke":
          if ("workschedule".equals(childTableInPostgres)) {
            child3ValueSchemaBuilder.field(fieldName, Timestamp.SCHEMA);
          } else {
            child3ValueSchemaBuilder.field(fieldName, field2.schema());
          }
          break;

        case _ID_FIELD:
          child3ValueSchemaBuilder.field(ID_FIELD, field2.schema());
          break;

        default:
          child3ValueSchemaBuilder.field(fieldName, field2.schema());
          break;
      }
    }

    // Thêm Schema chưa có
    if (foreignKeyInPostgres != null) {
      child3ValueSchemaBuilder.field(foreignKeyInPostgres,
              oldValueSchema.field(ID_FIELD).schema());
    }
    if (child2ValueSchema.field(SYNC_ACTOR_FIELD) == null) {
      child3ValueSchemaBuilder.field(
              SYNC_ACTOR_FIELD, new SchemaBuilder(Schema.Type.STRING).build());
    }
    switch (record.topic()) {
      case "class":
        child3ValueSchemaBuilder.field("higher", oldValueSchema.field(ID_FIELD).schema());
        child3ValueSchemaBuilder.field("lower", child2ValueSchema.field("class_id").schema());
        child3ValueSchemaBuilder.field("depth", SchemaBuilder.type(Schema.Type.INT64).build());
        child3ValueSchemaBuilder.field("type", SchemaBuilder.type(Schema.Type.STRING).build());
        break;
      case "class_group":
        child3ValueSchemaBuilder.field("higher", oldValueSchema.field(ID_FIELD).schema());
        child3ValueSchemaBuilder.field("lower", child2ValueSchema.field("class_group_id").schema());
        child3ValueSchemaBuilder.field("depth", SchemaBuilder.type(Schema.Type.INT64).build());
        break;
      default:
        break;
    }

    return child3ValueSchemaBuilder.build();
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private Struct buildValueChildRecord(SinkRecord record,
                                       Struct oldValue,
                                       String childTableInPostgres,
                                       String foreignKeyInPostgres,
                                       Schema child3ValueSchema,
                                       Schema child2ValueSchema,
                                       Set<String> excludedFields,
                                       Struct child2Value) {
    Struct child3Value = new Struct(child3ValueSchema);
    // Bỏ qua Value trong danh sách bỏ qua, thay thế Schema
    for (Field field2 : child2ValueSchema.fields()) {
      if (excludedFields.contains(field2.name())) {
        continue;
      }

      String fieldName = field2.name();
      switch (fieldName) {
        case _ID_FIELD:
          child3Value.put(ID_FIELD, child2Value.get(field2));
          break;
        case SYNC_ACTOR_FIELD:
          child3Value.put(fieldName, SYNC_ACTOR_MONGODB);
          break;
        case "hiduke":
          if ("workschedule".equals(childTableInPostgres)) {
            int hidukeLong = ((Long) child2Value.get(field2)).intValue();
            Calendar calendar = Calendar.getInstance();
            calendar.set(1970, Calendar.JANUARY, 1);
            calendar.add(Calendar.DATE, hidukeLong);
            Date resultDate = calendar.getTime();

            child3Value.put(fieldName, resultDate);
          } else {
            child3Value.put(fieldName, child2Value.get(field2));
          }
          break;
        default:
          child3Value.put(fieldName, child2Value.get(field2));
          break;
      }
    }

    // Thêm Value chưa có
    if (foreignKeyInPostgres != null) {
      child3Value.put(foreignKeyInPostgres, oldValue.get(ID_FIELD));
    }
    if (child2ValueSchema.field(SYNC_ACTOR_FIELD) == null) {
      child3Value.put(SYNC_ACTOR_FIELD, SYNC_ACTOR_MONGODB);
    }
    switch (record.topic()) {
      case "class":
        child3Value.put("higher", oldValue.get(ID_FIELD));
        child3Value.put("lower", child2Value.get("class_id"));
        child3Value.put("depth", 1L);
        child3Value.put("type", "");
        break;
      case "class_group":
        child3Value.put("higher", oldValue.get(ID_FIELD));
        child3Value.put("lower", child2Value.get("class_group_id"));
        child3Value.put("depth", 1L);
        break;
      default:
        break;
    }
    return child3Value;
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
