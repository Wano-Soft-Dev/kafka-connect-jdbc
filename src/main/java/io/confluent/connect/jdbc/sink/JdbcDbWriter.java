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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.List;
import java.util.ArrayList;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

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

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  void write(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException {
    final Connection connection = cachedConnectionProvider.getConnection();
    String schemaName = getSchemaSafe(connection).orElse(null);
    String catalogName = getCatalogSafe(connection).orElse(null);
    try {
      final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
      for (SinkRecord record : records) {
        if ("syain".equals(record.topic())) {
          Field isHaveFieldsSyainBusyos = record.valueSchema().field("syain_busyos");
          if (isHaveFieldsSyainBusyos != null) {
            SinkRecord newRecord = getNewRecord(record);

            final TableId tableId = destinationTable(newRecord.topic(), schemaName, catalogName);
            BufferedRecords buffer = bufferByTable.get(tableId);
            if (buffer == null) {
              buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
              bufferByTable.put(tableId, buffer);
            }
            buffer.add(newRecord);

            List<SinkRecord> listChildRecord = getChildRecord(record);

            for (SinkRecord childRecord : listChildRecord) {
              final TableId childTableId =
                      destinationTable(childRecord.topic(), schemaName, catalogName);
              BufferedRecords childBuffer = bufferByTable.get(childTableId);
              if (childBuffer == null) {
                childBuffer = new BufferedRecords(
                        config, childTableId, dbDialect, dbStructure, connection);
                bufferByTable.put(childTableId, childBuffer);
              }
              childBuffer.add(childRecord);
            }
          }
        } else {
          final TableId tableId = destinationTable(record.topic(), schemaName, catalogName);
          BufferedRecords buffer = bufferByTable.get(tableId);
          if (buffer == null) {
            buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
            bufferByTable.put(tableId, buffer);
          }
          buffer.add(record);
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

  private SinkRecord getNewRecord(SinkRecord record) {
    Schema oldValueSchema = record.valueSchema();
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    for (Field field : oldValueSchema.fields()) {
      if (!"syain_busyos".equals(field.name())) {
        schemaBuilder.field(field.name(), field.schema());
      }
    }
    Schema newValueSchema = schemaBuilder.build();

    Struct oldValue = (Struct) record.value();
    Struct newValue = new Struct(newValueSchema);

    for (Field field : oldValueSchema.fields()) {
      if (!"syain_busyos".equals(field.name())) {
        newValue.put(field.name(), oldValue.get(field));
      }
    }

    return new SinkRecord(record.topic(), record.kafkaPartition(),
            record.keySchema(), record.key(), newValueSchema,
            newValue, record.kafkaOffset(), record.timestamp(),
            record.timestampType(), record.headers());
  }

  private List<SinkRecord> getChildRecord(SinkRecord record) {
    List<SinkRecord> listChildRecord = new ArrayList<>();
    Schema oldValueSchema = record.valueSchema();
    Struct oldValue = (Struct) record.value();

    Struct child1Value = ((Struct) oldValue.get("syain_busyos"));

    child1Value.schema().schema().fields().forEach(field -> {
      Schema child2ValueSchema = field.schema();
      Struct child2Value = (Struct) child1Value.get(field.name());

      // build KeySchema
      Schema child3KeySchema = record.keySchema();

      // build Key
      Struct child3Key = new Struct(child3KeySchema);
      child3Key.put("id", child2Value.get(child2ValueSchema.field("_id")));

      // build ValueSchema
      SchemaBuilder child3ValueSchemaBuilder = SchemaBuilder.struct();
      for (Field field2 : child2ValueSchema.fields()) {
        if ("_id".equals(field2.name())) {
          child3ValueSchemaBuilder.field("id", field2.schema());
        } else {
          child3ValueSchemaBuilder.field(field2.name(), field2.schema());
        }
      }
      child3ValueSchemaBuilder.field("syain_id", oldValueSchema.field("id").schema());

      Schema child3ValueSchema = child3ValueSchemaBuilder.build();

      // build Value
      Struct child3Value = new Struct(child3ValueSchema);
      for (Field field2 : child2ValueSchema.fields()) {
        if ("_id".equals(field2.name())) {
          child3Value.put("id", child2Value.get(field2));
        } else {
          child3Value.put(field2.name(), child2Value.get(field2));
        }
      }
      child3Value.put("syain_id", oldValue.get("id"));

      SinkRecord childRecord = new SinkRecord(
              "syain_busyo",
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
