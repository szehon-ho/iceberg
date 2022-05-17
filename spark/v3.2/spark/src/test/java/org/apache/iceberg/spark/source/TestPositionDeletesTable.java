/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.PositionDeletesTableScan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkStructLike;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.FILE_PATH;

@RunWith(Parameterized.class)
public class TestPositionDeletesTable extends SparkTestBase {

  public static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "data", Types.StringType.get())
  );
  private FileFormat format;

  @Parameterized.Parameters(name = "fileFormat = {0}")
  public static Object[][] parameters() {
    return new Object[][] {
        { FileFormat.PARQUET},
        { FileFormat.AVRO},
        { FileFormat.ORC}
    };
  }

  public TestPositionDeletesTable(FileFormat format) {
    this.format = format;
  }

  protected Table createTable(String name, Schema schema, PartitionSpec spec) {
    Map<String, String> properties = ImmutableMap.of(
        TableProperties.FORMAT_VERSION, "2",
        TableProperties.DEFAULT_FILE_FORMAT, format.toString());
    return catalog.createTable(TableIdentifier.of("default", name), schema, spec, properties);
  }

  protected void dropTable(String name) {
    catalog.dropTable(TableIdentifier.of("default", name));
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testPartitionedTable() throws IOException {
    // Create table with two partitions
    String tableName = "partitioned_table";
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, spec);

    GenericRecord record = GenericRecord.create(tab.schema());
    List<org.apache.iceberg.data.Record> dataRecordsA = Lists.newArrayList(
        record.copy("id", 29, "data", "a"),
        record.copy("id", 43, "data", "a"),
        record.copy("id", 61, "data", "a"),
        record.copy("id", 89, "data", "a"));
    DataFile dataFileA = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), dataRecordsA);

    List<org.apache.iceberg.data.Record> dataRecordsB = Lists.newArrayList(
        record.copy("id", 100, "data", "b"),
        record.copy("id", 121, "data", "b"),
        record.copy("id", 122, "data", "b"),
        record.copy("id", 149, "data", "b"));
    DataFile dataFileB = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b"), dataRecordsB);

    tab.newAppend()
        .appendFile(dataFileA)
        .appendFile(dataFileB)
        .commit();

    // Add position deletes for both partitions
    List<PositionDelete<?>> deletesA = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileA.path(), 0L, "id", 29, "data", "a"),
        positionDelete(tab.schema(), dataFileA.path(), 1L, "id", 43, "data", "a"));
    List<PositionDelete<?>> deletesB = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileB.path(), 2L, "id", 122, "data", "b"),
        positionDelete(tab.schema(), dataFileB.path(), 3L, "id", 149, "data", "b"));
    DeleteFile deleteFileA = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), deletesA);
    DeleteFile deleteFileB = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b"), deletesB);

    tab.newRowDelta()
        .addDeletes(deleteFileA)
        .addDeletes(deleteFileB)
        .commit();

    // Select deletes from one partition
    StructLikeSet actual = actualPositionDeleteRowSet(tableName, tab, "row.data='b'");
    GenericRecord partitionB = GenericRecord.create(tab.spec().partitionType());
    partitionB.setField("data", "b");
    StructLikeSet expected = expectedPosDeleteRowSet(tab, deletesB, partitionB);

    Assert.assertEquals("Position Delete table should contain expected rows", expected, actual);
    dropTable(tableName);
  }

  @Test
  public void testSplitTasks() throws IOException {
    String tableName = "big_table";
    Table tab = createTable(tableName, SCHEMA, PartitionSpec.unpartitioned());
    tab.updateProperties()
        .set("read.split.target-size", "100")
        .commit();

    GenericRecord record = GenericRecord.create(tab.schema());
    List<org.apache.iceberg.data.Record> dataRecords = Lists.newArrayList();
    for (int i = 0; i < 1000; i++) {
      dataRecords.add(record.copy("id", i, "data", String.valueOf(i)));
    }
    DataFile dFile = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of(), dataRecords);
    tab.newAppend()
        .appendFile(dFile)
        .commit();

    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    for (long i = 0; i < 1000; i++) {
      deletes.add(Pair.of(dFile.path(), i));
    }
    Pair<DeleteFile, CharSequenceSet> posDeletes = FileHelpers.writeDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of(0), deletes);
    tab.newRowDelta()
        .addDeletes(posDeletes.first())
        .commit();

    PositionDeletesTableScan deleteTableScan = new PositionDeletesTableScan(
        ((HasTableOperations) tab).operations(),
        tab,
        DeleteSchemaUtil.metadataTableSchema(tab));
    Assert.assertTrue("Position delete scan should produce more than one split",
        Iterables.size(deleteTableScan.planTasks()) > 1);

    StructLikeSet actual = actualPositionDeleteRowSet(tableName, tab);
    StructLikeSet expected = expectedPosDeleteRowSet(tab, deletes);

    Assert.assertEquals("Position Delete table should contain expected rows", expected, actual);
    dropTable(tableName);
  }

  @Test
  public void testMetadataColumns() throws IOException {
    // Create table with two partitions
    String tableName = "metadata_columns";
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, spec);

    GenericRecord record = GenericRecord.create(tab.schema());
    List<org.apache.iceberg.data.Record> dataRecordsA = Lists.newArrayList(
        record.copy("id", 29, "data", "a"),
        record.copy("id", 43, "data", "a"),
        record.copy("id", 61, "data", "a"),
        record.copy("id", 89, "data", "a"));
    DataFile dataFileA = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), dataRecordsA);

    List<org.apache.iceberg.data.Record> dataRecordsB = Lists.newArrayList(
        record.copy("id", 100, "data", "b"),
        record.copy("id", 121, "data", "b"),
        record.copy("id", 122, "data", "b"),
        record.copy("id", 149, "data", "b"));
    DataFile dataFileB = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b"), dataRecordsB);

    tab.newAppend()
        .appendFile(dataFileA)
        .appendFile(dataFileB)
        .commit();

    // Add position deletes for both partitions
    List<PositionDelete<?>> deletesA = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileA.path(), 0L, "id", 29, "data", "a"),
        positionDelete(tab.schema(), dataFileA.path(), 1L, "id", 43, "data", "a"));
    List<PositionDelete<?>> deletesB = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileB.path(), 2L, "id", 122, "data", "b"),
        positionDelete(tab.schema(), dataFileB.path(), 3L, "id", 149, "data", "b"));
    DeleteFile deleteFileA = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), deletesA);
    DeleteFile deleteFileB = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b"), deletesB);

    tab.newRowDelta()
        .addDeletes(deleteFileA)
        .addDeletes(deleteFileB)
        .commit();

    // Select deletes from one partition
    Types.StructType selectSchema = new Schema(
        DELETE_FILE_PATH,
        FILE_PATH
    ).asStruct();
    StructLikeSet actual = actualPositionDeleteRowSet(
        tableName,
        tab,
        selectSchema);
    StructLikeSet expected = StructLikeSet.create(selectSchema);
    GenericRecord expectedRecord = GenericRecord.create(selectSchema);
    expected.add(expectedRecord.copy(DELETE_FILE_PATH.name(), dataFileA.path(), FILE_PATH.name(), deleteFileA.path()));
    expected.add(expectedRecord.copy(DELETE_FILE_PATH.name(), dataFileB.path(), FILE_PATH.name(), deleteFileB.path()));
    Assert.assertEquals("Position Delete table should contain expected rows", expected, actual);
    dropTable(tableName);
  }

  @Test
  public void testPartitionFilterIdentity() throws IOException {
    // Create table with two partitions
    String tableName = "partition_filter";
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, spec);

    GenericRecord record = GenericRecord.create(tab.schema());
    List<org.apache.iceberg.data.Record> dataRecordsA = Lists.newArrayList(
        record.copy("id", 29, "data", "a"),
        record.copy("id", 43, "data", "a"),
        record.copy("id", 61, "data", "a"),
        record.copy("id", 89, "data", "a"));
    DataFile dataFileA = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), dataRecordsA);

    List<org.apache.iceberg.data.Record> dataRecordsB = Lists.newArrayList(
        record.copy("id", 100, "data", "b"),
        record.copy("id", 121, "data", "b"),
        record.copy("id", 122, "data", "b"),
        record.copy("id", 149, "data", "b"));
    DataFile dataFileB = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b"), dataRecordsB);

    tab.newAppend()
        .appendFile(dataFileA)
        .appendFile(dataFileB)
        .commit();

    // Add position deletes for both partitions
    List<PositionDelete<?>> deletesA = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileA.path(), 0L, "id", 29, "data", "a"),
        positionDelete(tab.schema(), dataFileA.path(), 1L, "id", 43, "data", "a"));
    List<PositionDelete<?>> deletesB = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileB.path(), 2L, "id", 122, "data", "b"),
        positionDelete(tab.schema(), dataFileB.path(), 3L, "id", 149, "data", "b"));
    DeleteFile deleteFileA = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), deletesA);
    DeleteFile deleteFileB = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b"), deletesB);

    tab.newRowDelta()
        .addDeletes(deleteFileA)
        .addDeletes(deleteFileB)
        .commit();

    // Prepare expected values
    GenericRecord partitionRecordTemplate = GenericRecord.create(tab.spec().partitionType());
    org.apache.iceberg.data.Record partitionA = partitionRecordTemplate.copy("data", "a");
    org.apache.iceberg.data.Record partitionB = partitionRecordTemplate.copy("data", "b");
    StructLikeSet expectedA = expectedPosDeleteRowSet(tab, deletesA, partitionA);
    StructLikeSet expectedB = expectedPosDeleteRowSet(tab, deletesB, partitionB);

    // Select deletes from all partitions
    StructLikeSet actual = actualPositionDeleteRowSet(tableName, tab);
    StructLikeSet allExpected = StructLikeSet.create(DeleteSchemaUtil.metadataTableSchema(tab).asStruct());
    allExpected.addAll(expectedA);
    allExpected.addAll(expectedB);
    Assert.assertEquals("Position Delete table should contain expected rows", allExpected, actual);

    // Select deletes from one partition
    StructLikeSet actual2 = actualPositionDeleteRowSet(
        tableName,
        tab,
        "partition.data = 'a' AND pos >= 0");

    Assert.assertEquals("Position Delete table should contain expected rows", expectedA, actual2);
    dropTable(tableName);
  }

  @Test
  public void testPartitionTransformFilter() throws IOException {
    // Create table with two partitions
    String tableName = "partition_filter";
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).truncate("data", 1).build();
    Table tab = createTable(tableName, SCHEMA, spec);

    GenericRecord record = GenericRecord.create(tab.schema());
    List<org.apache.iceberg.data.Record> dataRecordsA = Lists.newArrayList(
        record.copy("id", 29, "data", "aa"),
        record.copy("id", 43, "data", "aa"),
        record.copy("id", 61, "data", "aa"),
        record.copy("id", 89, "data", "aa"));
    DataFile dataFileA = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), dataRecordsA);

    List<org.apache.iceberg.data.Record> dataRecordsB = Lists.newArrayList(
        record.copy("id", 100, "data", "bb"),
        record.copy("id", 121, "data", "bb"),
        record.copy("id", 122, "data", "bb"),
        record.copy("id", 149, "data", "bb"));
    DataFile dataFileB = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b"), dataRecordsB);

    tab.newAppend()
        .appendFile(dataFileA)
        .appendFile(dataFileB)
        .commit();

    // Add position deletes for both partitions
    List<PositionDelete<?>> deletesA = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileA.path(), 0L, "id", 29, "data", "aa"),
        positionDelete(tab.schema(), dataFileA.path(), 1L, "id", 43, "data", "aa"));
    List<PositionDelete<?>> deletesB = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileB.path(), 2L, "id", 122, "data", "bb"),
        positionDelete(tab.schema(), dataFileB.path(), 3L, "id", 149, "data", "bb"));
    DeleteFile deleteFileA = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), deletesA);
    DeleteFile deleteFileB = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b"), deletesB);

    tab.newRowDelta()
        .addDeletes(deleteFileA)
        .addDeletes(deleteFileB)
        .commit();

    // Prepare expected values
    GenericRecord partitionRecordTemplate = GenericRecord.create(tab.spec().partitionType());
    org.apache.iceberg.data.Record partitionA = partitionRecordTemplate.copy("data_trunc", "a");
    org.apache.iceberg.data.Record partitionB = partitionRecordTemplate.copy("data_trunc", "b");
    StructLikeSet expectedA = expectedPosDeleteRowSet(tab, deletesA, partitionA);
    StructLikeSet expectedB = expectedPosDeleteRowSet(tab, deletesB, partitionB);

    // Select deletes from all partitions
    StructLikeSet actual = actualPositionDeleteRowSet(tableName, tab);
    StructLikeSet allExpected = StructLikeSet.create(DeleteSchemaUtil.metadataTableSchema(tab).asStruct());
    allExpected.addAll(expectedA);
    allExpected.addAll(expectedB);
    Assert.assertEquals("Position Delete table should contain expected rows", allExpected, actual);

    // Select deletes from one partition
    StructLikeSet actual2 = actualPositionDeleteRowSet(
        tableName,
        tab,
        "partition.data_trunc = 'a' AND pos >= 0");

    Assert.assertEquals("Position Delete table should contain expected rows", expectedA, actual2);
    dropTable(tableName);
  }

  @Test
  public void testPartitionEvolutionReplace() throws Exception {
    // Create table partitioned by "data"
    String tableName = "partition_evolution";
    PartitionSpec originalSpec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, originalSpec);

    // Add data files
    GenericRecord record = GenericRecord.create(tab.schema());
    List<org.apache.iceberg.data.Record> dataRecordsA = Lists.newArrayList(
        record.copy("id", 29, "data", "a"),
        record.copy("id", 43, "data", "a"),
        record.copy("id", 61, "data", "a"),
        record.copy("id", 89, "data", "a"));
    DataFile dataFileA = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), dataRecordsA);

    List<org.apache.iceberg.data.Record> dataRecordsB = Lists.newArrayList(
        record.copy("id", 100, "data", "b"),
        record.copy("id", 121, "data", "b"),
        record.copy("id", 122, "data", "b"),
        record.copy("id", 149, "data", "b"));
    DataFile dataFileB = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b"), dataRecordsB);

    tab.newAppend()
        .appendFile(dataFileA)
        .appendFile(dataFileB)
        .commit();

    // Add position deletes for both partitions
    List<PositionDelete<?>> deletesA = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileA.path(), 0L, "id", 29, "data", "a"),
        positionDelete(tab.schema(), dataFileA.path(), 1L, "id", 43, "data", "a"));
    List<PositionDelete<?>> deletesB = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileB.path(), 2L, "id", 122, "data", "b"),
        positionDelete(tab.schema(), dataFileB.path(), 3L, "id", 149, "data", "b"));
    DeleteFile deleteFileA = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), deletesA);
    DeleteFile deleteFileB = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b"), deletesB);
    tab.newRowDelta()
        .addDeletes(deleteFileA)
        .addDeletes(deleteFileB)
        .commit();

    // Switch partition spec from (data) to (id)
    tab.updateSpec()
        .removeField("data")
        .addField("id")
        .commit();

    // Add data files
    List<org.apache.iceberg.data.Record> dataRecords10 = Lists.newArrayList(
        record.copy("id", 10, "data", "b"),
        record.copy("id", 10, "data", "f"),
        record.copy("id", 10, "data", "h"),
        record.copy("id", 10, "data", "l"));
    DataFile dataFile10 = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of(10), dataRecords10);

    List<org.apache.iceberg.data.Record> dataRecords99 = Lists.newArrayList(
        record.copy("id", 99, "data", "n"),
        record.copy("id", 99, "data", "p"),
        record.copy("id", 99, "data", "s"),
        record.copy("id", 99, "data", "y"));
    DataFile dataFile99 = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of(99), dataRecords99);

    tab.newAppend()
        .appendFile(dataFile10)
        .appendFile(dataFile99)
        .commit();

    // Add position deletes
    List<PositionDelete<?>> deletes10 = Lists.newArrayList(
        positionDelete(tab.schema(), dataFile10.path(), 0L, "id", 10, "data", "b"),
        positionDelete(tab.schema(), dataFile10.path(), 1L, "id", 10, "data", "f"));
    List<PositionDelete<?>> deletes99 = Lists.newArrayList(
        positionDelete(tab.schema(), dataFile99.path(), 2L, "id", 99, "data", "s"),
        positionDelete(tab.schema(), dataFile99.path(), 3L, "id", 99, "data", "y"));
    DeleteFile deleteFile10 = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of(10), deletes10);
    DeleteFile deleteFile99 = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of(99), deletes99);
    tab.newRowDelta()
        .addDeletes(deleteFile10)
        .addDeletes(deleteFile99)
        .commit();

    // Select deletes from 'data' partition
    GenericRecord partitionRecordTemplate = GenericRecord.create(Partitioning.partitionType(tab));
    org.apache.iceberg.data.Record partitionA = partitionRecordTemplate.copy("data", "a");
    StructLikeSet expectedA = expectedPosDeleteRowSet(tab, Partitioning.partitionType(tab), deletesA, partitionA);
    StructLikeSet actualA = actualPositionDeleteRowSet(
        tableName,
        tab,
        "partition.data = 'a' AND pos >= 0");
    Assert.assertEquals("Position Delete table should contain expected rows", expectedA, actualA);

    org.apache.iceberg.data.Record partition10 = partitionRecordTemplate.copy("id", 10);
    StructLikeSet expected10 = expectedPosDeleteRowSet(tab, Partitioning.partitionType(tab), deletes10, partition10);
    StructLikeSet actual10 = actualPositionDeleteRowSet(
        tableName,
        tab,
        "partition.id = 10 AND pos >= 0");

    Assert.assertEquals("Position Delete table should contain expected rows", expected10, actual10);
    dropTable(tableName);
  }

  @Test
  public void testPartitionEvolutionAdd() throws Exception {
    // Create table partitioned by "data"
    String tableName = "partition_evolution_add";
    Table tab = createTable(tableName, SCHEMA, PartitionSpec.unpartitioned());

    // Add data file and delete
    GenericRecord record = GenericRecord.create(tab.schema());
    List<org.apache.iceberg.data.Record> dataRecordsUnpartitioned = Lists.newArrayList(
        record.copy("id", 29, "data", "a"),
        record.copy("id", 43, "data", "a"),
        record.copy("id", 61, "data", "b"),
        record.copy("id", 89, "data", "b"));
    DataFile dataFileUnpartitioned = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), dataRecordsUnpartitioned);
    tab.newAppend()
        .appendFile(dataFileUnpartitioned)
        .commit();

    List<PositionDelete<?>> deletesUnpartitioned = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileUnpartitioned.path(), 0L, "id", 29, "data", "a"),
        positionDelete(tab.schema(), dataFileUnpartitioned.path(), 1L, "id", 43, "data", "a"));
    DeleteFile deleteFileUnpartitioned = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), deletesUnpartitioned);
    tab.newRowDelta()
        .addDeletes(deleteFileUnpartitioned)
        .commit();

    // Switch partition spec to (data)
    tab.updateSpec()
        .addField("data")
        .commit();

    // Add data file and delete files for partitions
    List<org.apache.iceberg.data.Record> dataRecordsA = Lists.newArrayList(
        record.copy("id", 103, "data", "a"),
        record.copy("id", 137, "data", "a"),
        record.copy("id", 164, "data", "a"),
        record.copy("id", 187, "data", "a"));
    DataFile dataFile10 = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), dataRecordsA);

    List<org.apache.iceberg.data.Record> dataRecordsB = Lists.newArrayList(
        record.copy("id", 214, "data", "b"),
        record.copy("id", 232, "data", "b"),
        record.copy("id", 267, "data", "b"),
        record.copy("id", 290, "data", "b"));
    DataFile dataFile99 = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b"), dataRecordsB);
    tab.newAppend()
        .appendFile(dataFile10)
        .appendFile(dataFile99)
        .commit();

    List<PositionDelete<?>> deletesA = Lists.newArrayList(
        positionDelete(tab.schema(), dataFile10.path(), 0L, "id", 103, "data", "a"),
        positionDelete(tab.schema(), dataFile10.path(), 1L, "id", 137, "data", "a"));
    List<PositionDelete<?>> deletesB = Lists.newArrayList(
        positionDelete(tab.schema(), dataFile99.path(), 2L, "id", 267, "data", "b"),
        positionDelete(tab.schema(), dataFile99.path(), 3L, "id", 290, "data", "b"));
    DeleteFile deleteFileA = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), deletesA);
    DeleteFile deleteFileB = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b'"), deletesB);
    tab.newRowDelta()
        .addDeletes(deleteFileA)
        .addDeletes(deleteFileB)
        .commit();

    // Select deletes from 'data' partition
    GenericRecord partitionRecordTemplate = GenericRecord.create(Partitioning.partitionType(tab));
    org.apache.iceberg.data.Record partitionA = partitionRecordTemplate.copy("data", "a");
    StructLikeSet expectedA = expectedPosDeleteRowSet(tab, Partitioning.partitionType(tab), deletesA, partitionA);
    StructLikeSet actualA = actualPositionDeleteRowSet(
        tableName,
        tab,
        "partition.data = 'a' AND pos >= 0");
    Assert.assertEquals("Position Delete table should contain expected rows", expectedA, actualA);

    // Select deletes from 'unpartitioned' partition
    org.apache.iceberg.data.Record unpartitionedRecord = partitionRecordTemplate.copy("data", null);
    StructLikeSet expectedUnpartitioned = expectedPosDeleteRowSet(tab, Partitioning.partitionType(tab),
        deletesUnpartitioned, unpartitionedRecord);
    StructLikeSet actualUnpartitioned = actualPositionDeleteRowSet(
        tableName,
        tab,
        "partition.data IS NULL and pos >= 0");

    Assert.assertEquals("Position Delete table should contain expected rows", expectedUnpartitioned,
        actualUnpartitioned);
    dropTable(tableName);
  }

  @Test
  public void testPartitionEvolutionRemove() throws Exception {
    // Create table partitioned by "data"
    String tableName = "partition_evolution_remove";
    PartitionSpec originalSpec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, originalSpec);

    // Add data files
    GenericRecord record = GenericRecord.create(tab.schema());
    List<org.apache.iceberg.data.Record> dataRecordsA = Lists.newArrayList(
        record.copy("id", 29, "data", "a"),
        record.copy("id", 43, "data", "a"),
        record.copy("id", 61, "data", "a"),
        record.copy("id", 89, "data", "a"));
    DataFile dataFileA = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), dataRecordsA);

    List<org.apache.iceberg.data.Record> dataRecordsB = Lists.newArrayList(
        record.copy("id", 100, "data", "b"),
        record.copy("id", 121, "data", "b"),
        record.copy("id", 122, "data", "b"),
        record.copy("id", 149, "data", "b"));
    DataFile dataFileB = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b"), dataRecordsB);

    tab.newAppend()
        .appendFile(dataFileA)
        .appendFile(dataFileB)
        .commit();

    // Add position deletes for both partitions
    List<PositionDelete<?>> deletesA = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileA.path(), 0L, "id", 29, "data", "a"),
        positionDelete(tab.schema(), dataFileA.path(), 1L, "id", 43, "data", "a"));
    List<PositionDelete<?>> deletesB = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileB.path(), 2L, "id", 122, "data", "b"),
        positionDelete(tab.schema(), dataFileB.path(), 3L, "id", 149, "data", "b"));
    DeleteFile deleteFileA = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), deletesA);
    DeleteFile deleteFileB = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("b"), deletesB);
    tab.newRowDelta()
        .addDeletes(deleteFileA)
        .addDeletes(deleteFileB)
        .commit();

    // Remove partition field
    tab.updateSpec()
        .removeField("data")
        .commit();

    // Add data file and delete
    List<org.apache.iceberg.data.Record> dataRecordsUnpartitioned = Lists.newArrayList(
        record.copy("id", 103, "data", "a"),
        record.copy("id", 138, "data", "a"),
        record.copy("id", 166, "data", "b"),
        record.copy("id", 189, "data", "b"));
    DataFile dataFileUnpartitioned = FileHelpers.writeDataFile(tab,
        Files.localOutput(temp.newFile()), dataRecordsUnpartitioned);
    tab.newAppend()
        .appendFile(dataFileUnpartitioned)
        .commit();

    List<PositionDelete<?>> deletesUnpartitioned = Lists.newArrayList(
        positionDelete(tab.schema(), dataFileUnpartitioned.path(), 0L, "id", 29, "data", "a"),
        positionDelete(tab.schema(), dataFileUnpartitioned.path(), 1L, "id", 43, "data", "a"));
    DeleteFile deleteFileUnpartitioned = FileHelpers.writePosDeleteFile(
        tab, Files.localOutput(temp.newFile()), org.apache.iceberg.TestHelpers.Row.of("a"), deletesUnpartitioned);
    tab.newRowDelta()
        .addDeletes(deleteFileUnpartitioned)
        .commit();

    // Select deletes from 'data' partition
    GenericRecord partitionRecordTemplate = GenericRecord.create(Partitioning.partitionType(tab));
    org.apache.iceberg.data.Record partitionA = partitionRecordTemplate.copy("data", "a");
    StructLikeSet expectedA = expectedPosDeleteRowSet(tab, Partitioning.partitionType(tab), deletesA, partitionA);
    StructLikeSet actualA = actualPositionDeleteRowSet(
        tableName,
        tab,
        "partition.data = 'a' AND pos >= 0");
    Assert.assertEquals("Position Delete table should contain expected rows", expectedA, actualA);

    // Select deletes from 'unpartitioned' partition
    org.apache.iceberg.data.Record unpartitionedRecord = partitionRecordTemplate.copy("data", null);
    StructLikeSet expectedUnpartitioned = expectedPosDeleteRowSet(tab, Partitioning.partitionType(tab),
        deletesUnpartitioned, unpartitionedRecord);
    StructLikeSet actualUnpartitioned = actualPositionDeleteRowSet(
        tableName,
        tab,
        "partition.data IS NULL and pos >= 0");

    Assert.assertEquals("Position Delete table should contain expected rows", expectedUnpartitioned,
        actualUnpartitioned);
    dropTable(tableName);
  }

  private StructLikeSet actualPositionDeleteRowSet(String tableName, Table table) {
    return actualPositionDeleteRowSet(tableName, table, null, null);
  }

  private StructLikeSet actualPositionDeleteRowSet(String tableName, Table table, String filter) {
    return actualPositionDeleteRowSet(tableName, table, filter, null);
  }

  private StructLikeSet actualPositionDeleteRowSet(String tableName, Table table, Types.StructType selectSchema) {
    return actualPositionDeleteRowSet(tableName, table, null, selectSchema);
  }

  private StructLikeSet actualPositionDeleteRowSet(String tableName, Table table, String filter,
                                                   Types.StructType selectSchema) {
    Dataset<Row> df = spark.read()
        .format("iceberg")
        .load("default." + tableName + ".position_deletes");
    if (selectSchema != null) {
      Column[] columns = selectSchema.fields()
          .stream()
          .map(field -> new Column(field.name()))
          .toArray(Column[]::new);
      df = df.select(columns);
    }
    if (filter != null) {
      df = df.filter(filter);
    }
    Types.StructType projection;
    if (selectSchema != null) {
      projection = selectSchema;
    } else {
      projection = DeleteSchemaUtil.metadataTableSchema(table).asStruct();
    }
    StructLikeSet set = StructLikeSet.create(projection);
    df.collectAsList().forEach(row -> {
      SparkStructLike rowWrapper = new SparkStructLike(projection);
      set.add(rowWrapper.wrap(row));
    });

    return set;
  }

  private PositionDelete<GenericRecord> positionDelete(Schema tableSchema, CharSequence path, Long position,
                                                       String field1, Object value1, String field2, Object value2) {
    PositionDelete<GenericRecord> posDelete = PositionDelete.create();
    GenericRecord nested = GenericRecord.create(tableSchema);
    nested = (GenericRecord) nested.copy(field1, value1, field2, value2);
    posDelete.set(path, position, nested);
    return posDelete;
  }

  private StructLikeSet expectedPosDeleteRowSet(Table testTable, List<Pair<CharSequence, Long>> deletes) {
    Types.StructType posDeleteSchema = DeleteSchemaUtil.metadataTableSchema(testTable).asStruct();
    StructLikeSet set = StructLikeSet.create(posDeleteSchema);
    GenericRecord record = GenericRecord.create(posDeleteSchema);
    deletes.stream()
        .map(p -> record.copy("file_path", p.first(), "pos", p.second()))
        .forEach(set::add);
    return set;
  }

  private StructLikeSet expectedPosDeleteRowSet(Table testTable,
                                                Types.StructType partitionType,
                                                List<PositionDelete<?>> deletes,
                                                StructLike partitionStruct) {
    Types.StructType posDeleteSchema = DeleteSchemaUtil.metadataTableSchema(testTable, partitionType).asStruct();
    StructLikeSet set = StructLikeSet.create(posDeleteSchema);
    deletes.stream()
        .map(p -> {
          GenericRecord record = GenericRecord.create(posDeleteSchema);
          record.setField("file_path", p.path());
          record.setField("pos", p.pos());
          record.setField("row", p.row());
          if (partitionStruct != null) {
            record.setField("partition", partitionStruct);
          }
          return record;
        })
        .forEach(set::add);
    return set;
  }

  private StructLikeSet expectedPosDeleteRowSet(Table testTable,
                                                List<PositionDelete<?>> deletes,
                                                StructLike partitionStruct) {
    return expectedPosDeleteRowSet(testTable, Partitioning.partitionType(testTable), deletes, partitionStruct);
  }
}
