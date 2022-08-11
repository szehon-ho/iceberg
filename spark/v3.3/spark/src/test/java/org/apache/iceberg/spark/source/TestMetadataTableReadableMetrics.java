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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Option;
import scala.collection.JavaConverters;

public class TestMetadataTableReadableMetrics extends SparkTestBase {

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private static final Types.StructType LEAF_STRUCT_TYPE =
      Types.StructType.of(
          optional(1, "leafLongCol", Types.LongType.get()),
          optional(2, "leafDoubleCol", Types.DoubleType.get()));

  private static final Types.StructType NESTED_STRUCT_TYPE =
      Types.StructType.of(required(3, "leafStructCol", LEAF_STRUCT_TYPE));

  private static final Schema NESTED_SCHEMA =
      new Schema(required(4, "nestedStructCol", NESTED_STRUCT_TYPE));

  private static final Schema PRIMITIVE_SCHEMA =
      new Schema(
          required(1, "booleanCol", Types.BooleanType.get()),
          required(2, "intCol", Types.IntegerType.get()),
          required(3, "longCol", Types.LongType.get()),
          required(4, "floatCol", Types.FloatType.get()),
          required(5, "doubleCol", Types.DoubleType.get()),
          optional(6, "decimalCol", Types.DecimalType.of(10, 2)),
          optional(7, "stringCol", Types.StringType.get()),
          optional(8, "dateCol", Types.DateType.get()),
          optional(9, "timeCol", Types.TimeType.get()),
          optional(10, "timestampCol", Types.TimestampType.withoutZone()),
          optional(11, "fixedCol", Types.FixedType.ofLength(3)),
          optional(12, "binaryCol", Types.BinaryType.get()));

  protected Table createTable(String name, Schema schema, PartitionSpec spec) {
    return catalog.createTable(
        TableIdentifier.of("default", name), schema, spec, ImmutableMap.of());
  }

  protected void dropTable(String name) {
    catalog.dropTable(TableIdentifier.of("default", name));
  }

  protected GenericRecord createPrimitiveRecord(
      boolean booleanCol,
      int intCol,
      long longCol,
      float floatCol,
      double doubleCol,
      BigDecimal decimalCol,
      String stringCol,
      LocalDate dateCol,
      LocalTime timeCol,
      LocalDateTime timestampCol,
      byte[] fixedCol,
      ByteBuffer binaryCol) {
    GenericRecord record = GenericRecord.create(PRIMITIVE_SCHEMA);
    record.set(0, booleanCol);
    record.set(1, intCol);
    record.set(2, longCol);
    record.set(3, floatCol);
    record.set(4, doubleCol);
    record.set(5, decimalCol);
    record.set(6, stringCol);
    record.set(7, dateCol);
    record.set(8, timeCol);
    record.set(9, timestampCol);
    record.set(10, fixedCol);
    record.set(11, binaryCol);
    return record;
  }

  private GenericRecord createNestedRecord(Long longCol, Double doubleCol) {
    GenericRecord record = GenericRecord.create(NESTED_SCHEMA);
    GenericRecord nested = GenericRecord.create(NESTED_STRUCT_TYPE);
    GenericRecord leaf = GenericRecord.create(LEAF_STRUCT_TYPE);
    leaf.set(0, longCol);
    leaf.set(1, doubleCol);
    nested.set(0, leaf);
    record.set(0, nested);
    return record;
  }

  @Test
  public void testPrimitiveColumns() throws Exception {
    String tableName = "primitiveColumns";
    Table table = createTable(tableName, PRIMITIVE_SCHEMA, PartitionSpec.unpartitioned());

    List<Record> records =
        Lists.newArrayList(
            createPrimitiveRecord(
                false,
                1,
                1L,
                0,
                1.0D,
                new BigDecimal("1.00"),
                "1",
                DateTimeUtil.dateFromDays(1),
                DateTimeUtil.timeFromMicros(1),
                DateTimeUtil.timestampFromMicros(1L),
                Base64.getDecoder().decode("1111"),
                ByteBuffer.wrap(Base64.getDecoder().decode("1111"))),
            createPrimitiveRecord(
                true,
                2,
                2L,
                0,
                2.0D,
                new BigDecimal("2.00"),
                "2",
                DateTimeUtil.dateFromDays(2),
                DateTimeUtil.timeFromMicros(2),
                DateTimeUtil.timestampFromMicros(2L),
                Base64.getDecoder().decode("2222"),
                ByteBuffer.wrap(Base64.getDecoder().decode("2222"))));

    DataFile dataFile =
        FileHelpers.writeDataFile(table, Files.localOutput(temp.newFile()), records);
    table.newAppend().appendFile(dataFile).commit();

    Dataset<Row> df = spark.read().format("iceberg").load("default." + tableName + ".files");

    List<Row> rows = df.collectAsList();
    Assert.assertEquals("Expected only one data file", 1, rows.size());
    Row row = rows.get(0);
    String[] primitiveColumns =
        new String[] {
          "booleanCol",
          "intCol",
          "longCol",
          "floatCol",
          "doubleCol",
          "decimalCol",
          "stringCol",
          "dateCol",
          "timeCol",
          "timestampCol",
          "fixedCol",
          "binaryCol"
        };
    checkMetric(
        row,
        "column_size",
        primitiveColumns,
        l -> Assert.assertTrue("Column size should be greater than 0", l > 0));
    checkMetric(
        row,
        "value_count",
        primitiveColumns,
        l -> Assert.assertEquals("Value count should be 2", l.longValue(), 2L));
    checkMetric(
        row,
        "null_value_count",
        primitiveColumns,
        l -> Assert.assertEquals("Null value count should be 0", l.longValue(), 0L));
    checkMetric(
        row,
        "nan_value_count",
        new String[] {"floatCol", "doubleCol"},
        l -> Assert.assertEquals("Nan value count should be 0", l.longValue(), 0L));

    checkMetricValues(
        row,
        "lower_bound",
        ImmutableMap.ofEntries(
            Maps.immutableEntry("booleanCol", "false"),
            Maps.immutableEntry("stringCol", "1"),
            Maps.immutableEntry("intCol", "1"),
            Maps.immutableEntry("longCol", "1"),
            Maps.immutableEntry("floatCol", "0.0"),
            Maps.immutableEntry("doubleCol", "1.0"),
            Maps.immutableEntry("decimalCol", "1.00"),
            Maps.immutableEntry("binaryCol", "1111"),
            Maps.immutableEntry("fixedCol", "1111"),
            Maps.immutableEntry("dateCol", "1970-01-02"),
            Maps.immutableEntry("timeCol", "00:00:00.000001"),
            Maps.immutableEntry("timestampCol", "1970-01-01T00:00:00.000001")));

    checkMetricValues(
        row,
        "upper_bound",
        ImmutableMap.ofEntries(
            Maps.immutableEntry("booleanCol", "true"),
            Maps.immutableEntry("stringCol", "2"),
            Maps.immutableEntry("intCol", "2"),
            Maps.immutableEntry("longCol", "2"),
            Maps.immutableEntry("floatCol", "0.0"),
            Maps.immutableEntry("doubleCol", "2.0"),
            Maps.immutableEntry("decimalCol", "2.00"),
            Maps.immutableEntry("binaryCol", "2222"),
            Maps.immutableEntry("fixedCol", "2222"),
            Maps.immutableEntry("dateCol", "1970-01-03"),
            Maps.immutableEntry("timeCol", "00:00:00.000002"),
            Maps.immutableEntry("timestampCol", "1970-01-01T00:00:00.000002")));
  }

  @Test
  public void testSelect() throws Exception {
    String tableName = "testSelect";
    Table table = createTable(tableName, PRIMITIVE_SCHEMA, PartitionSpec.unpartitioned());

    List<Record> records =
        Lists.newArrayList(
            createPrimitiveRecord(
                false,
                1,
                1L,
                0,
                1.0D,
                new BigDecimal("1.00"),
                "1",
                DateTimeUtil.dateFromDays(1),
                DateTimeUtil.timeFromMicros(1),
                DateTimeUtil.timestampFromMicros(1L),
                Base64.getDecoder().decode("1111"),
                ByteBuffer.wrap(Base64.getDecoder().decode("1111"))),
            createPrimitiveRecord(
                true,
                2,
                2L,
                0,
                2.0D,
                new BigDecimal("2.00"),
                "2",
                DateTimeUtil.dateFromDays(2),
                DateTimeUtil.timeFromMicros(2),
                DateTimeUtil.timestampFromMicros(2L),
                Base64.getDecoder().decode("2222"),
                ByteBuffer.wrap(Base64.getDecoder().decode("2222"))));

    DataFile dataFile =
        FileHelpers.writeDataFile(table, Files.localOutput(temp.newFile()), records);
    table.newAppend().appendFile(dataFile).commit();

    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .load("default." + tableName + ".files")
            .select(new Column("file_path"), functions.map_keys(new Column("readable_metrics")));

    List<Row> rows = df.collectAsList();
    Assert.assertEquals("Expected only one data file", 1, rows.size());
    Row row = rows.get(0);
    Set<String> expectedKeys =
        ImmutableSet.of(
            "booleanCol",
            "intCol",
            "longCol",
            "floatCol",
            "doubleCol",
            "decimalCol",
            "stringCol",
            "dateCol",
            "timeCol",
            "timestampCol",
            "fixedCol",
            "binaryCol");
    checkCollectionValues(row, "map_keys(readable_metrics)", expectedKeys);
  }

  @Test
  public void testNullNanValues() throws Exception {
    String tableName = "testNullNanValues";
    Table table = createTable(tableName, PRIMITIVE_SCHEMA, PartitionSpec.unpartitioned());

    List<Record> records =
        Lists.newArrayList(
            createPrimitiveRecord(
                false, 0, 0, Float.NaN, Double.NaN, null, "0", null, null, null, null, null),
            createPrimitiveRecord(
                false,
                0,
                1,
                Float.NaN,
                1.0,
                new BigDecimal("1.00"),
                "1",
                null,
                null,
                null,
                null,
                null));

    DataFile dataFile =
        FileHelpers.writeDataFile(table, Files.localOutput(temp.newFile()), records);
    table.newAppend().appendFile(dataFile).commit();

    Dataset<Row> df = spark.read().format("iceberg").load("default." + tableName + ".files");

    List<Row> rows = df.collectAsList();
    Assert.assertEquals("Expected only one data file", 1, rows.size());
    Row row = rows.get(0);

    checkMetricValues(
        row,
        "null_value_count",
        ImmutableMap.ofEntries(
            Maps.immutableEntry("booleanCol", 0L),
            Maps.immutableEntry("stringCol", 0L),
            Maps.immutableEntry("intCol", 0L),
            Maps.immutableEntry("longCol", 0L),
            Maps.immutableEntry("floatCol", 0L),
            Maps.immutableEntry("doubleCol", 0L),
            Maps.immutableEntry("decimalCol", 1L),
            Maps.immutableEntry("binaryCol", 2L),
            Maps.immutableEntry("fixedCol", 2L),
            Maps.immutableEntry("dateCol", 2L),
            Maps.immutableEntry("timeCol", 2L),
            Maps.immutableEntry("timestampCol", 2L)));

    Map<String, Long> expectedNanValues = Maps.newHashMap();
    expectedNanValues.put("booleanCol", null);
    expectedNanValues.put("stringCol", null);
    expectedNanValues.put("intCol", null);
    expectedNanValues.put("longCol", null);
    expectedNanValues.put("floatCol", 2L);
    expectedNanValues.put("doubleCol", 1L);
    expectedNanValues.put("decimalCol", null);
    expectedNanValues.put("binaryCol", null);
    expectedNanValues.put("fixedCol", null);
    expectedNanValues.put("dateCol", null);
    expectedNanValues.put("timeCol", null);
    expectedNanValues.put("timestampCol", null);

    checkMetricValues(row, "nan_value_count", expectedNanValues);
  }

  @Test
  public void testNestedValues() throws Exception {
    String tableName = "testNestedValues";
    Table table = createTable(tableName, NESTED_SCHEMA, PartitionSpec.unpartitioned());

    List<Record> records =
        Lists.newArrayList(
            createNestedRecord(0L, 0.0),
            createNestedRecord(1L, Double.NaN),
            createNestedRecord(null, null));
    DataFile dataFile =
        FileHelpers.writeDataFile(table, Files.localOutput(temp.newFile()), records);
    table.newAppend().appendFile(dataFile).commit();

    Dataset<Row> df = spark.read().format("iceberg").load("default." + tableName + ".files");
    List<Row> rows = df.collectAsList();
    Assert.assertEquals("Expected only one data file", 1, rows.size());
    Row row = rows.get(0);

    String[] nestedColumns =
        new String[] {
          "nestedStructCol.leafStructCol.leafDoubleCol", "nestedStructCol.leafStructCol.leafLongCol"
        };
    checkMetric(
        row,
        "column_size",
        nestedColumns,
        l -> Assert.assertTrue("Column size should be greater than 0", l > 0));
    checkMetric(
        row,
        "value_count",
        nestedColumns,
        l -> Assert.assertEquals("Value count should be 3", l.longValue(), 3L));
    checkMetric(
        row,
        "null_value_count",
        nestedColumns,
        l -> Assert.assertEquals("Null value count should be 1", l.longValue(), 1L));

    Map<String, Long> expectedNanValues = Maps.newHashMap();
    expectedNanValues.put("nestedStructCol.leafStructCol.leafDoubleCol", 1L);
    expectedNanValues.put("nestedStructCol.leafStructCol.leafLongCol", null);
    checkMetricValues(row, "nan_value_count", expectedNanValues);

    checkMetricValues(
        row,
        "lower_bound",
        ImmutableMap.of(
            "nestedStructCol.leafStructCol.leafLongCol", "0",
            "nestedStructCol.leafStructCol.leafDoubleCol", "0.0"));
    checkMetricValues(
        row,
        "upper_bound",
        ImmutableMap.of(
            "nestedStructCol.leafStructCol.leafLongCol", "1",
            "nestedStructCol.leafStructCol.leafDoubleCol", "0.0"));
  }

  private <T> void checkCollectionValues(Row row, String columnName, Set<T> expectedValues) {
    Set<Long> actualValues =
        Sets.newHashSet(JavaConverters.asJavaCollection(row.getAs(columnName)));
    Assert.assertEquals("Collection values should match", expectedValues, actualValues);
  }

  private void checkMetric(Row row, String metricName, String[] columns, Consumer<Long> check) {

    scala.collection.Map<String, Row> metrics = row.getAs("readable_metrics");
    for (String column : columns) {
      Option<Row> rowOption = metrics.get(column);
      Assert.assertTrue("Missing metric for column: " + column, rowOption.isDefined());
      Long metric = rowOption.get().getAs(metricName);
      check.accept(metric);
    }
  }

  private <T> void checkMetricValues(Row row, String metricName, Map<String, T> expected) {
    scala.collection.Map<String, Row> metrics = row.getAs("readable_metrics");
    Assert.assertEquals("Size does not match for " + metricName, expected.size(), metrics.size());
    for (String column : expected.keySet()) {
      Option<Row> rowOption = metrics.get(column);
      Assert.assertTrue("Missing metric for column: " + column, rowOption.isDefined());
      Assert.assertEquals(
          "Values do not match for " + column,
          expected.get(column),
          rowOption.get().getAs(metricName));
    }
  }
}
