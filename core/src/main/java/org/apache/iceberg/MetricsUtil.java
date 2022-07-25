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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

public class MetricsUtil {

  private MetricsUtil() {}

  /**
   * Construct mapping relationship between column id to NaN value counts from input metrics and
   * metrics config.
   */
  public static Map<Integer, Long> createNanValueCounts(
      Stream<FieldMetrics<?>> fieldMetrics, MetricsConfig metricsConfig, Schema inputSchema) {
    Preconditions.checkNotNull(metricsConfig, "metricsConfig is required");

    if (fieldMetrics == null || inputSchema == null) {
      return Maps.newHashMap();
    }

    return fieldMetrics
        .filter(
            metrics ->
                metricsMode(inputSchema, metricsConfig, metrics.id()) != MetricsModes.None.get())
        .collect(Collectors.toMap(FieldMetrics::id, FieldMetrics::nanValueCount));
  }

  /** Extract MetricsMode for the given field id from metrics config. */
  public static MetricsModes.MetricsMode metricsMode(
      Schema inputSchema, MetricsConfig metricsConfig, int fieldId) {
    Preconditions.checkNotNull(inputSchema, "inputSchema is required");
    Preconditions.checkNotNull(metricsConfig, "metricsConfig is required");

    String columnName = inputSchema.findColumnName(fieldId);
    return metricsConfig.columnMode(columnName);
  }

  // Utilities for Displaying Metrics

  static final Types.NestedField COLUMN_SIZES_METRICS =
      optional(
          300,
          "column_sizes_metrics",
          Types.MapType.ofRequired(301, 302, Types.StringType.get(), Types.LongType.get()),
          "Map of column name to total size on disk");
  static final Types.NestedField VALUE_COUNT_METRICS =
      optional(
          303,
          "value_counts_metrics",
          Types.MapType.ofRequired(304, 305, Types.StringType.get(), Types.LongType.get()),
          "Map of column name to total count, including null and NaN");
  static final Types.NestedField NULL_VALUE_COUNTS_METRICS =
      optional(
          306,
          "null_value_counts_metrics",
          Types.MapType.ofRequired(307, 308, Types.StringType.get(), Types.LongType.get()),
          "Map of column name to null value count");
  static final Types.NestedField NAN_VALUE_COUNTS_METRICS =
      optional(
          309,
          "nan_value_counts_metrics",
          Types.MapType.ofRequired(310, 311, Types.StringType.get(), Types.LongType.get()),
          "Map of column name to number of NaN values in the column");
  static final Types.NestedField LOWER_BOUNDS_METRICS =
      optional(
          312,
          "lower_bounds_metrics",
          Types.MapType.ofRequired(313, 314, Types.StringType.get(), Types.StringType.get()),
          "Map of column name to lower bound in string format");
  static final Types.NestedField UPPER_BOUNDS_METRICS =
      optional(
          315,
          "upper_bounds_metrics",
          Types.MapType.ofRequired(316, 317, Types.StringType.get(), Types.StringType.get()),
          "Map of column name to upper bound in string format");
  public static final Schema METRICS_DISPLAY_SCHEMA =
      new Schema(
          COLUMN_SIZES_METRICS,
          VALUE_COUNT_METRICS,
          NULL_VALUE_COUNTS_METRICS,
          NAN_VALUE_COUNTS_METRICS,
          LOWER_BOUNDS_METRICS,
          UPPER_BOUNDS_METRICS);

  public static class Metric {
    private final String quotedName;
    private final Types.NestedField field;
    private final ByteBuffer value;

    Metric(String quotedName, Types.NestedField field, ByteBuffer value) {
      this.quotedName = quotedName;
      this.field = field;
      this.value = value;
    }

    String quotedName() {
      return quotedName;
    }

    boolean valid() {
      return quotedName != null && field != null && value != null;
    }

    Optional<String> convertToReadable() {
      try {
        return Optional.of(
            Transforms.identity(field.type())
                .toHumanString(Conversions.fromByteBuffer(field.type(), value)));
      } catch (Exception e) { // Ignore
        return Optional.empty();
      }
    }
  }

  /**
   * Convert map of Iceberg count metrics to readable metrics.
   *
   * @param counts map of Iceberg bounds metrics (column id, count)
   * @param quotedNameById map of Iceberg column id to full name
   * @return map of corresponding readable metrics (column full name, count)
   */
  public static Map<String, Long> readableCountsMetrics(
      Map<Integer, Long> counts, Map<Integer, String> quotedNameById) {
    if (counts == null) {
      return null;
    }
    return counts.entrySet().stream()
        .filter(e -> quotedNameById.get(e.getKey()) != null)
        .map(e -> Maps.immutableEntry(quotedNameById.get(e.getKey()), e.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Convert map of Iceberg bound metrics to readable metrics.
   *
   * @param boundsMetrics map of Iceberg bounds metrics (column id, byte value)
   * @param quotedNameById map of Iceberg column id to full name
   * @param schema Iceberg table schema
   * @return map of corresponding readable metrics (column full name, readable value)
   */
  public static Map<String, String> readableBoundsMetrics(
      Map<Integer, ByteBuffer> boundsMetrics, Map<Integer, String> quotedNameById, Schema schema) {
    if (boundsMetrics == null) {
      return null;
    }
    return boundsMetrics.entrySet().stream()
        .map(
            boundMetric ->
                new Metric(
                    quotedNameById.get(boundMetric.getKey()),
                    schema.findField(boundMetric.getKey()),
                    boundMetric.getValue()))
        .filter(Metric::valid)
        .map(metric -> Maps.immutableEntry(metric.quotedName(), metric.convertToReadable()))
        .filter(entry -> entry.getValue().isPresent()) // Error in converting to readable string
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get()));
  }
}
