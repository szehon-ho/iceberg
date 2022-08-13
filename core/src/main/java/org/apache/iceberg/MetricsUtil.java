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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsUtil {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsUtil.class);

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

  /**
   * Return a readable metrics map
   *
   * @param schema schema of original data table
   * @param namesById pre-computed map of all column ids in schema to readable name, see {@link
   *     org.apache.iceberg.types.TypeUtil#indexNameById(Types.StructType)}
   * @param contentFile content file with metrics
   * @return map of readable column name to column metric, of which the bounds are made readable
   */
  public static Map<String, StructLike> readableMetricsMap(
      Schema schema,
      Map<Integer, String> namesById,
      ContentFile<?> contentFile) {
    Map<String, StructLike> metricsStruct = Maps.newHashMapWithExpectedSize(namesById.size());

    Map<Integer, Long> columnSizes = contentFile.columnSizes();
    Map<Integer, Long> valueCounts = contentFile.valueCounts();
    Map<Integer, Long> nullValueCounts = contentFile.nullValueCounts();
    Map<Integer, Long> nanValueCounts = contentFile.nanValueCounts();
    Map<Integer, ByteBuffer> lowerBounds = contentFile.lowerBounds();
    Map<Integer, ByteBuffer> upperBounds = contentFile.upperBounds();

    for (int id : namesById.keySet()) {
      Types.NestedField field = schema.findField(id);
      if (field.type().isPrimitiveType()) {
        // Iceberg stores metrics only for primitive types
        String colName = namesById.get(id);
        ReadableMetricsStruct struct =
            new ReadableMetricsStruct(
                columnSizes == null ? null : columnSizes.get(id),
                valueCounts == null ? null : valueCounts.get(id),
                nullValueCounts == null ? null : nullValueCounts.get(id),
                nanValueCounts == null ? null : nanValueCounts.get(id),
                lowerBounds == null ? null : convertToReadable(field, lowerBounds.get(id)),
                upperBounds == null ? null : convertToReadable(field, upperBounds.get(id)));
        metricsStruct.put(colName, struct);
      }
    }
    return metricsStruct;
  }

  public static String convertToReadable(Types.NestedField field, ByteBuffer value) {
    if (field == null || value == null) {
      return null;
    }
    try {
      return Transforms.identity(field.type())
          .toHumanString(Conversions.fromByteBuffer(field.type(), value));
    } catch (Exception e) {
      LOG.warn("Error converting metric to readable form", e);
      return null;
    }
  }

  public static class ReadableMetricsStruct implements StructLike {

    private final Long columnSize;
    private final Long valueCount;
    private final Long nullValueCount;
    private final Long nanValueCount;
    private final String lowerBound;
    private final String upperBound;

    public ReadableMetricsStruct(
        Long columnSize,
        Long valueCount,
        Long nullValueCount,
        Long nanValueCount,
        String lowerBound,
        String upperBound) {
      this.columnSize = columnSize;
      this.valueCount = valueCount;
      this.nullValueCount = nullValueCount;
      this.nanValueCount = nanValueCount;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }

    @Override
    public int size() {
      return 6;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      Object value;
      switch (pos) {
        case 0:
          value = columnSize;
          break;
        case 1:
          value = valueCount;
          break;
        case 2:
          value = nullValueCount;
          break;
        case 3:
          value = nanValueCount;
          break;
        case 4:
          value = lowerBound;
          break;
        case 5:
          value = upperBound;
          break;
        default:
          throw new IllegalArgumentException(String.format("Invalid position %d", pos));
      }
      if (value == null) {
        return null;
      } else {
        return javaClass.cast(value);
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("ReadableMetricsStruct is read only");
    }
  }
}
