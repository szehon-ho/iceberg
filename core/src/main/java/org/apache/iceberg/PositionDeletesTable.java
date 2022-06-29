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

import java.util.Map;
import org.apache.iceberg.io.DeleteSchemaUtil;

public class PositionDeletesTable extends BaseMetadataTable {

  PositionDeletesTable(TableOperations ops, Table table) {
    super(ops, table, table.name() + ".position_deletes");
  }

  PositionDeletesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.POSITION_DELETES;
  }

  @Override
  public Scan newScan() {
    return new PositionDeletesTableScan(operations(), table(), schema());
  }

  @Override
  public Schema schema() {
    return DeleteSchemaUtil.metadataTableSchema(table());
  }

  @Override
  public PartitionSpec spec() {
    return table().spec();
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    return table().specs();
  }
}
