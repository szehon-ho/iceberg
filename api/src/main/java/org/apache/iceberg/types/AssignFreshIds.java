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
package org.apache.iceberg.types;

import org.apache.iceberg.Schema;

class AssignFreshIds extends BaseAssignIds {
  private final Schema baseSchema;
  private final TypeUtil.NextID nextId;

  AssignFreshIds(TypeUtil.NextID nextId) {
    this.baseSchema = null;
    this.nextId = nextId;
  }

  /**
   * Replaces the ids in a schema with ids from a base schema, or uses nextId to assign a fresh ids.
   *
   * @param visitingSchema current schema that will have ids replaced (for id to name lookup)
   * @param baseSchema base schema to assign existing ids from
   * @param nextId new id assigner
   */
  AssignFreshIds(Schema visitingSchema, Schema baseSchema, TypeUtil.NextID nextId) {
    super(visitingSchema);
    this.baseSchema = baseSchema;
    this.nextId = nextId;
  }

  @Override
  protected int idFor(String fullName) {
    if (baseSchema != null && fullName != null) {
      Types.NestedField field = baseSchema.findField(fullName);
      if (field != null) {
        return field.fieldId();
      }
    }

    return nextId.get();
  }
}
