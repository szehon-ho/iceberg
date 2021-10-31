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

package org.apache.spark.sql.catalyst.utils

import org.apache.iceberg.common.DynConstructors
import org.apache.iceberg.spark.Spark3VersionUtil
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

object PlanUtils {
  def isIcebergRelation(plan: LogicalPlan): Boolean = {
    def isIcebergTable(relation: DataSourceV2Relation): Boolean = relation.table match {
      case _: SparkTable => true
      case _ => false
    }

    plan match {
      case s: SubqueryAlias => isIcebergRelation(s.child)
      case r: DataSourceV2Relation => isIcebergTable(r)
      case _ => false
    }
  }

  private val repartitionByExpressionCtor: DynConstructors.Ctor[RepartitionByExpression] =
    DynConstructors.builder()
      .impl(classOf[RepartitionByExpression],
        classOf[Seq[Expression]],
        classOf[LogicalPlan],
        classOf[Option[Int]])
      .impl(classOf[RepartitionByExpression],
        classOf[Seq[Expression]],
        classOf[LogicalPlan],
        Integer.TYPE)
      .build()

  def createRepartitionByExpression(
      partitionExpressions: Seq[Expression],
      child: LogicalPlan,
      numPartitions: Int): RepartitionByExpression = {
    if (Spark3VersionUtil.isSpark30) {
      repartitionByExpressionCtor.newInstance(partitionExpressions, child, Integer.valueOf(numPartitions))
    } else {
      // Do not pass numPartitions because it is set automatically for AQE
      repartitionByExpressionCtor.newInstance(partitionExpressions, child, None)
    }
  }
}
