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
package org.apache.iceberg.actions;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.actions.AnalyzeTable.Summary;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

public class BaseAnalyzeTableActionResult implements AnalyzeTable.Result {

  private static final Joiner NEW_LINE = Joiner.on("\n");

  private final List<Summary> summaries;

  public BaseAnalyzeTableActionResult(List<Summary> summaries) {
    this.summaries = summaries;
  }

  @Override
  public String toFormattedString() {
    return NEW_LINE.join(Iterables.transform(summaries, Summary::toFormattedString));
  }

  @Override
  public List<Summary> summaries() {
    return summaries;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Summary> T summary(Class<T> clazz) {
    List<Summary> matchingSummaries =
        summaries.stream().filter(clazz::isInstance).collect(Collectors.toList());

    if (matchingSummaries.size() > 1) {
      throw new IllegalArgumentException(
          "There are multiple matching summaries: " + matchingSummaries);
    }

    return matchingSummaries.isEmpty() ? null : (T) matchingSummaries.get(0);
  }
}
