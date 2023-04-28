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

import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functionality used by RewritePositionDeleteFile Actions from different platforms to handle
 * commits.
 */
public class RewritePositionDeletesCommitManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(RewritePositionDeletesCommitManager.class);

  private final Table table;
  private final long startingSnapshotId;

  public RewritePositionDeletesCommitManager(Table table) {
    this.table = table;
    this.startingSnapshotId = table.currentSnapshot().snapshotId();
  }

  /**
   * Perform a commit operation on the table adding and removing files as required for this set of
   * file groups
   *
   * @param fileGroups fileSets to commit
   */
  public void commitFileGroups(Set<RewritePositionDeleteGroup> fileGroups) {
    Set<DeleteFile> rewrittenDeleteFiles = Sets.newHashSet();
    Set<DeleteFile> addedDeleteFiles = Sets.newHashSet();
    for (RewritePositionDeleteGroup group : fileGroups) {
      rewrittenDeleteFiles.addAll(group.rewrittenDeleteFiles());
      addedDeleteFiles.addAll(group.addedDeleteFiles());
    }

    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshotId);
    rewrite.rewriteFiles(
        ImmutableSet.of(), rewrittenDeleteFiles, ImmutableSet.of(), addedDeleteFiles);

    rewrite.commit();
  }

  /**
   * Clean up a specified file set by removing any files created for that operation, should not
   * throw any exceptions
   *
   * @param fileGroup group of files which has already been rewritten
   */
  public void abortFileGroup(RewritePositionDeleteGroup fileGroup) {
    Preconditions.checkState(
        fileGroup.addedDeleteFiles() != null, "Cannot abort a fileGroup that was not rewritten");

    Tasks.foreach(fileGroup.addedDeleteFiles())
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure((deleteFile, exc) -> LOG.warn("Failed to delete: {}", deleteFile.path(), exc))
        .run(deleteFile -> table.io().deleteFile(deleteFile.path().toString()));
  }

  public void commitOrClean(Set<RewritePositionDeleteGroup> rewriteGroups) {
    try {
      commitFileGroups(rewriteGroups);
    } catch (CommitStateUnknownException e) {
      LOG.error(
          "Commit state unknown for {}, cannot clean up files because they may have been committed successfully.",
          rewriteGroups,
          e);
      throw e;
    } catch (Exception e) {
      LOG.error("Cannot commit groups {}, attempting to clean up written files", rewriteGroups, e);
      rewriteGroups.forEach(this::abortFileGroup);
      throw e;
    }
  }

  /**
   * An async service which allows for committing multiple file groups as their rewrites complete.
   * The service also allows for partial-progress since commits can fail. Once the service has been
   * closed no new file groups should not be offered.
   *
   * @param rewritesPerCommit number of file groups to include in a commit
   * @return the service for handling commits
   */
  public CommitService service(int rewritesPerCommit) {
    return new CommitService(rewritesPerCommit);
  }

  public class CommitService extends BaseCommitService<RewritePositionDeleteGroup> {

    CommitService(int rewritesPerCommit) {
      super(table, rewritesPerCommit);
    }

    @Override
    protected void commitOrClean(Set<RewritePositionDeleteGroup> batch) {
      RewritePositionDeletesCommitManager.this.commitOrClean(batch);
    }

    @Override
    protected void abortFileGroup(RewritePositionDeleteGroup group) {
      RewritePositionDeletesCommitManager.this.abortFileGroup(group);
    }
  }
}
