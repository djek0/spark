/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.util.Properties
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.MerkleTree

/**
 * Result of a Merkle tree build task.
 * Contains the built tree and metadata.
 */
case class MerkleTreeBuildResult(
  stageId: Int,
  taskIndex: Int,
  partitionId: Int,
  tree: MerkleTree,
  leafCount: Int,
  rootHash: Int,
  buildTimeMs: Long
) extends Serializable

/**
 * A special task used to build Merkle trees on executors.
 * This task reads the finals file from shared storage and builds the Merkle tree
 * on the executor that originally produced the results, then returns the tree to the driver.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage
 * @param taskIndex the task index whose results we're building a tree for
 * @param partitionId index of the partition
 * @param finalsFilePath absolute path to the finals file in shared storage
 * @param isBinary whether the finals file is in binary format
 * @param localProperties copy of thread-local properties
 * @param serializedTaskMetrics serialized TaskMetrics
 * @param jobId id of the job
 * @param appId id of the app
 * @param appAttemptId attempt id of the app
 */
private[spark] class MerkleTreeBuildTask(
    stageId: Int,
    stageAttemptId: Int,
    val taskIndex: Int,
    partitionId: Int,
    val finalsFilePath: String,
    val isBinary: Boolean,
    localProperties: Properties = new Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None)
  extends Task[MerkleTreeBuildResult](
    stageId, 
    stageAttemptId, 
    partitionId, 
    localProperties, 
    serializedTaskMetrics,
    jobId, 
    appId, 
    appAttemptId, 
    isBarrier = false)
  with Serializable with Logging {

  /**
   * Run the Merkle tree build task by reading the finals file and building the tree.
   * This ensures the tree building happens on the executor, not the driver.
   */
  override def runTask(context: TaskContext): MerkleTreeBuildResult = {
    val startTime = System.currentTimeMillis()
    
    logInfo(s"[MERKLE BUILD] Building Merkle tree for stage $stageId, task index $taskIndex, " +
      s"partition $partitionId on executor ${context.taskAttemptId()}")
    logInfo(s"[MERKLE BUILD] Reading finals file: $finalsFilePath")
    
    // Build tree from finals file (using existing method)
    val tree = MerkleTree.buildFromFinalsFile(finalsFilePath, isBinary)
    
    val buildTimeMs = System.currentTimeMillis() - startTime
    
    logInfo(s"[MERKLE BUILD] Tree built: ${tree.leafCount} leaves, height=${tree.height}, " +
      s"rootHash=${tree.rootHash}, buildTime=${buildTimeMs}ms")
    
    // Return result with tree and metadata
    MerkleTreeBuildResult(
      stageId = stageId,
      taskIndex = taskIndex,
      partitionId = partitionId,
      tree = tree,
      leafCount = tree.leafCount,
      rootHash = tree.rootHash,
      buildTimeMs = buildTimeMs
    )
  }

  /**
   * No preferred locations - let Spark decide where to run this.
   * In the future, we could prefer the executor that originally ran taskIndex.
   */
  override def preferredLocations: Seq[TaskLocation] = Seq.empty

  /**
   * Identify this as a Merkle tree build task.
   */
  def isMerkleTreeBuildTask: Boolean = true
}
