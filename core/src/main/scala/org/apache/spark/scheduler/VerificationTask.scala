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

/**
 * Result of a Byzantine verification task.
 * Contains the verdict about which replica is correct.
 */
case class VerificationVerdict(
  stageId: Int,
  partitionId: Int,
  replicaIndex1: Int,
  replicaIndex2: Int,
  verifierHash: String,
  replicaHash1: String,
  replicaHash2: String,
  verdict: String  // "REPLICA_1_CORRECT", "REPLICA_2_CORRECT", "BOTH_MATCH", "NEITHER_MATCH"
) extends Serializable {
  
  def logVerdict(): String = {
    verdict match {
      case "REPLICA_1_CORRECT" =>
        s"[✓] VERDICT: Replica 1 (idx$replicaIndex1) is CORRECT, Replica 2 (idx$replicaIndex2) is BYZANTINE"
      case "REPLICA_2_CORRECT" =>
        s"[✓] VERDICT: Replica 2 (idx$replicaIndex2) is CORRECT, Replica 1 (idx$replicaIndex1) is BYZANTINE"
      case "BOTH_MATCH" =>
        s"[?] UNEXPECTED: Both replicas match verifier - possible race condition"
      case "NEITHER_MATCH" =>
        s"[X] CRITICAL: Verifier differs from BOTH replicas - system error!"
      case _ =>
        s"[?] UNKNOWN VERDICT: $verdict"
    }
  }
}

/**
 * A special task used for Byzantine fault verification.
 * This task recomputes a partition on a third executor (not the ones that ran the original replicas)
 * to determine which replica produced the correct result.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage
 * @param partitionId index of the partition to verify
 * @param originalTask the original task to recompute
 * @param excludedExecutors set of executor IDs that should NOT run this verification task
 * @param replicaIndex1 index of first replica
 * @param replicaIndex2 index of second replica
 * @param replicaHash1 hash of first replica's result
 * @param replicaHash2 hash of second replica's result
 * @param localProperties copy of thread-local properties
 * @param serializedTaskMetrics serialized TaskMetrics
 * @param jobId id of the job
 * @param appId id of the app
 * @param appAttemptId attempt id of the app
 */
private[spark] class VerificationTask(
    stageId: Int,
    stageAttemptId: Int,
    partitionId: Int,
    val originalTask: Task[_],
    val excludedExecutors: Set[String],
    val replicaIndex1: Int,
    val replicaIndex2: Int,
    val replicaHash1: String,
    val replicaHash2: String,
    val targetElementId: Option[Long] = None,  // For single-element verification
    val merkleLeafHashes: Option[(Int, Int)] = None,  // (leafHash1, leafHash2) for Merkle mode
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None)
  extends Task[VerificationVerdict](
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
   * Run the verification task by executing the original task's computation.
   * Computes the result, hashes it, compares with replica hashes, and returns verdict.
   * This ensures all verification logic runs on the executor, not the driver.
   */
  override def runTask(context: TaskContext): VerificationVerdict = {
    val modeStr = targetElementId.map(id => s"single-element (UID $id)").getOrElse("full-task")
    logInfo(s"[VERIFICATION] Running verification task for partition $partitionId on executor ${context.taskAttemptId()} - mode: $modeStr")
    logInfo(s"[VERIFICATION] Excluded executors: $excludedExecutors")
    
    // Choose verification strategy based on mode
    (targetElementId, merkleLeafHashes) match {
      case (Some(uid), Some((leafHash1, leafHash2))) =>
        // SINGLE-ELEMENT MODE: Compare leaf hashes
        logInfo(s"[EXECUTOR RECOMPUTE] Single-element mode: UID $uid")
        logInfo(s"[EXECUTOR RECOMPUTE] Replica 1 (idx$replicaIndex1) leaf hash: $leafHash1")
        logInfo(s"[EXECUTOR RECOMPUTE] Replica 2 (idx$replicaIndex2) leaf hash: $leafHash2")
        
        // Create TaskContext with targetElementId for filtering
        val filteredContext = new org.apache.spark.TaskContextImpl(
          stageId = context.stageId(),
          stageAttemptNumber = context.stageAttemptNumber(),
          partitionId = context.partitionId(),
          taskAttemptId = context.taskAttemptId(),
          attemptNumber = context.attemptNumber(),
          taskIndex = context.taskIndex(),
          taskMemoryManager = context.taskMemoryManager(),
          localProperties = localProperties,
          metricsSystem = SparkEnv.get.metricsSystem,
          taskMetrics = context.taskMetrics(),
          resources = context.resources(),
          targetElementId = Some(uid)
        )
        
        TaskContext.setTaskContext(filteredContext)
        val result = originalTask.runTask(filteredContext)
        
        // Extract single element and compute leaf hash (same as SafeWriter format)
        val elementAtUid = result match {
          case arr: Array[_] if arr.length > 0 => arr(0)
          case other => other
        }
        val verifierValue = elementAtUid match {
          case arr: Array[_] => arr.mkString("[", ",", "]")
          case other => other.toString
        }
        val verifierLeafHash = verifierValue.hashCode
        
        logInfo(s"[VERIFICATION] Verifier leaf hash: $verifierLeafHash")
        
        // Compare leaf hashes
        val verdict = (verifierLeafHash == leafHash1, verifierLeafHash == leafHash2) match {
          case (true, false) => "REPLICA_1_CORRECT"
          case (false, true) => "REPLICA_2_CORRECT"
          case (true, true) => "BOTH_MATCH"
          case (false, false) => "NEITHER_MATCH"
        }
        
        logInfo(s"[VERIFICATION] Verdict: $verdict")
        
        VerificationVerdict(
          stageId = stageId,
          partitionId = partitionId,
          replicaIndex1 = replicaIndex1,
          replicaIndex2 = replicaIndex2,
          verifierHash = verifierLeafHash.toString,
          replicaHash1 = leafHash1.toString,
          replicaHash2 = leafHash2.toString,
          verdict = verdict
        )
        
      case _ =>
        // FULL-TASK MODE: Compare full result hashes
        logInfo(s"[VERIFICATION] Full-task mode")
        logInfo(s"[VERIFICATION] Replica 1 (idx$replicaIndex1) hash: $replicaHash1")
        logInfo(s"[VERIFICATION] Replica 2 (idx$replicaIndex2) hash: $replicaHash2")
        
        val result = originalTask.runTask(context)
        val verifierHash = TaskResultVerificationManager.computeTaskResultHash(result)
        logInfo(s"[VERIFICATION] Verifier hash: $verifierHash")
        
        val verdict = (verifierHash == replicaHash1, verifierHash == replicaHash2) match {
          case (true, false) => "REPLICA_1_CORRECT"
          case (false, true) => "REPLICA_2_CORRECT"
          case (true, true) => "BOTH_MATCH"
          case (false, false) => "NEITHER_MATCH"
        }
        
        logInfo(s"[VERIFICATION] Verdict: $verdict")
        
        VerificationVerdict(
          stageId = stageId,
          partitionId = partitionId,
          replicaIndex1 = replicaIndex1,
          replicaIndex2 = replicaIndex2,
          verifierHash = verifierHash,
          replicaHash1 = replicaHash1,
          replicaHash2 = replicaHash2,
          verdict = verdict
        )
    }
  }

  /**
   * No preferred locations - verification task can run anywhere (except excluded executors).
   */
  override def preferredLocations: Seq[TaskLocation] = Seq.empty

  /**
   * Identify this as a verification task.
   */
  def isVerificationTask: Boolean = true
}
