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
import scala.util.control.NonFatal

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
        s"[VERDICT] Replica 1 (idx$replicaIndex1) is CORRECT, Replica 2 (idx$replicaIndex2) is BYZANTINE"
      case "REPLICA_2_CORRECT" =>
        s"[VERDICT] Replica 2 (idx$replicaIndex2) is CORRECT, Replica 1 (idx$replicaIndex1) is BYZANTINE"
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
    val pairIndex: Int,  // replica pair k = index / 2: the key the driver files this verdict under
    val excludedExecutors: Set[String],
    val replicaIndex1: Int,
    val replicaIndex2: Int,
    val replicaHash1: String,
    val replicaHash2: String,
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None,
    val targetRange: Option[(Long, Long)] = None,  // chunk verification: UIDs [lo, hi)
    val chunkBoxes: Option[(Long, Long)] = None)   // (box of replicaIndex1, box of replicaIndex2)
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
    // This task's own context runs a full-task recompute below: a verifier, not a replica
    context match {
      case c: TaskContextImpl => c.fullTaskVerifier = true
      case _ =>
    }
    if (targetRange.isDefined && chunkBoxes.isDefined) return runChunkVerification(context)

    logInfo(s"[VERIFICATION] Running full-task verification for partition $partitionId on executor ${context.taskAttemptId()}")
    logInfo(s"[VERIFICATION] Excluded executors: $excludedExecutors")

    // FULL-TASK MODE: Compare full result hashes
    logInfo(s"[VERIFICATION] Full-task mode")
    logInfo(s"[VERIFICATION] Replica 1 (idx$replicaIndex1) hash: $replicaHash1")
    logInfo(s"[VERIFICATION] Replica 2 (idx$replicaIndex2) hash: $replicaHash2")

    // Start timer for full-task verification
    val fullTaskStartTime = System.nanoTime()

    val result = try {
      originalTask.runTask(context)
    } catch {
      case t: Throwable =>
        logWarning(s"[VERIFICATION] originalTask.runTask failed for partition $partitionId: ${t.getMessage}")
        return VerificationVerdict(stageId, partitionId, replicaIndex1, replicaIndex2,
          "ERROR", replicaHash1, replicaHash2, "NEITHER_MATCH")
    }

    // Log full-task verification time
    val fullTaskTime = (System.nanoTime() - fullTaskStartTime) / 1e9
    logWarning(s"[PERF-FULLTASK] Stage $stageId partition $partitionId: ${fullTaskTime}s")

    // Use same hashing logic as Executor.scala for consistency
    val verifierHash = if (originalTask.isInstanceOf[ShuffleMapTask]) {
      // For ShuffleMapTask: Hash block sizes (deterministic across replicas)
      // MapStatus includes BlockManagerId which is non-deterministic
      val mapStatus = result.asInstanceOf[MapStatus]

      // Exactly one size per reduce partition (see ShuffleMapTask.blockSizesForHash)
      val blockSizes = originalTask.asInstanceOf[ShuffleMapTask].blockSizesForHash(mapStatus)
      val sizeString = blockSizes.mkString(",")
      logInfo(s"[VERIFICATION SHUFFLE HASH] Hashing ${blockSizes.length} block sizes: ${sizeString.take(200)}...")
      TaskResultVerificationManager.computeTaskResultHash(
        java.nio.ByteBuffer.wrap(sizeString.getBytes("UTF-8"))
      )
    } else {
      // For ResultTask: Hash the full serialized result (current behavior)
      TaskResultVerificationManager.computeTaskResultHash(result)
    }

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

  /**
   * Chunk verification: recompute only the UIDs [lo, hi) up to the replicas' logging level, sum
   * their terms into one box, and compare it with the two replicas' boxes for that chunk. The
   * action (func) never runs. Anything unsure returns NEITHER_MATCH, which the driver turns into
   * a full-task retry; it is never committed.
   */
  private def runChunkVerification(context: TaskContext): VerificationVerdict = {
    val (lo, hi) = targetRange.get
    val (boxA, boxB) = chunkBoxes.get
    val rangeCtx = new TaskContextImpl(
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
      targetRange = Some((lo, hi)))
    rangeCtx.rangeBox = new org.apache.spark.verification.RangeBox(lo, hi)
    val t0 = System.nanoTime()
    val verifierBox: Option[Long] =
      try {
        TaskContext.setTaskContext(rangeCtx)
        // Drain only the chunk: neither the action nor the shuffle writer runs
        val it: Iterator[_] = originalTask match {
          case rt: ResultTask[_, _] => rt.iteratorForVerification(rangeCtx)
          case smt: ShuffleMapTask => smt.iteratorForVerification(rangeCtx)
          case _ => null
        }
        if (it == null) None
        else {
          while (it.hasNext) it.next()
          Some(rangeCtx.rangeBox.sum)
        }
      } catch {
        case NonFatal(e) =>
          // Never fail the aux task: that would abort the real stage
          logWarning(s"[CHUNK] verification failed for partition $partitionId: ${e.getMessage}")
          None
      } finally {
        try rangeCtx.markTaskCompleted(None) catch {
          case NonFatal(e) => logWarning(s"[CHUNK] completion listeners failed: ${e.getMessage}")
        }
        TaskContext.setTaskContext(context)
      }
    logWarning(s"[PERF-CHUNK-VERIFY] Stage $stageId partition $partitionId: " +
      s"${(System.nanoTime() - t0) / 1e9}s (UIDs [$lo, $hi), ${rangeCtx.rangeBox.count} outputs)")
    val verdict = verifierBox match {
      case Some(v) if v == boxA && v != boxB => "REPLICA_1_CORRECT"
      case Some(v) if v == boxB && v != boxA => "REPLICA_2_CORRECT"
      case _ => "NEITHER_MATCH"
    }
    logWarning(s"[VERIFICATION] Chunk verdict: $verdict (verifier=${verifierBox.getOrElse("ERROR")} " +
      s"vs replica1=$boxA vs replica2=$boxB)")
    VerificationVerdict(stageId, partitionId, replicaIndex1, replicaIndex2,
      verifierBox.map(_.toString).getOrElse("ERROR"), boxA.toString, boxB.toString, verdict)
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
