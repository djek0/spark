package org.apache.spark.scheduler

import org.apache.spark.SparkEnv

import java.io.File
import java.nio.file.Files
import java.nio.ByteBuffer
import scala.util.Properties.envOrElse
import scala.collection.mutable.{HashMap, HashSet}
import org.apache.spark.internal.Logging
import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.FileFormatUtils
import org.apache.spark.rdd.UnsafeStageMarkers


object TaskResultVerificationManager extends Logging {

  // Reference to DAGScheduler for verdict callbacks
  private var dagScheduler: DAGScheduler = _
  
  def setDAGScheduler(scheduler: DAGScheduler): Unit = {
    dagScheduler = scheduler
  }

  // Maps taskId -> (stageId, taskIndex) where taskIndex is the array index (not taskId!)
  var tidToStageIndexInfo = new HashMap[Long, (Int, Int)]
  // Maps (stageId, taskIndex) -> resultHash
  var stageIndexToResultHash = new HashMap[(Int, Int), String]
  // Track verified partitions to prevent duplicate file verification
  var verifiedPartitions = new HashSet[(Int, Int)]()
  // Track partitions with verification in progress to prevent duplicate verification tasks
  var verificationInProgress = new HashSet[(Int, Int)]()
  // Store Task objects for driver recomputation (only original tasks, not verification tasks)
  var stageIndexToOriginalTask = new HashMap[(Int, Int), Task[_]]()
  // Track which executor and host ran which task (for host-level locality and third-executor verification)
  var stageIndexToExecutor = new HashMap[(Int, Int), (String, String)]()
  // Track Merkle tree build results from executors
  private val merkleTreeBuildResults = new HashMap[String, MerkleTreeBuildResult]()
  private val merkleTreeBuildLock = new Object()
  // Track partitions where consensus was found (hashes match)
  private val consensusFound = new scala.collection.mutable.HashSet[(Int, Int)]()

  // Sealed trait for Merkle tree build outcomes
  private sealed trait MerkleBuildOutcome
  private case class BothTreesBuilt(tree1: org.apache.spark.rdd.MerkleTree, tree2: org.apache.spark.rdd.MerkleTree) extends MerkleBuildOutcome
  private case class EarlyVerdict(correctIndex: Int, byzantineIndex: Int, reason: String) extends MerkleBuildOutcome
  private case class SizeMismatch(leafCount1: Int, leafCount2: Int, reason: String) extends MerkleBuildOutcome  // Empty vs non-empty partition
  private case class MerkleBuildFailure(reason: String) extends MerkleBuildOutcome  // Build failed (driver or executor)

  // Configuration: Enable third-executor verification
  private val useExecutorVerification = envOrElse("EXEC_VERIFICATION", "true").toBoolean
  // Configuration: Enable Merkle tree-based verification (find disagreeing element instead of full recompute)
  private val useMerkleVerification = envOrElse("MERKLE_VERIFICATION", "true").toBoolean
  // Configuration: Enable debug mode (verbose logging, file verification, Merkle tree persistence)
  private val debugMode = envOrElse("DEBUG_MODE", "false").toBoolean
  // NOTE: verificationTimeoutMs is UNUSED - actual timeout is in DAGScheduler (spark.verification.timeout)
  private val verificationTimeoutMs = envOrElse("VERIFICATION_TIMEOUT_MS", "5000").toInt
  // Merkle tree building timeout: should be much faster than original task (just reading + hashing)
  private val merkleTreeBuildTimeoutMs = envOrElse("MERKLE_BUILD_TIMEOUT_MS", "10000").toInt
  // Configuration: Build Merkle trees on driver instead of executors (for testing/debugging)
  private val buildMerkleTreesOnDriver = envOrElse("MERKLE_BUILD_ON_DRIVER", "false").toBoolean

  logInfo(s"[CONFIG] Executor verification enabled: $useExecutorVerification")
  logInfo(s"[CONFIG] Merkle tree verification enabled: $useMerkleVerification")
  logInfo(s"[CONFIG] Debug mode enabled: $debugMode")
  logInfo(s"[CONFIG] Verification timeout: ${verificationTimeoutMs}ms")
  logInfo(s"[CONFIG] Merkle tree build timeout: ${merkleTreeBuildTimeoutMs}ms")
  logInfo(s"[CONFIG] Build Merkle trees on driver: $buildMerkleTreesOnDriver")

  /**
   * Detects if a stage involves shuffle operations (reads OR writes shuffle).
   * Driver cannot execute such tasks due to missing TaskMemoryManager infrastructure.
   * 
   * @param stageId Stage ID to check
   * @return true if stage reads from shuffle OR is a ShuffleMapStage (writes shuffle)
   */
  private def stageReadsFromShuffle(stageId: Int): Boolean = {
    val stageOpt = dagScheduler.stageIdToStage.get(stageId)
    stageOpt match {
      case Some(stage) =>
        stage match {
          case _: ShuffleMapStage =>
            // ShuffleMapStage writes shuffle data - driver cannot execute (needs TaskMemoryManager)
            logInfo(s"[SHUFFLE DETECTION] Stage $stageId is ShuffleMapStage - WRITES shuffle (driver unsupported)")
            true
          case _: ResultStage =>
            // ResultStage may read from shuffle if it has parents
            val readsFromShuffle = stage.parents.nonEmpty
            if (readsFromShuffle) {
              logInfo(s"[SHUFFLE DETECTION] Stage $stageId is ResultStage with ${stage.parents.length} parent(s) - READS from shuffle")
            } else {
              logInfo(s"[SHUFFLE DETECTION] Stage $stageId is ResultStage with no parents - no shuffle operations")
            }
            readsFromShuffle
        }
      case None =>
        logWarning(s"[SHUFFLE DETECTION] Stage $stageId not found in stageIdToStage map")
        false
    }
  }

  /**
   * Register a task for verification and store it for potential driver recomputation.
   * Merged registration method to avoid HashMap duplication.
   * @param tid Task ID
   * @param indexStage Tuple of (stageId, taskIndex)
   * @param task Task object for recomputation
   * @param executorId Executor ID where task is running
   * @param host Host where task is running
   */
  def addNewRunningTask(tid: Long, indexStage: (Int, Int), task: Task[_], executorId: String, host: String): Unit = {
    if(tidToStageIndexInfo.contains(tid)){
      logDebug(s"Task $tid already registered in verification manager")
      return
    }
    logDebug(s"Registered task $tid with stage ${indexStage._1}, index ${indexStage._2}, executor $executorId, host $host")
    tidToStageIndexInfo(tid) = indexStage
    stageIndexToOriginalTask(indexStage) = task
    stageIndexToExecutor(indexStage) = (executorId, host)
    logDebug(s"Stored original task (${task.getClass.getSimpleName})")
  }

  /**
   * Shared hash computation method used by both executor and driver.
   * Ensures consistent hashing across all verification points.
   */
  def computeTaskResultHash(valueBytes: ByteBuffer): String = {
    java.util.Arrays.hashCode(valueBytes.array()).toLong.toString
  }

  def computeTaskResultHash(result: Any): String = {
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val valueBytes = ser.serialize(result)
    computeTaskResultHash(valueBytes)
  }

  /**
   * Check if consensus has been found (hashes match) for a partition.
   * Called by DAGScheduler batching logic to determine if async verification is needed.
   */
  def hasConsensus(stageId: Int, partitionId: Int): Boolean = {
    val result = consensusFound.contains((stageId, partitionId))
    if (result) {
      logDebug(s"[CONSENSUS CHECK] Found consensus flag for stage $stageId, partition $partitionId")
    }
    result
  }

  /**
   * Remove consensus flag after it has been used.
   * Called by DAGScheduler after processing the consensus result.
   */
  def cleanupConsensusFlag(stageId: Int, partitionId: Int): Unit = {
    consensusFound.remove((stageId, partitionId))
    logDebug(s"[CONSENSUS CLEANUP] Removed consensus flag for stage $stageId, partition $partitionId")
  }

  /**
   * Clean up task references for a completed partition to prevent memory leaks.
   * Called after verification completes (consensus or Byzantine resolution).
   */
  private def cleanupPartitionTasks(stageId: Int, index1: Int, index2: Int): Unit = {
    val stageIndex1 = (stageId, index1)
    val stageIndex2 = (stageId, index2)
    
    // Remove original tasks
    stageIndexToOriginalTask.remove(stageIndex1)
    stageIndexToOriginalTask.remove(stageIndex2)
    
    // Verification tasks are not registered, nothing more to remove
    
    // Remove executor mappings
    stageIndexToExecutor.remove(stageIndex1)
    stageIndexToExecutor.remove(stageIndex2)
    
    logDebug(s"[CLEANUP] Removed task references for stage $stageId, indexes $index1 and $index2")
  }

  /**
   * Clean up all data structures for a completed stage to prevent memory leaks.
   * Should be called when a stage completes (success or failure).
   * @param stageId The stage ID to clean up
   */
  def cleanupStage(stageId: Int): Unit = {
    logInfo(s"[CLEANUP] Starting cleanup for stage $stageId")
    // Remove all task ID mappings for this stage
    val tidsToRemove = tidToStageIndexInfo.filter(_._2._1 == stageId).keys.toList
    logInfo(s"[CLEANUP] Removing ${tidsToRemove.size} task ID mappings for stage $stageId")
    tidsToRemove.foreach(tidToStageIndexInfo.remove)
    
    // Remove all result hashes for this stage
    val stageIndexesToRemove = stageIndexToResultHash.keys.filter(_._1 == stageId).toList
    logInfo(s"[CLEANUP] Removing ${stageIndexesToRemove.size} result hashes for stage $stageId: ${stageIndexesToRemove.take(10)}")
    stageIndexesToRemove.foreach(stageIndexToResultHash.remove)
    
    // Remove all original tasks for this stage
    val originalTasksToRemove = stageIndexToOriginalTask.keys.filter(_._1 == stageId).toList
    originalTasksToRemove.foreach(stageIndexToOriginalTask.remove)

    // Remove all executor mappings for this stage
    val executorMappingsToRemove = stageIndexToExecutor.keys.filter(_._1 == stageId).toList
    executorMappingsToRemove.foreach(stageIndexToExecutor.remove)
    
    // Remove all verified partitions for this stage
    val verifiedToRemove = verifiedPartitions.filter(_._1 == stageId).toList
    verifiedToRemove.foreach(verifiedPartitions.remove)
    
    // Remove all verification in progress for this stage
    val verificationInProgressToRemove = verificationInProgress.filter(_._1 == stageId).toList
    verificationInProgressToRemove.foreach(verificationInProgress.remove)
    
    // Remove all pending verifications for this stage
    val pendingToRemove = pendingVerifications.keys.filter(_._1 == stageId).toList
    pendingToRemove.foreach(pendingVerifications.remove)
    
    // Remove all consensus flags for this stage
    val consensusToRemove = consensusFound.filter(_._1 == stageId).toList
    consensusToRemove.foreach(consensusFound.remove)
    
    // Remove all Merkle tree build results for this stage (taskIds contain stage info)
    // Note: merkleTreeBuildResults uses taskId as key, need to clean based on tidToStageIndexInfo
    val merkleTaskIdsToRemove = tidsToRemove.map(_.toString)
    merkleTreeBuildLock.synchronized {
      merkleTaskIdsToRemove.foreach(merkleTreeBuildResults.remove)
    }
    
    logInfo(s"[CLEANUP] Cleaned up all data structures for stage $stageId")
  }

  def addNewResultForTid(tid: Long, resultHash: String): Unit = {
    if(tidToStageIndexInfo.contains(tid)){
      val stageIndex = tidToStageIndexInfo(tid)
      stageIndexToResultHash(stageIndex) = resultHash
      logInfo(s"[HASH STORE] Stored result hash for task $tid (stage ${stageIndex._1}, index ${stageIndex._2}): $resultHash")
    } else {
      logWarning(s"[HASH STORE] Task $tid not found in running tasks, cannot store result hash")
    }
  }

  /**
   * Store a Merkle tree build result from an executor.
   * Called by TaskResultGetter when a MerkleTreeBuildTask completes.
   */
  def storeMerkleTreeBuildResult(result: MerkleTreeBuildResult): Unit = {
    val key = s"${result.stageId}_${result.taskIndex}_${result.partitionId}"
    merkleTreeBuildLock.synchronized {
      merkleTreeBuildResults(key) = result
      merkleTreeBuildLock.notifyAll()
    }
    logInfo(s"[MERKLE BUILD] Stored result for stage ${result.stageId}, idx ${result.taskIndex}, " +
      s"partition ${result.partitionId}: ${result.leafCount} leaves, rootHash=${result.rootHash}")
  }

  /**
   * Wait for a Merkle tree build result with timeout.
   * Returns Some(result) if received, None if timeout.
   * 
   * NOTE: This uses a blocking wait. The Merkle tree build tasks run on executors in parallel,
   * but this method blocks the calling thread while waiting for results. If both executors time out,
   * this returns None for both, triggering fallback to driver recomputation.
   */
  private def waitForMerkleTreeBuildResult(stageId: Int, taskIndex: Int, partitionId: Int, 
                                           timeoutMs: Long): Option[MerkleTreeBuildResult] = {
    val key = s"${stageId}_${taskIndex}_${partitionId}"
    val deadline = System.currentTimeMillis() + timeoutMs
    
    merkleTreeBuildLock.synchronized {
      while (!merkleTreeBuildResults.contains(key)) {
        val remaining = deadline - System.currentTimeMillis()
        if (remaining <= 0) {
          logWarning(s"[MERKLE BUILD] Timeout waiting for result: stage $stageId, idx $taskIndex, partition $partitionId (${timeoutMs}ms)")
          logWarning(s"[MERKLE BUILD] Executor may be Byzantine (non-responsive) or overloaded")
          return None
        }
        merkleTreeBuildLock.wait(remaining)
      }
      val result = merkleTreeBuildResults.remove(key)
      result
    }
  }

  /**
   * Check consensus between replica hashes and set consensus flag.
   * Does NOT dispatch verification - only checks if hashes match.
   * Called early to set consensus flag before batching completes.
   */
  def checkConsensus(tid: Long): Unit = {
    logDebug(s"Checking consensus for task $tid")
    if(tidToStageIndexInfo.contains(tid)) {
      val stageIndex = tidToStageIndexInfo(tid)
      if(stageIndexToResultHash.contains(stageIndex)){
        val stageId = stageIndex._1
        val index = stageIndex._2
        val partnerIndex = if (index % 2 == 0) index + 1 else index - 1
        val partitionId = index / 2
        val partitionKey = (stageId, partitionId)
        
        if(stageIndexToResultHash.contains((stageId, partnerIndex))){
          // Both replicas completed - check consensus
          if (!verificationInProgress.contains(partitionKey)) {
            verificationInProgress += partitionKey
            if(stageIndexToResultHash(stageIndex)==stageIndexToResultHash((stageId,partnerIndex))){
              logInfo(s"[+] CONSENSUS: Valid result for stage $stageId, partition $partitionId (indexes ${if (index % 2 == 0) s"$index, $partnerIndex" else s"$partnerIndex, $index"})")
              consensusFound.add((stageId, partitionId))
              logInfo(s"[CONSENSUS FLAG] Set consensus flag for stage $stageId, partition $partitionId")
              cleanupPartitionTasks(stageId, index, partnerIndex)
            } else {
              logDebug(s"[CONSENSUS] No consensus for partition $partitionId - hashes differ, will need verification")
            }
          }
        }
      }
    }
  }

  /**
   * Dispatch verification for Byzantine fault detection.
   * Only called when hashes differ (after checkConsensus() determines no consensus).
   * Called late (after pendingVerificationResults is stored) to avoid race conditions.
   */
  def verifyResult(tid: Long, taskScheduler: TaskScheduler): Unit = {
    logDebug(s"Verifying result for task $tid")
    if(tidToStageIndexInfo.contains(tid)) {
      val stageIndex = tidToStageIndexInfo(tid)
      if(stageIndexToResultHash.contains(stageIndex)){
        val stageId = stageIndex._1
        val index = stageIndex._2  // This is the real array index (0,1,2,3...) not taskId
        logDebug(s"Verifying task $tid (stage $stageId, index $index)")
        // Replica pairing: even index (0,2,4...) pairs with odd index (1,3,5...)
        val partnerIndex = if (index % 2 == 0) index + 1 else index - 1
        val partitionId = index / 2
        val partitionKey = (stageId, partitionId)
        
        if(stageIndexToResultHash.contains((stageId, partnerIndex))){
          // Both replicas completed - check if verification already done
          if (debugMode && !verifiedPartitions.contains(partitionKey)) {
            // First replica to verify this partition - do file verification (debug mode only)
            verifiedPartitions += partitionKey
            logDebug(s"[VERIFY] First replica (index $index) verifying partition $partitionId")
            verifyReplicaFiles(stageId, 
              if (index % 2 == 0) index else partnerIndex, 
              if (index % 2 == 0) partnerIndex else index, 
              partitionId)
          } else if (debugMode) {
            // Second replica - partition already verified by partner
            logDebug(s"[VERIFY] Partition $partitionId already verified by partner (index $partnerIndex)")
          }


          // Check if consensus already found by checkConsensus()
          if (verificationInProgress.contains(partitionKey)) {
            if (!consensusFound.contains(partitionKey)) {
              // Byzantine fault detected - dispatch verification
              logError(s"[X] BYZANTINE FAULT DETECTED: Hash mismatch for stage $stageId, partition $partitionId (indexes ${if (index % 2 == 0) s"$index, $partnerIndex" else s"$partnerIndex, $index"})")
              logInfo(s"[VERIFICATION DISPATCH] Launching async verification after batching complete")
              dispatchVerification(stageId, index, partnerIndex, partitionId, taskScheduler)
            } else {
              logDebug(s"[CONSENSUS] Already found for partition $partitionId, skipping verification dispatch")
            }
          } else {
            logDebug(s"[VERIFICATION] Not initiated yet for partition $partitionId (checkConsensus not called?)")
          }
        } else {
          logDebug(s"[*] Waiting for partner task $partnerIndex to complete")
        }
      } else {
        logDebug(s"[*] No result hash stored yet for task $tid")
      }
    } else {
      logWarning(s"[!] Task $tid not found in running tasks during verification")
    }
  }

  /**
   * Dispatch verification to appropriate method based on configuration.
   * Two independent config axes:
   * - EXEC_VERIFICATION: false=driver, true=third-executor
   * - MERKLE_VERIFICATION: false=full task, true=single element via Merkle tree
   * 
   * Merkle trees are ALWAYS built on the original executors (Phase 1: on driver from shared storage).
   * Comparison and single-element recomputation happens on driver or third executor.
   */
  private def dispatchVerification(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      taskScheduler: TaskScheduler): Unit = {
    
    logInfo(s"[VERIFICATION] Dispatching: EXEC_VERIFICATION=$useExecutorVerification, MERKLE_VERIFICATION=$useMerkleVerification")
    
    // Check if stage involves shuffle operations (driver cannot execute such tasks)
    val involvesShuffleOps = stageReadsFromShuffle(stageId)

    // Auto-switch to executor verification if driver verification was requested but stage involves shuffle
    if (involvesShuffleOps && !useExecutorVerification) {
      logWarning(s"[DISPATCH] Stage $stageId involves shuffle operations - driver verification NOT supported")
      logWarning(s"[DISPATCH] Reason: Driver TaskContext has taskMemoryManager=null, causing NullPointerException during shuffle read/write")
      logWarning(s"[DISPATCH] Auto-switching to THIRD EXECUTOR verification for safety")
      logWarning(s"[DISPATCH] To avoid this warning, set EXEC_VERIFICATION=true for stages with shuffle operations")
      verifyOnThirdExecutor(stageId, index1, index2, partitionId, taskScheduler)
    } else if (useExecutorVerification) {
      verifyOnThirdExecutor(stageId, index1, index2, partitionId, taskScheduler)
    } else {
      // Driver verification from the start - mark as active (last resort, no fallback available)
      logInfo(s"[DISPATCH] About to mark driver verification active for stage $stageId, partition $partitionId")
      dagScheduler.markDriverVerificationActive(stageId, partitionId)
      logInfo(s"[DISPATCH] About to call verifyOnDriver for stage $stageId, partition $partitionId")
      verifyOnDriver(stageId, index1, index2, partitionId, taskScheduler)
      logInfo(s"[DISPATCH] verifyOnDriver returned for stage $stageId, partition $partitionId")
    }
  }

  /**
   * Attempt to verify task result on a third executor (not the ones that ran replicas).
   * If MERKLE_VERIFICATION=true: Build Merkle trees on original executors, find disagreeing UID,
   *   then submit single-element verification to third executor.
   * If MERKLE_VERIFICATION=false: Submit full task verification to third executor.
   * Falls back to driver verification on failure.
   */
  private def verifyOnThirdExecutor(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      taskScheduler: TaskScheduler): Unit = {
    
    logInfo(s"[THIRD-EXECUTOR] Starting verification for partition $partitionId (merkle=$useMerkleVerification)")
    
    // Get executors that ran the original replicas
    val (executor1, host1) = stageIndexToExecutor.getOrElse((stageId, index1), ("unknown", "unknown"))
    val (executor2, host2) = stageIndexToExecutor.getOrElse((stageId, index2), ("unknown", "unknown"))
    val excludedExecutors = Set(executor1, executor2)

    logInfo(s"[THIRD-EXECUTOR] Excluded executors: $excludedExecutors")

    // Get the task object (only original tasks, not verification tasks)
    val taskOpt = stageIndexToOriginalTask.get((stageId, index1))
      .orElse(stageIndexToOriginalTask.get((stageId, index2)))

    taskOpt match {
      case None =>
        logError(s"[X] Task not found, falling back to driver")
        verifyOnDriver(stageId, index1, index2, partitionId, taskScheduler)

      case Some(task) =>
        try {
          // ShuffleMapTask: Disable Merkle verification - use full-task mode only
          // Merkle verification requires individual elements, but ShuffleMapTask produces MapStatus (not elements)
          val isShuffleMapTask = task.isInstanceOf[ShuffleMapTask]
          val useMerkleForThisTask = useMerkleVerification && !isShuffleMapTask
          
          if (isShuffleMapTask && useMerkleVerification) {
            logInfo(s"[VERIFICATION] ShuffleMapTask detected - disabling Merkle verification, using full-task mode")
          }
          
          if (useMerkleForThisTask) {
            // Step 1: Build Merkle trees and find disagreeing UID + per-tree leaf hashes
            val disagreement = buildMerkleTreesAndFindDisagreement(stageId, index1, index2, partitionId, taskScheduler)

            disagreement match {
              case Some((uid, correctIdx, byzantineIdx)) if uid == -1L =>
                // Early verdict: one executor timed out during tree building
                logInfo(s"[THIRD-EXECUTOR] Early verdict received - skipping verification")
                logInfo(s"[THIRD-EXECUTOR] Replica idx$correctIdx is CORRECT")
                logInfo(s"[THIRD-EXECUTOR] Replica idx$byzantineIdx is BYZANTINE (timeout)")
                // Determine verdict based on which index is correct
                val verdict = if (correctIdx == index1) "REPLICA_1_CORRECT" else "REPLICA_2_CORRECT"
                dagScheduler.eventProcessLoop.post(
                  VerificationVerdictEvent(stageId, index1, index2, partitionId, verdict, None))                // Verification complete - no need to submit to third executor

              case Some((uid, leafHash1, leafHash2)) if uid == -2L =>
                // UID mismatch: potential swap attack detected
                logWarning(s"[THIRD-EXECUTOR] UID mismatch detected at disagreement point - potential swap attack")
                logWarning(s"[THIRD-EXECUTOR] Falling back to full task recomputation for security")
                submitVerificationTaskToExecutor(task, index1, index2, excludedExecutors, taskScheduler)

              case Some((uid, leafCount1, leafCount2)) if uid == -3L =>
                // Size mismatch: one empty, one non-empty partition
                logWarning(s"[THIRD-EXECUTOR] Size mismatch detected (empty vs non-empty partition)")
                logWarning(s"[THIRD-EXECUTOR] Replica 1: $leafCount1 leaves, Replica 2: $leafCount2 leaves")
                logWarning(s"[THIRD-EXECUTOR] Cannot use Merkle tree - submitting full task verification")
                submitVerificationTaskToExecutor(task, index1, index2, excludedExecutors, taskScheduler)

              case Some((uid, leafHash1, leafHash2)) =>
                logInfo(s"[THIRD-EXECUTOR] Found disagreement at UID $uid, submitting single-element verification to third executor")
                submitVerificationTaskToExecutor(task, index1, index2, excludedExecutors, taskScheduler, Some(uid), Some((leafHash1, leafHash2)))

              case None =>
                logWarning(s"[THIRD-EXECUTOR] Merkle comparison found no disagreement, falling back to full task")
                submitVerificationTaskToExecutor(task, index1, index2, excludedExecutors, taskScheduler)
            }
          } else {
            // Full task verification on third executor
            submitVerificationTaskToExecutor(task, index1, index2, excludedExecutors, taskScheduler)
            logInfo(s"[THIRD-EXECUTOR] Full task verification submitted, will process result when complete")
          }

        } catch {
          case e: NotImplementedError =>
            logWarning(s"[!] ${e.getMessage}, falling back to driver")
            dagScheduler.restartTimeoutForDriverVerification(stageId, partitionId)
            verifyOnDriver(stageId, index1, index2, partitionId, taskScheduler)

          case e: Exception =>
            logError(s"[X] Third-executor verification failed: ${e.getMessage}")
            logError(s"[X] This includes Merkle tree build timeouts (both executors)")
            logError(s"[X] Falling back to driver recomputation as last resort")
            dagScheduler.restartTimeoutForDriverVerification(stageId, partitionId)
            verifyOnDriver(stageId, index1, index2, partitionId, taskScheduler)
        }
    }
  }

  // Track metadata for pending verification tasks: (stageId, partitionId) -> (index1, index2)
  private val pendingVerifications = new HashMap[(Int, Int), (Int, Int)]()

  /**
   * Submit verification task to a third executor (with exclusion constraints).
   * Creates a VerificationTask, wraps it in a TaskSet, and submits to scheduler.
   * Non-blocking - result will be processed when task completes.
   */
  private def submitVerificationTaskToExecutor(
      task: Task[_],
      index1: Int,
      index2: Int,
      excludedExecutors: Set[String],
      taskScheduler: TaskScheduler,
      targetElementId: Option[Long] = None,
      merkleLeafHashes: Option[(Int, Int)] = None): Unit = {

    val modeStr = targetElementId.map(id => s"single-element (UID $id)").getOrElse("full-task")
    logInfo(s"[THIRD-EXECUTOR] Creating verification task (excluding: $excludedExecutors) - mode: $modeStr")

    // Get replica hashes to pass to executor
    val hash1 = stageIndexToResultHash.getOrElse((task.stageId, index1), "MISSING")
    val hash2 = stageIndexToResultHash.getOrElse((task.stageId, index2), "MISSING")

    logInfo(s"[THIRD-EXECUTOR] Passing replica hashes to executor: idx$index1=$hash1, idx$index2=$hash2")

    // Create verification task WITH HASHES (and optional single-element parameters)
    val verificationTask = new VerificationTask(
      stageId = task.stageId,
      stageAttemptId = task.stageAttemptId,
      partitionId = task.partitionId,
      originalTask = task,
      excludedExecutors = excludedExecutors,
      replicaIndex1 = index1,
      replicaIndex2 = index2,
      replicaHash1 = hash1,
      replicaHash2 = hash2,
      targetElementId = targetElementId,
      merkleLeafHashes = merkleLeafHashes,
      localProperties = task.localProperties,
      serializedTaskMetrics = SparkEnv.get.closureSerializer.newInstance()
        .serialize(task.metrics).array(),
      jobId = task.jobId,
      appId = task.appId,
      appAttemptId = task.appAttemptId
    )

    // Wrap in TaskSet with single task
    // CRITICAL FIX: Use a very high stageAttemptId to avoid collision with original TaskSet
    // Original TaskSet typically has stageAttemptId = 0
    // Verification TaskSets use stageAttemptId = 1000000 + partitionId to ensure uniqueness
    // This prevents verification TaskSet completion from removing the original TaskSet from pool
    val verificationStageAttemptId = 1000000 + task.partitionId
    
    val verificationTaskSet = new TaskSet(
      tasks = Array(verificationTask),
      stageId = task.stageId,  // Use ORIGINAL stageId to stay part of the same stage
      stageAttemptId = verificationStageAttemptId,  // Unique attempt ID for verification
      priority = Int.MinValue,  // Highest priority - prioritise verification tasks to complete first
      properties = task.localProperties,
      resourceProfileId = 0,  // Default resource profile
      isAuxiliary = true  // Mark as auxiliary to prevent zombie conflicts
    )

    // Store metadata so we can process result when task completes
    val partitionKey = (task.stageId, task.partitionId)
    pendingVerifications(partitionKey) = (index1, index2)

    logInfo(s"[THIRD-EXECUTOR] Submitting verification TaskSet for stage ${task.stageId}, partition ${task.partitionId}")

    // Submit to scheduler (non-blocking)
    taskScheduler.submitTasks(verificationTaskSet)
  }

  /**
   * Complete a verification task and process its result.
   * Called by DAGScheduler when verification task completes.
   */
  def completeVerificationTask(stageId: Int, partitionId: Int, result: Any): Unit = {
    val partitionKey = (stageId, partitionId)
    pendingVerifications.get(partitionKey) match {
      case Some((index1, index2)) =>
        logInfo(s"[THIRD-EXECUTOR] Verification task for stage $stageId, partition $partitionId completed")
        // Process the result immediately
        handleThirdExecutorResult(stageId, index1, index2, partitionId, result)
        pendingVerifications.remove(partitionKey)
      case None =>
        logWarning(s"[THIRD-EXECUTOR] No pending verification found for stage $stageId, partition $partitionId")
    }
  }

  /**
   * Handle result from third-executor verification.
   * Executor has already computed the verdict, driver just logs it.
   */
  private def handleThirdExecutorResult(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      verifierResult: Any): Unit = {

    // Track whether we're falling back to driver (driver needs task for recomputation)
    var fallbackToDriver = false
    
    // Cast result to VerificationVerdict
    verifierResult match {
      case verdict: VerificationVerdict =>
        // Executor already computed the verdict!
        logInfo(s"[THIRD-EXECUTOR] Received verdict from executor:")
        logInfo(s"[THIRD-EXECUTOR] Verifier hash: ${verdict.verifierHash}")
        logInfo(s"[THIRD-EXECUTOR] Replica 1 (idx${verdict.replicaIndex1}): ${verdict.replicaHash1}")
        logInfo(s"[THIRD-EXECUTOR] Replica 2 (idx${verdict.replicaIndex2}): ${verdict.replicaHash2}")
        logInfo(verdict.logVerdict())

        // Handle verdict
        verdict.verdict match {
          case "NEITHER_MATCH" =>
            // Third executor differs from both - fall back to driver verification
            logWarning(s"[THIRD-EXECUTOR] NEITHER_MATCH - falling back to driver verification")
            // Cancel existing timeout and restart with longer duration for driver verification
            dagScheduler.restartTimeoutForDriverVerification(stageId, partitionId)
            executeOnDriver(stageId, index1, index2, partitionId, None, None)
            fallbackToDriver = true  // Keep task references for driver recomputation

          case _ =>
            // Report verdict to DAGScheduler (REPLICA_1_CORRECT, REPLICA_2_CORRECT, BOTH_MATCH)
            dagScheduler.eventProcessLoop.post(
              VerificationVerdictEvent(stageId, index1, index2, partitionId,
                verdict.verdict,
                verifierResult = None))
        }

      case _ =>
        logError(s"[X] Unexpected result type from verification task: ${verifierResult.getClass}")
        logError(s"[X] Expected VerificationVerdict, got: $verifierResult")
        // Fall back to driver verification
        logWarning(s"[X] Falling back to driver verification due to unexpected result")
        // Cancel existing timeout and restart with longer duration for driver verification
        dagScheduler.restartTimeoutForDriverVerification(stageId, partitionId)
        executeOnDriver(stageId, index1, index2, partitionId, None, None)
        fallbackToDriver = true  // Keep task references for driver recomputation
    }

    // Only cleanup if NOT falling back to driver (driver needs task for recomputation)
    if (!fallbackToDriver) {
      cleanupPartitionTasks(stageId, index1, index2)
      logDebug(s"[VERIFICATION] Third-executor verification complete for partition $partitionId")
    } else {
      logDebug(s"[VERIFICATION] Keeping task references for driver recomputation")
    }
  }

  /**
   * Verify task result on driver.
   * If MERKLE_VERIFICATION=true: Build Merkle trees (from shared storage), find disagreeing UID,
   *   then recompute single element on driver.
   * If MERKLE_VERIFICATION=false: Recompute full task on driver.
   *
   * Note: Package-private (accessible to DAGScheduler for timeout fallback handling).
   */
  private[scheduler] def verifyOnDriver(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      taskScheduler: TaskScheduler): Unit = {

    logInfo(s"[DRIVER] Starting verification for partition $partitionId (merkle=$useMerkleVerification)")

    // Check if this is a ShuffleMapTask - disable Merkle if so
    val taskOpt = stageIndexToOriginalTask.get((stageId, index1))
      .orElse(stageIndexToOriginalTask.get((stageId, index2)))
    val isShuffleMapTask = taskOpt.exists(_.isInstanceOf[ShuffleMapTask])
    val useMerkleForThisTask = useMerkleVerification && !isShuffleMapTask
    
    if (isShuffleMapTask && useMerkleVerification) {
      logInfo(s"[DRIVER] ShuffleMapTask detected - disabling Merkle verification, using full-task mode")
    }

    if (useMerkleForThisTask) {
      // Build Merkle trees and find disagreeing UID + per-tree leaf hashes
      val disagreement = buildMerkleTreesAndFindDisagreement(stageId, index1, index2, partitionId, taskScheduler)

      disagreement match {
        case Some((uid, correctIdx, byzantineIdx)) if uid == -1L =>
          // Early verdict: one executor timed out during tree building
          logInfo(s"[DRIVER] Early verdict received - skipping verification")
          logInfo(s"[DRIVER] Replica idx$correctIdx is CORRECT")
          logInfo(s"[DRIVER] Replica idx$byzantineIdx is BYZANTINE (timeout)")
          // Determine verdict based on which index is correct
          val verdict = if (correctIdx == index1) "REPLICA_1_CORRECT" else "REPLICA_2_CORRECT"
          dagScheduler.eventProcessLoop.post(
            VerificationVerdictEvent(stageId, index1, index2, partitionId, verdict, None))
          // Verification complete - no need to recompute

        case Some((uid, leafHash1, leafHash2)) if uid == -2L =>
          // UID mismatch: potential swap attack detected
          logWarning(s"[DRIVER] UID mismatch detected at disagreement point - potential swap attack")
          logWarning(s"[DRIVER] Falling back to full task recomputation for security")
          executeOnDriver(stageId, index1, index2, partitionId, None, None)

        case Some((uid, leafCount1, leafCount2)) if uid == -3L =>
          // Size mismatch: one empty, one non-empty partition
          logWarning(s"[DRIVER] Size mismatch detected (empty vs non-empty partition)")
          logWarning(s"[DRIVER] Replica 1: $leafCount1 leaves, Replica 2: $leafCount2 leaves")
          logWarning(s"[DRIVER] Cannot use Merkle tree - falling back to full task recomputation")
          executeOnDriver(stageId, index1, index2, partitionId, None, None)

        case Some((uid, leafHash1, leafHash2)) =>
          logInfo(s"[DRIVER] Found disagreement at UID $uid, recomputing single element")
          executeOnDriver(stageId, index1, index2, partitionId, Some(uid), Some((leafHash1, leafHash2)))

        case None =>
          logWarning(s"[DRIVER] Merkle comparison found no disagreement, falling back to full task")
          executeOnDriver(stageId, index1, index2, partitionId, None, None)
      }
    } else {
      // Full task recomputation on driver
      executeOnDriver(stageId, index1, index2, partitionId, None, None)
    }
  }

  /**
   * Execute task recomputation on driver and compare result with replicas.
   *
   * @param elementId If specified, only recompute this single element (for Merkle tree verification)
   * @param merkleLeafHashes If specified, (leafHash1, leafHash2) from Merkle tree comparison.
   *                         leafHash1 corresponds to index1's tree, leafHash2 to index2's tree.
   *                         Used for Merkle-compatible comparison instead of full-result hashes.
   */
  private def executeOnDriver(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      elementId: Option[Long] = None,
      merkleLeafHashes: Option[(Int, Int)] = None): Unit = {

    val modeStr = elementId.map(id => s"element $id").getOrElse("full task")
    logInfo(s"[DRIVER RECOMPUTE] Starting driver recomputation for stage $stageId, partition $partitionId ($modeStr)")

    val stageIndex1 = (stageId, index1)
    val stageIndex2 = (stageId, index2)

    // Get the task object (use either replica's task - they compute same partition)
    // Only retrieve original tasks, not verification tasks
    val taskOpt = stageIndexToOriginalTask.get(stageIndex1).orElse(stageIndexToOriginalTask.get(stageIndex2))

    taskOpt match {
      case None =>
        logError(s"[X] Cannot recompute: Task not found for stage $stageId, partition $partitionId")
        return
      case Some(task) =>
        // Create TaskMemoryManager for driver execution (needed for shuffle reads)
        // Declare outside try block so it's in scope for finally block cleanup
        val taskMemoryManager = new org.apache.spark.memory.TaskMemoryManager(
          SparkEnv.get.memoryManager,
          -1L  // Special task ID for driver execution
        )

        try {
          logInfo(s"[DRIVER RECOMPUTE] Running task on driver: partitionId=$partitionId")

          // Create minimal TaskContext for driver execution
          val driverTaskContext = new TaskContextImpl(
            stageId = task.stageId,
            stageAttemptNumber = task.stageAttemptId,  // Constructor uses stageAttemptNumber
            partitionId = task.partitionId,
            taskAttemptId = -1L,  // Special ID for driver execution
            attemptNumber = 0,
            taskIndex = index1,   // Use replica 1's taskIndex
            taskMemoryManager = taskMemoryManager,  // Provide memory manager for shuffle reads
            localProperties = task.localProperties,
            metricsSystem = SparkEnv.get.metricsSystem,
            taskMetrics = TaskMetrics.empty,  // Constructor uses taskMetrics, not metrics
            resources = Map.empty,
            targetElementId = elementId  // Pass element ID for single-element verification
          )

          // Set the context
          TaskContext.setTaskContext(driverTaskContext)

          // Run the task on driver
          val driverResult = task match {
            case rt: ResultTask[_, _] =>
              rt.runTask(driverTaskContext)
            case smt: ShuffleMapTask =>
              smt.runTask(driverTaskContext)
            case _ =>
              logError(s"[X] Unknown task type: ${task.getClass.getName}")
              return
          }

          // Choose comparison strategy based on whether we have Merkle leaf hashes
          (elementId, merkleLeafHashes) match {
            case (Some(uid), Some((leafHash1, leafHash2))) =>
              // MERKLE MODE: Extract the single element from driver result
              // The RDD already filtered to UID via targetElementId, so result contains only that element
              val elementAtUid = driverResult match {
                case arr: Array[_] if arr.length > 0 => arr(0)  // Extract first (only) element
                case other => other
              }

              // Format it the same way SafeWriter does
              val driverValue = elementAtUid match {
                case arr: Array[_] => arr.mkString("[", ",", "]")
                case other => other.toString
              }
              val driverLeafHash = driverValue.hashCode

              logInfo(s"[DRIVER RECOMPUTE] Merkle single-element comparison for UID $uid:")
              logInfo(s"[DRIVER RECOMPUTE]   Driver value: '$driverValue' -> leafHash=$driverLeafHash")
              logInfo(s"[DRIVER RECOMPUTE]   Replica 1 (idx$index1) leaf hash: $leafHash1")
              logInfo(s"[DRIVER RECOMPUTE]   Replica 2 (idx$index2) leaf hash: $leafHash2")

              (driverLeafHash == leafHash1, driverLeafHash == leafHash2) match {
                case (true, false) =>
                  logInfo(s"[VERDICT] Replica 1 (idx$index1) is CORRECT, Replica 2 (idx$index2) is BYZANTINE")
                  dagScheduler.eventProcessLoop.post(
                    VerificationVerdictEvent(stageId, index1, index2, partitionId, "REPLICA_1_CORRECT", Some(driverResult)))

                case (false, true) =>
                  logInfo(s"[VERDICT] Replica 2 (idx$index2) is CORRECT, Replica 1 (idx$index1) is BYZANTINE")
                  dagScheduler.eventProcessLoop.post(
                    VerificationVerdictEvent(stageId, index1, index2, partitionId, "REPLICA_2_CORRECT", Some(driverResult)))

                case (true, true) =>
                  logWarning(s"[?] UNEXPECTED: Both replicas match driver leaf hash - possible hash collision")
                  dagScheduler.eventProcessLoop.post(
                    VerificationVerdictEvent(stageId, index1, index2, partitionId, "BOTH_MATCH", Some(driverResult)))

                case (false, false) =>
                  logError(s"[X] CRITICAL: Driver leaf hash differs from BOTH replicas")
                  logError(s"[X] Using driver result as ground truth (system error or Byzantine driver)")
                  dagScheduler.eventProcessLoop.post(
                    VerificationVerdictEvent(stageId, index1, index2, partitionId, "NEITHER_MATCH", Some(driverResult)))
              }

            case _ =>
              // FULL TASK MODE: Compare using serialized result hashes (original logic)
              // For ShuffleMapTask: Use block size hashing (same as Executor.scala)
              val driverHash = if (task.isInstanceOf[ShuffleMapTask]) {
                val mapStatus = driverResult.asInstanceOf[MapStatus]
                val blockSizes = {
                  val builder = scala.collection.mutable.ArrayBuffer[Long]()
                  var idx = 0
                  try {
                    while (true) {
                      builder += mapStatus.getSizeForBlock(idx)
                      idx += 1
                    }
                  } catch {
                    case _: ArrayIndexOutOfBoundsException => // Expected - marks end of array
                  }
                  builder.toSeq
                }
                val sizeString = blockSizes.mkString(",")
                logInfo(s"[DRIVER SHUFFLE HASH] Hashing ${blockSizes.length} block sizes: ${sizeString.take(200)}...")
                computeTaskResultHash(ByteBuffer.wrap(sizeString.getBytes("UTF-8")))
              } else {
                computeTaskResultHash(driverResult)
              }
              logInfo(s"[HASH LOOKUP] Looking up hashes for stage $stageId: idx$index1=(${stageId},$index1), idx$index2=(${stageId},$index2)")
              logInfo(s"[HASH LOOKUP] Current hash map size: ${stageIndexToResultHash.size}, contains idx$index1: ${stageIndexToResultHash.contains((stageId, index1))}, contains idx$index2: ${stageIndexToResultHash.contains((stageId, index2))}")
              val hash1 = stageIndexToResultHash.getOrElse((stageId, index1), "MISSING")
              val hash2 = stageIndexToResultHash.getOrElse((stageId, index2), "MISSING")

              logInfo(s"[DRIVER RECOMPUTE] Full-result comparison:")
              logInfo(s"[DRIVER RECOMPUTE]   Driver hash: $driverHash")
              logInfo(s"[DRIVER RECOMPUTE]   Replica 1 (idx$index1) hash: $hash1")
              logInfo(s"[DRIVER RECOMPUTE]   Replica 2 (idx$index2) hash: $hash2")

              (driverHash == hash1, driverHash == hash2) match {
                case (true, false) =>
                  logInfo(s"[VERDICT] Replica 1 (idx$index1) is CORRECT, Replica 2 (idx$index2) is BYZANTINE")
                  dagScheduler.eventProcessLoop.post(
                    VerificationVerdictEvent(stageId, index1, index2, partitionId, "REPLICA_1_CORRECT", Some(driverResult)))

                case (false, true) =>
                  logInfo(s"[VERDICT] Replica 2 (idx$index2) is CORRECT, Replica 1 (idx$index1) is BYZANTINE")
                  dagScheduler.eventProcessLoop.post(
                    VerificationVerdictEvent(stageId, index1, index2, partitionId, "REPLICA_2_CORRECT", Some(driverResult)))

                case (true, true) =>
                  logWarning(s"[?] UNEXPECTED: Both replicas match driver, but were reported as different - possible race condition")
                  dagScheduler.eventProcessLoop.post(
                    VerificationVerdictEvent(stageId, index1, index2, partitionId, "BOTH_MATCH", Some(driverResult)))

                case (false, false) =>
                  logError(s"[X] CRITICAL: Driver result differs from BOTH replicas - system error or driver fault!")
                  logError(s"[X] Using driver result as ground truth")
                  dagScheduler.eventProcessLoop.post(
                    VerificationVerdictEvent(stageId, index1, index2, partitionId, "NEITHER_MATCH", Some(driverResult)))
              }
          }

          // Clean up task references (keep verificationInProgress as permanent marker)
          cleanupPartitionTasks(stageId, index1, index2)
          logDebug(s"[VERIFICATION] Cleared verification tracking for partition $partitionId")

        } catch {
          case e: Exception =>
            logError(s"[X] Driver recomputation FAILED with exception: ${e.getMessage}", e)
            logError(s"[X] This is the last resort - no further fallback available")
            logError(s"[X] Driver verification timeout will fire and abort the job")
            // Note: We don't report verdict here - let timeout handle it
            // The timeout is our safety net for all driver failures (including exceptions)
        } finally {
          // Clean up TaskContext and TaskMemoryManager
          TaskContext.unset()
          try {
            taskMemoryManager.cleanUpAllAllocatedMemory()
          } catch {
            case e: Exception =>
              logWarning(s"[CLEANUP] Failed to clean up TaskMemoryManager: ${e.getMessage}")
          }
        }
    }
  }

  /**
   * Build Merkle trees from both replica finals files and find the first disagreement.
   * Always builds on executors (local I/O is faster than driver reading from shared storage).
   *
   * @return Some((uid, leafHash1, leafHash2)) of the disagreeing element with per-tree leaf hashes,
   *         Some((-1, correctIndex, byzantineIndex)) for early verdict (timeout case),
   *         Some((-2, leafHash1, leafHash2)) for UID mismatch (potential swap attack),
   *         Some((-3, leafCount1, leafCount2)) for size mismatch (empty vs non-empty),
   *         or None if trees match. leafHash1 corresponds to index1's tree, leafHash2 to index2's tree.
   *         Special cases: uid=-1 indicates early verdict (leafHash1=correctIndex, leafHash2=byzantineIndex)
   *                        uid=-2 indicates UID mismatch at disagreement point (swap attack detection)
   *                        uid=-3 indicates size mismatch (leafHash1=leafCount1, leafHash2=leafCount2)
   */
  private def buildMerkleTreesAndFindDisagreement(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      taskScheduler: TaskScheduler): Option[(Long, Int, Int)] = {

    logInfo(s"[MERKLE] Building Merkle trees for stage $stageId, partition $partitionId")
    logInfo(s"[MERKLE] Building trees for idx$index1 and idx$index2")

    val (executor1, host1) = stageIndexToExecutor.getOrElse((stageId, index1), ("unknown", "unknown"))
    val (executor2, host2) = stageIndexToExecutor.getOrElse((stageId, index2), ("unknown", "unknown"))
    logInfo(s"[MERKLE] Original executors: $executor1@$host1 (idx$index1), $executor2@$host2 (idx$index2)")
    
    try {
      val appName = Option(SparkEnv.get).flatMap(env => Option(env.conf.get("spark.app.name", "unknown"))).getOrElse("unknown")
      val (isBinary, _, _) = FileFormatUtils.getFileFormat(debugMode)  // Use existing constant from line 52
      
      // Finals file paths (used for building Merkle trees)
      val finalsPath1 = FileFormatUtils.buildFinalsPath(appName, stageId, index1, partitionId, debugMode)
      val finalsPath2 = FileFormatUtils.buildFinalsPath(appName, stageId, index2, partitionId, debugMode)
      
      // Check if finals files exist
      val file1Exists = new java.io.File(finalsPath1).exists()
      val file2Exists = new java.io.File(finalsPath2).exists()
      
      (file1Exists, file2Exists) match {
        case (true, true) =>
          // Both exist - proceed with verification
          
        case (true, false) =>
          // Only replica 1 exists - replica 1 is correct
          logInfo(s"[FILE CHECK] Only replica 1 file exists - declaring replica 1 correct")
          logInfo(s"[EARLY VERDICT] Replica 1 (idx$index1) is CORRECT (file exists)")
          logInfo(s"[EARLY VERDICT] Replica 2 (idx$index2) is BYZANTINE (file missing)")
          return Some((-1L, index1, index2))
          
        case (false, true) =>
          // Only replica 2 exists - replica 2 is correct
          logInfo(s"[FILE CHECK] Only replica 2 file exists - declaring replica 2 correct")
          logInfo(s"[EARLY VERDICT] Replica 2 (idx$index2) is CORRECT (file exists)")
          logInfo(s"[EARLY VERDICT] Replica 1 (idx$index1) is BYZANTINE (file missing)")
          return Some((-1L, index2, index1))
          
        case (false, false) =>
          // Neither exists - trigger full-task recomputation
          logWarning(s"[FILE CHECK] Both replica files missing - cannot build Merkle trees")
          return None
      }
      
      // Build Merkle trees - either on driver or executors based on configuration
      val outcome = if (buildMerkleTreesOnDriver) {
        logInfo(s"[MERKLE] Building trees on DRIVER (MERKLE_BUILD_ON_DRIVER=true)")
        buildMerkleTreesOnDriver(stageId, index1, index2, partitionId, finalsPath1, finalsPath2, isBinary)
      } else {
        logInfo(s"[MERKLE] Building trees on EXECUTORS (default)")
        buildMerkleTreesOnExecutors(stageId, index1, index2, partitionId, finalsPath1, finalsPath2, isBinary, host1, host2, taskScheduler)
      }
      
      // Handle outcome
      outcome match {
        case EarlyVerdict(correctIdx, byzantineIdx, reason) =>
          // Executor timed out - declare responsive executor correct
          logInfo(s"[MERKLE] Executor timeout: $reason")
          logInfo(s"[MERKLE] Early verdict: responsive executor is correct")
          // Return special signal: uid=-1, correctIndex, byzantineIndex
          Some((-1L, correctIdx, byzantineIdx))
          
        case BothTreesBuilt(tree1, tree2) =>
          // Both paths (driver and executor) end up here with trees built
          logInfo(s"[MERKLE] Comparing trees...")
          val disagreement = org.apache.spark.rdd.MerkleTree.verify(tree1, tree2)
          
          disagreement match {
            case Some((uid, hash1, hash2)) =>
              logInfo(s"[MERKLE] Found disagreement at UID $uid (idx$index1 leafHash=$hash1, idx$index2 leafHash=$hash2)")
            case None =>
              logWarning(s"[MERKLE] Trees match but hashes differ - possible hash collision or race condition")
          }
          
          disagreement
          
        case SizeMismatch(leafCount1, leafCount2, reason) =>
          // Size mismatch: one empty, one non-empty - return -3
          logWarning(s"[MERKLE] Size mismatch: $reason")
          Some((-3L, leafCount1, leafCount2))
          
        case MerkleBuildFailure(reason) =>
          // Build failed (driver or executor)
          logError(s"[MERKLE] Build failed: $reason")
          None
      }
      
    } catch {
      case e: Exception =>
        logError(s"[MERKLE] Failed to build or compare trees: ${e.getMessage}", e)
        None
    }
  }

  /**
   * Build Merkle trees directly on the driver by reading finals files.
   * Alternative to executor-based building, useful for testing/debugging or shared storage scenarios.
   * Returns BothTreesBuilt with the trees, or MerkleBuildFailure on error.
   */
  private def buildMerkleTreesOnDriver(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      finalsPath1: String,
      finalsPath2: String,
      isBinary: Boolean): MerkleBuildOutcome = {
    
    logInfo(s"[MERKLE] Building trees on driver for stage $stageId, partition $partitionId")
    
    try {
      // Build both trees directly on driver
      val tree1 = org.apache.spark.rdd.MerkleTree.buildFromFinalsFile(finalsPath1, isBinary)
      val tree2 = org.apache.spark.rdd.MerkleTree.buildFromFinalsFile(finalsPath2, isBinary)
      
      logInfo(s"[MERKLE] Driver built tree 1: ${tree1.leafCount} leaves, rootHash=${tree1.rootHash}")
      logInfo(s"[MERKLE] Driver built tree 2: ${tree2.leafCount} leaves, rootHash=${tree2.rootHash}")
      
      // Check for unsafe stage markers
      val isUnsafe1 = (tree1.leafCount == 1 && tree1.rootHash == UnsafeStageMarkers.UNSAFE_STAGE_HASH)
      val isUnsafe2 = (tree2.leafCount == 1 && tree2.rootHash == UnsafeStageMarkers.UNSAFE_STAGE_HASH)
      
      if (isUnsafe1 && isUnsafe2) {
        logInfo(s"[MERKLE] Both replicas are UNSAFE_STAGE - triggering full-task recomputation")
        return MerkleBuildFailure("Both replicas from unsafe stage - full-task verification required")
      }
      
      if (isUnsafe1 || isUnsafe2) {
        logWarning(s"[MERKLE] Replica mismatch: unsafe=${if (isUnsafe1) "idx" + index1 else "idx" + index2} - triggering recomputation")
        return MerkleBuildFailure("Unsafe/safe mismatch - full-task verification required")
      }
      
      // Return trees - disagreement finding will happen in common code
      BothTreesBuilt(tree1, tree2)
      
    } catch {
      case e: Exception =>
        logError(s"[MERKLE] Failed to build trees on driver: ${e.getMessage}")
        MerkleBuildFailure(s"Driver tree building failed: ${e.getMessage}")
    }
  }

  /**
   * Build Merkle trees on executors by submitting MerkleTreeBuildTasks.
   * Returns EarlyVerdict if one executor times out (indicating Byzantine behavior).
   * Executors have local access to finals files, making this faster than driver-based building.
   */
  private def buildMerkleTreesOnExecutors(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      finalsPath1: String,
      finalsPath2: String,
      isBinary: Boolean,
      host1: String,
      host2: String,
      taskScheduler: TaskScheduler): MerkleBuildOutcome = {
    
    try {
      // TaskScheduler passed as parameter
      try {
          logInfo(s"[MERKLE] Submitting tree build tasks to executors")
          
          // Create MerkleTreeBuildTasks for both replicas
          val serializedMetrics = SparkEnv.get.closureSerializer.newInstance()
            .serialize(TaskMetrics.registered).array()
          
          val task1 = new MerkleTreeBuildTask(
            stageId = stageId,
            stageAttemptId = 0,
            taskIndex = index1,
            partitionId = partitionId,
            finalsFilePath = finalsPath1,
            isBinary = isBinary,
            preferredHost = host1,
            localProperties = new java.util.Properties(),
            serializedTaskMetrics = serializedMetrics
          )

          val task2 = new MerkleTreeBuildTask(
            stageId = stageId,
            stageAttemptId = 0,
            taskIndex = index2,
            partitionId = partitionId,
            finalsFilePath = finalsPath2,
            isBinary = isBinary,
            preferredHost = host2,
            localProperties = new java.util.Properties(),
            serializedTaskMetrics = serializedMetrics
          )
          
          // Submit both tasks as a single TaskSet
          // CRITICAL FIX: Use unique stageAttemptId to avoid collision with original TaskSet
          // Merkle TaskSets use stageAttemptId = 2000000 + partitionId
          val merkleStageAttemptId = 2000000 + partitionId
          
          val taskSet = new TaskSet(
            tasks = Array(task1, task2),
            stageId = stageId,  // Use ORIGINAL stageId to stay part of the same stage
            stageAttemptId = merkleStageAttemptId,  // Unique attempt ID for Merkle
            priority = Int.MinValue,  // Highest priority - prioritise Merkle tasks to complete first
            properties = new java.util.Properties(),
            resourceProfileId = 0,
            isAuxiliary = true  // Mark as auxiliary to prevent zombie conflicts
          )
          
        logInfo(s"[MERKLE] Submitting TaskSet with 2 tree build tasks")
        taskScheduler.submitTasks(taskSet)
        
        // Calculate adaptive Merkle timeout proportional to verification timeout
        // Merkle timeout = verification timeout / 2 (Merkle is part of verification process)
        val baseVerificationTimeoutMs = SparkEnv.get.conf.get("spark.verification.timeout", "20000").toInt
        val adaptiveMerkleTimeout = baseVerificationTimeoutMs / 2  // Half of verification timeout
        
        logInfo(s"[MERKLE] Waiting for tree build results (timeout=${adaptiveMerkleTimeout}ms, " +
                s"${adaptiveMerkleTimeout / 1000}s, based on verification timeout)")
        val result1Opt = waitForMerkleTreeBuildResult(stageId, index1, partitionId, adaptiveMerkleTimeout)
        val result2Opt = waitForMerkleTreeBuildResult(stageId, index2, partitionId, adaptiveMerkleTimeout)
        
        // Smart fallback: use partial results if available
        (result1Opt, result2Opt) match {
          case (Some(result1), Some(result2)) =>
            // Both executors responded
            logInfo(s"[MERKLE] Successfully received both tree build results from executors")
            logInfo(s"[MERKLE] Tree 1: ${result1.leafCount} leaves, rootHash=${result1.rootHash}, buildTime=${result1.buildTimeMs}ms")
            logInfo(s"[MERKLE] Tree 2: ${result2.leafCount} leaves, rootHash=${result2.rootHash}, buildTime=${result2.buildTimeMs}ms")
            
            // Check if either tree represents an empty partition or unsafe stage
            val isEmpty1 = (result1.leafCount == 1 && result1.rootHash == UnsafeStageMarkers.EMPTY_PARTITION_HASH)
            val isEmpty2 = (result2.leafCount == 1 && result2.rootHash == UnsafeStageMarkers.EMPTY_PARTITION_HASH)
            val isUnsafe1 = (result1.leafCount == 1 && result1.rootHash == UnsafeStageMarkers.UNSAFE_STAGE_HASH)
            val isUnsafe2 = (result2.leafCount == 1 && result2.rootHash == UnsafeStageMarkers.UNSAFE_STAGE_HASH)
            
            // If both are unsafe stages, skip Merkle and do full-task recomputation
            if (isUnsafe1 && isUnsafe2) {
              logInfo(s"[MERKLE] Both replicas are UNSAFE_STAGE - triggering full-task recomputation")
              return MerkleBuildFailure("Both replicas from unsafe stage - full-task verification required")
            }
            
            // If one is unsafe and other isn't, that's suspicious
            if (isUnsafe1 || isUnsafe2) {
              logWarning(s"[MERKLE] Replica mismatch: unsafe=${if (isUnsafe1) "idx" + index1 else "idx" + index2} - triggering recomputation")
              return MerkleBuildFailure("Unsafe/safe mismatch - full-task verification required")
            }
            
            if (isEmpty1 && isEmpty2) {
              logInfo(s"[MERKLE] Both replicas produced EMPTY results (valid empty partition)")
              logInfo(s"[MERKLE] Trees will be compared - if they match, consensus achieved")
              // Let trees be compared normally - they'll match and return None
              BothTreesBuilt(result1.tree, result2.tree)
              
            } else if (isEmpty1 || isEmpty2) {
              // One empty, one non-empty - size mismatch
              logWarning(s"[MERKLE] Size mismatch: idx$index1=${result1.leafCount} leaves, idx$index2=${result2.leafCount} leaves")
              logWarning(s"[MERKLE] One empty, one non-empty - full task recomputation needed")
              // Return size mismatch outcome
              SizeMismatch(result1.leafCount, result2.leafCount, "Empty vs non-empty partition")
              
            } else {
              // Normal case: both non-empty
              BothTreesBuilt(result1.tree, result2.tree)
            }
            
          case (Some(result1), None) =>
            // Replica 2 timed out/failed
            // Empty or not, if one succeeded and other failed → success wins
            logWarning(s"[MERKLE] Replica 2 (idx$index2) timed out/failed on tree building")
            logInfo(s"[EARLY VERDICT] Replica 1 (idx$index1) is CORRECT (responded in ${result1.buildTimeMs}ms)")
            logInfo(s"[EARLY VERDICT] Replica 2 (idx$index2) is BYZANTINE (timeout/failure = non-responsive)")
            logInfo(s"[MERKLE] Skipping tree comparison - timeout indicates Byzantine behavior")
            EarlyVerdict(index1, index2, s"Replica $index2 timed out after ${adaptiveMerkleTimeout}ms")
            
          case (None, Some(result2)) =>
            // Replica 1 timed out/failed
            // Empty or not, if one succeeded and other failed → success wins
            logWarning(s"[MERKLE] Replica 1 (idx$index1) timed out/failed on tree building")
            logInfo(s"[EARLY VERDICT] Replica 2 (idx$index2) is CORRECT (responded in ${result2.buildTimeMs}ms)")
            logInfo(s"[EARLY VERDICT] Replica 1 (idx$index1) is BYZANTINE (timeout/failure = non-responsive)")
            logInfo(s"[MERKLE] Skipping tree comparison - timeout indicates Byzantine behavior")
            EarlyVerdict(index2, index1, s"Replica $index1 timed out after ${adaptiveMerkleTimeout}ms")
            
          case (None, None) =>
            // Both executors timed out/failed - fall back to driver building
            logWarning(s"[MERKLE] Both replicas timed out/failed on tree building (${adaptiveMerkleTimeout}ms)")
            logWarning(s"[MERKLE] Executors likely busy - falling back to building trees on driver")
            logInfo(s"[MERKLE] Switching to driver-based tree building...")
            // Fall back to driver building instead of throwing
            return buildMerkleTreesOnDriver(stageId, index1, index2, partitionId, finalsPath1, finalsPath2, isBinary)
        }
      } catch {
        case e: Exception =>
          logError(s"[MERKLE] Task submission failed: ${e.getMessage}", e)
          logWarning(s"[MERKLE] Falling back to driver-based tree building")
          // Fall back to driver building instead of throwing
          return buildMerkleTreesOnDriver(stageId, index1, index2, partitionId, finalsPath1, finalsPath2, isBinary)
      }
    } catch {
      case e: Exception =>
        logError(s"[MERKLE] Unexpected error: ${e.getMessage}", e)
        logWarning(s"[MERKLE] Falling back to driver-based tree building as last resort")
        // Fall back to driver building as last resort
        buildMerkleTreesOnDriver(stageId, index1, index2, partitionId, finalsPath1, finalsPath2, isBinary)
    }
  }

  /**
   * Read and verify output files from both replica tasks.
   * Supports both binary (.bin) and text (.log) formats.
   * Binary format is much faster (4-5x) for reading and hashing.
   * Only reads committed files (not .tmp) to ensure complete data.
   */
  private def verifyReplicaFiles(stageId: Int, index1: Int, index2: Int, partitionId: Int): Unit = {
    try {
      val appName = Option(SparkEnv.get).flatMap(env => Option(env.conf.get("spark.app.name", "unknown"))).getOrElse("unknown")
      
      // Use FileFormatUtils to build file paths (uses existing debugMode constant from line 52)
      val file1 = new File(FileFormatUtils.buildFinalsPath(appName, stageId, index1, partitionId, debugMode))
      val file2 = new File(FileFormatUtils.buildFinalsPath(appName, stageId, index2, partitionId, debugMode))
      
      if (!file1.exists()) {
        logWarning(s"[!] Replica file not found: ${file1.getAbsolutePath}")
        return
      }
      if (!file2.exists()) {
        logWarning(s"[!] Replica file not found: ${file2.getAbsolutePath}")
        return
      }
      
      logInfo(s"[VERIFY] Reading replica files: ${file1.getName}, ${file2.getName}")
      
      // Read files as raw bytes for fast comparison
      val bytes1 = Files.readAllBytes(file1.toPath)
      val bytes2 = Files.readAllBytes(file2.toPath)
      
      logInfo(s"[SIZE] Replica 1 (idx$index1): ${bytes1.length} bytes")
      logInfo(s"[SIZE] Replica 2 (idx$index2): ${bytes2.length} bytes")
      
      // Compute fast hash directly on bytes
      val hash1 = java.util.Arrays.hashCode(bytes1)
      val hash2 = java.util.Arrays.hashCode(bytes2)
      
      logInfo(s"[HASH] Replica 1 (idx$index1) file hash: $hash1")
      logInfo(s"[HASH] Replica 2 (idx$index2) file hash: $hash2")
      
      // Byte-level comparison
      val filesMatch = java.util.Arrays.equals(bytes1, bytes2)
      
      if (filesMatch) {
        logInfo(s"[FILE VERIFICATION] PASSED: Replica files are identical for stage $stageId, partition $partitionId")
      } else {
        logError(s"[FILE VERIFICATION] FAILED: Replica files differ for stage $stageId, partition $partitionId")
        logError(s"    File 1: ${bytes1.length} bytes, hash=$hash1")
        logError(s"    File 2: ${bytes2.length} bytes, hash=$hash2")
      }
    } catch {
      case e: Exception =>
        logError(s"[!] Error reading replica files for stage $stageId, partition $partitionId: ${e.getMessage}")
    }
  }
  


}