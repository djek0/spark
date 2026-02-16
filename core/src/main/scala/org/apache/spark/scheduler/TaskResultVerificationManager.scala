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


object TaskResultVerificationManager extends Logging {

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
  // Track which executor ran which task (for third-executor verification)
  var stageIndexToExecutor = new HashMap[(Int, Int), String]()
  // Track Merkle tree build results from executors
  private val merkleTreeBuildResults = new HashMap[String, MerkleTreeBuildResult]()
  private val merkleTreeBuildLock = new Object()

  // Sealed trait for Merkle tree build outcomes
  private sealed trait MerkleBuildOutcome
  private case class BothTreesBuilt(tree1: org.apache.spark.rdd.MerkleTree, tree2: org.apache.spark.rdd.MerkleTree) extends MerkleBuildOutcome
  private case class EarlyVerdict(correctIndex: Int, byzantineIndex: Int, reason: String) extends MerkleBuildOutcome

  // Configuration: Enable third-executor verification
  private val useExecutorVerification = envOrElse("EXEC_VERIFICATION", "false").toBoolean
  // Configuration: Enable Merkle tree-based verification (find disagreeing element instead of full recompute)
  private val useMerkleVerification = envOrElse("MERKLE_VERIFICATION", "false").toBoolean
  private val verificationTimeoutMs = envOrElse("VERIFICATION_TIMEOUT_MS", "5000").toInt
  private val merkleTreeBuildTimeoutMs = envOrElse("MERKLE_BUILD_TIMEOUT_MS", "30000").toInt

  logInfo(s"[CONFIG] Executor verification enabled: $useExecutorVerification")
  logInfo(s"[CONFIG] Merkle tree verification enabled: $useMerkleVerification")
  logInfo(s"[CONFIG] Verification timeout: ${verificationTimeoutMs}ms")
  logInfo(s"[CONFIG] Merkle tree build timeout: ${merkleTreeBuildTimeoutMs}ms")

  /**
   * Register a task for verification and store it for potential driver recomputation.
   * Merged registration method to avoid HashMap duplication.
   * @param tid Task ID
   * @param indexStage Tuple of (stageId, taskIndex)
   * @param task Task object for recomputation
   * @param executorId Executor ID where task is running
   */
  def addNewRunningTask(tid: Int, indexStage: (Int, Int), task: Task[_], executorId: String): Unit = {
    if(tidToStageIndexInfo.contains(tid)){
      logDebug(s"Task $tid already registered in verification manager")
      return
    }
    logDebug(s"Registered task $tid with stage ${indexStage._1}, index ${indexStage._2}, executor $executorId")
    tidToStageIndexInfo(tid) = indexStage
    stageIndexToOriginalTask(indexStage) = task
    stageIndexToExecutor(indexStage) = executorId
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
    // Remove all task ID mappings for this stage
    val tidsToRemove = tidToStageIndexInfo.filter(_._2._1 == stageId).keys.toList
    tidsToRemove.foreach(tidToStageIndexInfo.remove)
    
    // Remove all result hashes for this stage
    val stageIndexesToRemove = stageIndexToResultHash.keys.filter(_._1 == stageId).toList
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
      logDebug(s"Stored result hash for task $tid (stage ${stageIndex._1}, index ${stageIndex._2}): $resultHash")
    } else {
      logWarning(s"[!] Task $tid not found in running tasks, cannot store result hash")
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
   */
  private def waitForMerkleTreeBuildResult(stageId: Int, taskIndex: Int, partitionId: Int, 
                                           timeoutMs: Long): Option[MerkleTreeBuildResult] = {
    val key = s"${stageId}_${taskIndex}_${partitionId}"
    val deadline = System.currentTimeMillis() + timeoutMs
    
    merkleTreeBuildLock.synchronized {
      while (!merkleTreeBuildResults.contains(key)) {
        val remaining = deadline - System.currentTimeMillis()
        if (remaining <= 0) {
          logWarning(s"[MERKLE BUILD] Timeout waiting for result: stage $stageId, idx $taskIndex, partition $partitionId")
          return None
        }
        merkleTreeBuildLock.wait(remaining)
      }
      val result = merkleTreeBuildResults.remove(key)
      result
    }
  }

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
          if (!verifiedPartitions.contains(partitionKey)) {
            // First replica to verify this partition - do file verification
            verifiedPartitions += partitionKey
            logDebug(s"[VERIFY] First replica (index $index) verifying partition $partitionId")
            verifyReplicaFiles(stageId, 
              if (index % 2 == 0) index else partnerIndex, 
              if (index % 2 == 0) partnerIndex else index, 
              partitionId)
          } else {
            // Second replica - partition already verified by partner
            logDebug(s"[VERIFY] Partition $partitionId already verified by partner (index $partnerIndex)")
          }


          // Check if verification already initiated for this partition
          if (!verificationInProgress.contains(partitionKey)) {
            verificationInProgress += partitionKey
            // Always check consensus (both replicas should check this)
            if(stageIndexToResultHash(stageIndex)==stageIndexToResultHash((stageId,partnerIndex))){
              logInfo(s"[+] CONSENSUS: Valid result for stage $stageId, partition $partitionId (indexes ${if (index % 2 == 0) s"$index, $partnerIndex" else s"$partnerIndex, $index"})")
              // Clean up task references after successful consensus
              cleanupPartitionTasks(stageId, index, partnerIndex)
            } else {
              logError(s"[X] BYZANTINE FAULT DETECTED: Hash mismatch for stage $stageId, partition $partitionId (indexes ${if (index % 2 == 0) s"$index, $partnerIndex" else s"$partnerIndex, $index"})")

              // Dispatch to appropriate verification method based on configuration
              dispatchVerification(stageId, index, partnerIndex, partitionId, taskScheduler)
            }
          } else {
            logDebug(s"[VERIFICATION] Already initiated for partition $partitionId, skipping duplicate")
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
    
    if (useExecutorVerification) {
      verifyOnThirdExecutor(stageId, index1, index2, partitionId, taskScheduler)
    } else {
      verifyOnDriver(stageId, index1, index2, partitionId, taskScheduler)
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
    val executor1 = stageIndexToExecutor.getOrElse((stageId, index1), "unknown")
    val executor2 = stageIndexToExecutor.getOrElse((stageId, index2), "unknown")
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
          if (useMerkleVerification) {
            // Step 1: Build Merkle trees and find disagreeing UID + per-tree leaf hashes
            val disagreement = buildMerkleTreesAndFindDisagreement(stageId, index1, index2, partitionId, taskScheduler)
            
            disagreement match {
              case Some((uid, correctIdx, byzantineIdx)) if uid == -1L =>
                // Early verdict: one executor timed out during tree building
                logInfo(s"[THIRD-EXECUTOR] Early verdict received - skipping verification")
                logInfo(s"[THIRD-EXECUTOR] Replica idx$correctIdx is CORRECT")
                logInfo(s"[THIRD-EXECUTOR] Replica idx$byzantineIdx is BYZANTINE (timeout)")
                // Verification complete - no need to submit to third executor
                
              case Some((uid, leafHash1, leafHash2)) if uid == -2L =>
                // UID mismatch: potential swap attack detected
                logWarning(s"[THIRD-EXECUTOR] UID mismatch detected at disagreement point - potential swap attack")
                logWarning(s"[THIRD-EXECUTOR] Falling back to full task recomputation for security")
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
            verifyOnDriver(stageId, index1, index2, partitionId, taskScheduler)
            
          case e: Exception =>
            logError(s"[X] Verification failed: ${e.getMessage}, falling back to driver")
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
    val verificationTaskSet = new TaskSet(
      tasks = Array(verificationTask),
      stageId = task.stageId,
      stageAttemptId = task.stageAttemptId,
      priority = Int.MaxValue,  // Highest priority
      properties = task.localProperties,
      resourceProfileId = 0  // Default resource profile
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
    
    // Cast result to VerificationVerdict
    verifierResult match {
      case verdict: VerificationVerdict =>
        // Executor already computed the verdict!
        logInfo(s"[THIRD-EXECUTOR] Received verdict from executor:")
        logInfo(s"[THIRD-EXECUTOR] Verifier hash: ${verdict.verifierHash}")
        logInfo(s"[THIRD-EXECUTOR] Replica 1 (idx${verdict.replicaIndex1}): ${verdict.replicaHash1}")
        logInfo(s"[THIRD-EXECUTOR] Replica 2 (idx${verdict.replicaIndex2}): ${verdict.replicaHash2}")
        logInfo(verdict.logVerdict())
        
      case _ =>
        logError(s"[X] Unexpected result type from verification task: ${verifierResult.getClass}")
        logError(s"[X] Expected VerificationVerdict, got: $verifierResult")
    }
    
    // Clean up task references (keep verificationInProgress as permanent marker)
    cleanupPartitionTasks(stageId, index1, index2)
    logDebug(s"[VERIFICATION] Third-executor verification complete for partition $partitionId")
  }

  /**
   * Verify task result on driver.
   * If MERKLE_VERIFICATION=true: Build Merkle trees (from shared storage), find disagreeing UID,
   *   then recompute single element on driver.
   * If MERKLE_VERIFICATION=false: Recompute full task on driver.
   */
  private def verifyOnDriver(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      taskScheduler: TaskScheduler): Unit = {
    
    logInfo(s"[DRIVER] Starting verification for partition $partitionId (merkle=$useMerkleVerification)")
    
    if (useMerkleVerification) {
      // Build Merkle trees and find disagreeing UID + per-tree leaf hashes
      val disagreement = buildMerkleTreesAndFindDisagreement(stageId, index1, index2, partitionId, taskScheduler)
      
      disagreement match {
        case Some((uid, correctIdx, byzantineIdx)) if uid == -1L =>
          // Early verdict: one executor timed out during tree building
          logInfo(s"[DRIVER] Early verdict received - skipping verification")
          logInfo(s"[DRIVER] Replica idx$correctIdx is CORRECT")
          logInfo(s"[DRIVER] Replica idx$byzantineIdx is BYZANTINE (timeout)")
          // Verification complete - no need to recompute
          
        case Some((uid, leafHash1, leafHash2)) if uid == -2L =>
          // UID mismatch: potential swap attack detected
          logWarning(s"[DRIVER] UID mismatch detected at disagreement point - potential swap attack")
          logWarning(s"[DRIVER] Falling back to full task recomputation for security")
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
            taskMemoryManager = null,  // Driver doesn't need this
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
                  logInfo(s"[✓] VERDICT: Replica 1 (idx$index1) is CORRECT, Replica 2 (idx$index2) is BYZANTINE")
                case (false, true) =>
                  logInfo(s"[✓] VERDICT: Replica 2 (idx$index2) is CORRECT, Replica 1 (idx$index1) is BYZANTINE")
                case (true, true) =>
                  logWarning(s"[?] UNEXPECTED: Both replicas match driver leaf hash - possible hash collision")
                case (false, false) =>
                  logError(s"[X] CRITICAL: Driver leaf hash differs from BOTH replicas - system error or driver fault!")
              }
              
            case _ =>
              // FULL TASK MODE: Compare using serialized result hashes (original logic)
              val driverHash = computeTaskResultHash(driverResult)
              val hash1 = stageIndexToResultHash.getOrElse((stageId, index1), "MISSING")
              val hash2 = stageIndexToResultHash.getOrElse((stageId, index2), "MISSING")
              
              logInfo(s"[DRIVER RECOMPUTE] Full-result comparison:")
              logInfo(s"[DRIVER RECOMPUTE]   Driver hash: $driverHash")
              logInfo(s"[DRIVER RECOMPUTE]   Replica 1 (idx$index1) hash: $hash1")
              logInfo(s"[DRIVER RECOMPUTE]   Replica 2 (idx$index2) hash: $hash2")
              
              (driverHash == hash1, driverHash == hash2) match {
                case (true, false) =>
                  logInfo(s"[✓] VERDICT: Replica 1 (idx$index1) is CORRECT, Replica 2 (idx$index2) is BYZANTINE")
                case (false, true) =>
                  logInfo(s"[✓] VERDICT: Replica 2 (idx$index2) is CORRECT, Replica 1 (idx$index1) is BYZANTINE")
                case (true, true) =>
                  logWarning(s"[?] UNEXPECTED: Both replicas match driver, but were reported as different - possible race condition")
                case (false, false) =>
                  logError(s"[X] CRITICAL: Driver result differs from BOTH replicas - system error or driver fault!")
              }
          }
          
          // Clean up task references (keep verificationInProgress as permanent marker)
          cleanupPartitionTasks(stageId, index1, index2)
          logDebug(s"[VERIFICATION] Cleared verification tracking for partition $partitionId")
          
        } catch {
          case e: Exception =>
            logError(s"[X] Driver recomputation failed: ${e.getMessage}", e)
        } finally {
          TaskContext.unset()
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
   *         or None if trees match. leafHash1 corresponds to index1's tree, leafHash2 to index2's tree.
   *         Special cases: uid=-1 indicates early verdict (leafHash1=correctIndex, leafHash2=byzantineIndex)
   *                        uid=-2 indicates UID mismatch at disagreement point (swap attack detection)
   */
  private def buildMerkleTreesAndFindDisagreement(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      taskScheduler: TaskScheduler): Option[(Long, Int, Int)] = {
    
    logInfo(s"[MERKLE] Building Merkle trees for stage $stageId, partition $partitionId")
    logInfo(s"[MERKLE] Building trees for idx$index1 and idx$index2")
    
    val executor1 = stageIndexToExecutor.getOrElse((stageId, index1), "unknown")
    val executor2 = stageIndexToExecutor.getOrElse((stageId, index2), "unknown")
    logInfo(s"[MERKLE] Original executors: $executor1 (idx$index1), $executor2 (idx$index2)")
    
    try {
      val userHome = System.getProperty("user.home")
      val appName = Option(SparkEnv.get).flatMap(env => Option(env.conf.get("spark.app.name", "unknown"))).getOrElse("unknown")
      val debugMode = envOrElse("DEBUG_MODE", "false").toBoolean
      val isBinary = !debugMode
      val ext = if (debugMode) ".log" else ".bin"
      val finalDir = if (debugMode) "logs" else "bins"
      
      // Finals file paths
      val finalsPath1 = s"$userHome/spark/spark-trace/$appName/$finalDir/spark_finals_stage${stageId}_idx${index1}_p${partitionId}${ext}"
      val finalsPath2 = s"$userHome/spark/spark-trace/$appName/$finalDir/spark_finals_stage${stageId}_idx${index2}_p${partitionId}${ext}"
      
      // Merkle tree output paths
      val merkleDir = s"$userHome/spark/spark-trace/$appName/merkle"
      new java.io.File(merkleDir).mkdirs()
      val merklePath1 = s"$merkleDir/merkle_stage${stageId}_idx${index1}_p${partitionId}.bin"
      val merklePath2 = s"$merkleDir/merkle_stage${stageId}_idx${index2}_p${partitionId}.bin"
      
      // Check if finals files exist
      if (!new java.io.File(finalsPath1).exists() || !new java.io.File(finalsPath2).exists()) {
        logWarning(s"[MERKLE] Finals files not found, cannot build Merkle trees")
        return None
      }
      
      // Always build trees on executors (files are local there, faster than driver)
      logInfo(s"[MERKLE] Submitting tree build tasks to executors")
      val outcome = buildMerkleTreesOnExecutors(stageId, index1, index2, partitionId, finalsPath1, finalsPath2, isBinary, taskScheduler)
      
      // Handle outcome
      outcome match {
        case EarlyVerdict(correctIdx, byzantineIdx, reason) =>
          // Executor timed out - declare responsive executor correct
          logInfo(s"[MERKLE] Executor timeout: $reason")
          logInfo(s"[MERKLE] Early verdict: responsive executor is correct")
          // Cleanup and return special signal: uid=-1, correctIndex, byzantineIndex
          new java.io.File(merklePath1).delete()
          new java.io.File(merklePath2).delete()
          Some((-1L, correctIdx, byzantineIdx))
          
        case BothTreesBuilt(tree1, tree2) =>
          // Normal case: compare trees
          logInfo(s"[MERKLE] Comparing trees...")
          val disagreement = org.apache.spark.rdd.MerkleTree.verify(tree1, tree2)
          
          disagreement match {
            case Some((uid, hash1, hash2)) =>
              logInfo(s"[MERKLE] Found disagreement at UID $uid (idx$index1 leafHash=$hash1, idx$index2 leafHash=$hash2)")
            case None =>
              logWarning(s"[MERKLE] Trees match but hashes differ - possible hash collision or race condition")
          }
          
          // Cleanup Merkle tree files
          new java.io.File(merklePath1).delete()
          new java.io.File(merklePath2).delete()
          logInfo(s"[MERKLE] Cleaned up Merkle tree files")
          
          disagreement
      }
      
    } catch {
      case e: Exception =>
        logError(s"[MERKLE] Failed to build or compare trees: ${e.getMessage}", e)
        None
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
            localProperties = new java.util.Properties(),
            serializedTaskMetrics = serializedMetrics
          )
          
          // Submit both tasks as a single TaskSet
          val taskSet = new TaskSet(
            tasks = Array(task1, task2),
            stageId = stageId,
            stageAttemptId = 0,
            priority = Int.MaxValue,  // Highest priority
            properties = new java.util.Properties(),
            resourceProfileId = 0
          )
          
        logInfo(s"[MERKLE] Submitting TaskSet with 2 tree build tasks")
        taskScheduler.submitTasks(taskSet)
        
        // Wait for results with timeout
        logInfo(s"[MERKLE] Waiting for tree build results (timeout=${merkleTreeBuildTimeoutMs}ms)")
        val result1Opt = waitForMerkleTreeBuildResult(stageId, index1, partitionId, merkleTreeBuildTimeoutMs)
        val result2Opt = waitForMerkleTreeBuildResult(stageId, index2, partitionId, merkleTreeBuildTimeoutMs)
        
        // Smart fallback: use partial results if available
        (result1Opt, result2Opt) match {
          case (Some(result1), Some(result2)) =>
            // Both executors responded - proceed with normal comparison
            logInfo(s"[MERKLE] ✓ Successfully received both tree build results from executors")
            logInfo(s"[MERKLE] Tree 1: ${result1.leafCount} leaves, rootHash=${result1.rootHash}, buildTime=${result1.buildTimeMs}ms")
            logInfo(s"[MERKLE] Tree 2: ${result2.leafCount} leaves, rootHash=${result2.rootHash}, buildTime=${result2.buildTimeMs}ms")
            BothTreesBuilt(result1.tree, result2.tree)
            
          case (Some(result1), None) =>
            // Only executor 1 responded - declare it CORRECT (timeout = Byzantine behavior)
            logWarning(s"[MERKLE] ⚠ Replica 2 (idx$index2) timed out on tree building")
            logInfo(s"[✓] EARLY VERDICT: Replica 1 (idx$index1) is CORRECT (responded in ${result1.buildTimeMs}ms)")
            logInfo(s"[✓] EARLY VERDICT: Replica 2 (idx$index2) is BYZANTINE (timeout = non-responsive)")
            logInfo(s"[MERKLE] Skipping tree comparison - timeout indicates Byzantine behavior")
            EarlyVerdict(index1, index2, s"Replica $index2 timed out after ${merkleTreeBuildTimeoutMs}ms")
            
          case (None, Some(result2)) =>
            // Only executor 2 responded - declare it CORRECT (timeout = Byzantine behavior)
            logWarning(s"[MERKLE] ⚠ Replica 1 (idx$index1) timed out on tree building")
            logInfo(s"[✓] EARLY VERDICT: Replica 2 (idx$index2) is CORRECT (responded in ${result2.buildTimeMs}ms)")
            logInfo(s"[✓] EARLY VERDICT: Replica 1 (idx$index1) is BYZANTINE (timeout = non-responsive)")
            logInfo(s"[MERKLE] Skipping tree comparison - timeout indicates Byzantine behavior")
            EarlyVerdict(index2, index1, s"Replica $index1 timed out after ${merkleTreeBuildTimeoutMs}ms")
            
          case (None, None) =>
            // Both executors timed out - cannot build trees, must do full task recomputation
            logWarning(s"[MERKLE] ✗ Both replicas timed out on tree building")
            logWarning(s"[MERKLE] Cannot determine correct replica from tree building, throwing exception to trigger full task recomputation")
            throw new RuntimeException("Both executors timed out building Merkle trees - full task recomputation required")
        }
      } catch {
        case e: Exception =>
          logError(s"[MERKLE] Task submission failed: ${e.getMessage}", e)
          logWarning(s"[MERKLE] Throwing exception to trigger full task recomputation")
          throw new RuntimeException(s"Merkle tree task submission failed: ${e.getMessage}", e)
      }
    } catch {
      case e: Exception =>
        logError(s"[MERKLE] Unexpected error: ${e.getMessage}", e)
        logWarning(s"[MERKLE] Throwing exception to trigger full task recomputation")
        throw new RuntimeException(s"Merkle tree building failed unexpectedly: ${e.getMessage}", e)
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
      val userHome = System.getProperty("user.home")
      val appName = Option(SparkEnv.get).flatMap(env => Option(env.conf.get("spark.app.name", "unknown"))).getOrElse("unknown")
      val debugMode = envOrElse("DEBUG_MODE", "false").toBoolean
      val finalDir = if (debugMode) "logs" else "bins"
      val dir = new File(s"$userHome/spark/spark-trace/$appName/$finalDir")

      // Determine file extension based on mode (committed files, not .tmp)
      val ext = if (debugMode) ".log" else ".bin"
      
      // Construct file paths for committed files
      val file1 = new File(dir, s"spark_finals_stage${stageId}_idx${index1}_p${partitionId}${ext}")
      val file2 = new File(dir, s"spark_finals_stage${stageId}_idx${index2}_p${partitionId}${ext}")
      
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
        logInfo(s"[✓] FILE VERIFICATION PASSED: Replica files are identical for stage $stageId, partition $partitionId")
      } else {
        logError(s"[✗] FILE VERIFICATION FAILED: Replica files differ for stage $stageId, partition $partitionId")
        logError(s"    File 1: ${bytes1.length} bytes, hash=$hash1")
        logError(s"    File 2: ${bytes2.length} bytes, hash=$hash2")
      }
    } catch {
      case e: Exception =>
        logError(s"[!] Error reading replica files for stage $stageId, partition $partitionId: ${e.getMessage}")
    }
  }
  


}