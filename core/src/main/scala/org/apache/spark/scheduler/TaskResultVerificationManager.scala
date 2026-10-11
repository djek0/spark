package org.apache.spark.scheduler

import org.apache.spark.SparkEnv

import java.nio.ByteBuffer
import scala.util.Properties.envOrElse
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.{HashMap, HashSet}
import org.apache.spark.internal.Logging
import scala.util.control.NonFatal
import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.verification.{ChunkConfig, ChunkDigest, DigestView}


object TaskResultVerificationManager extends Logging {

  // Reference to DAGScheduler for verdict callbacks
  private var dagScheduler: DAGScheduler = _
  
  def setDAGScheduler(scheduler: DAGScheduler): Unit = {
    dagScheduler = scheduler
  }

  // Thread safety: the four maps below are written from different threads -- task launch
  // (TaskSetManager.prepareLaunchingTask -> addNewRunningTask), the 4 TaskResultGetter threads
  // (addNewResultForTid) and the DAG event loop (consensus, dispatch, cleanup). Plain HashMaps lost
  // entries under that load ("Task N not found in running tasks, cannot store result hash"), which
  // sent honest pairs into verification and timeouts. TrieMap is a lock-free concurrent mutable.Map
  // with snapshot-consistent iteration, so the existing code works on it unchanged.
  // Maps taskId -> (stageId, taskIndex) where taskIndex is the array index (not taskId!)
  val tidToStageIndexInfo = new TrieMap[Long, (Int, Int)]
  // Maps (stageId, taskIndex) -> resultHash
  val stageIndexToResultHash = new TrieMap[(Int, Int), String]
  // Track partitions with verification in progress to prevent duplicate verification tasks
  var verificationInProgress = new HashSet[(Int, Int)]()
  // Store Task objects for driver recomputation (only original tasks, not verification tasks)
  val stageIndexToOriginalTask = new TrieMap[(Int, Int), Task[_]]()
  // Track which executor and host ran which task (for host-level locality and third-executor verification)
  val stageIndexToExecutor = new TrieMap[(Int, Int), (String, String)]()
  // Track partitions where consensus was found (hashes match)
  private val consensusFound = new scala.collection.mutable.HashSet[(Int, Int)]()
  // Track partitions that have retried with full-task verification after NEITHER_MATCH
  private val stagePartitionFullTaskRetry = new scala.collection.mutable.HashSet[(Int, Int)]()
  // Chunk digests of replica results, keyed (stageId, taskIndex). Written by TaskResultGetter
  // threads, read and removed on the DAG thread, hence concurrent.
  private val chunkDigests = new TrieMap[(Int, Int), DigestView]()

  // Configuration: Enable third-executor verification
  private val useExecutorVerification = envOrElse("EXEC_VERIFICATION", "true").toBoolean
  // Configuration: Enable chunk localization (recompute only the first differing chunk instead of the full task)
  private val useMerkleVerification = envOrElse("MERKLE_VERIFICATION", "true").toBoolean
  // Configuration: Restrict verification to trusted executor pool (e.g., SGX nodes, on-prem servers)
  private val trustedVerifiersOnly = envOrElse("TRUSTED_VERIFIERS_ONLY", "false").toBoolean
  // Configuration: Modulo divisor for trusted pool (default 2 = executors 0,2,4,6... are trusted)
  private val trustedVerifierModulo = envOrElse("TRUSTED_VERIFIER_MODULO", "2").toInt

  logInfo(s"[CONFIG] Executor verification enabled: $useExecutorVerification")
  logInfo(s"[CONFIG] Merkle tree verification enabled: $useMerkleVerification")
  logInfo(s"[CONFIG] Trusted verifiers only: $trustedVerifiersOnly")
  if (trustedVerifiersOnly) {
    logInfo(s"[CONFIG] Trusted verifier modulo: $trustedVerifierModulo (executor IDs divisible by $trustedVerifierModulo)")
  }
  // MERKLE_LOCALIZATION selected trees or shadow mode before C2; chunks are the only localization now
  envOrElse("MERKLE_LOCALIZATION", "chunks").trim.toLowerCase(java.util.Locale.ROOT) match {
    case "chunks" =>
    case other => logWarning(s"[CONFIG] MERKLE_LOCALIZATION=$other is no longer supported " +
      "(Merkle trees were removed); using chunks")
  }

  // NOTE: Accumulator warnings ("Attempted to access garbage collected accumulator") are EXPECTED
  // They occur when verification tasks try to access accumulators from original tasks after GC
  // This is harmless and does not affect correctness - verification uses RDD recomputation, not accumulators

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

    // Remove chunk digests
    chunkDigests.remove(stageIndex1)
    chunkDigests.remove(stageIndex2)
    
    logDebug(s"[CLEANUP] Removed task references for stage $stageId, indexes $index1 and $index2")
  }

  /**
   * Post a verification verdict and retry stage cleanup.
   * After posting a verdict, we check if the stage can now be cleaned up.
   */
  private def postVerdictAndRetryCleanup(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      verdict: String,
      verifierResult: Option[Any]): Unit = {
    // Post the verdict
    dagScheduler.eventProcessLoop.post(
      VerificationVerdictEvent(stageId, index1, index2, partitionId, verdict, verifierResult))
    chunkDigests.remove((stageId, index1))
    chunkDigests.remove((stageId, index2))
    
    // Retry cleanup in case this was the last pending verification for the stage
    logInfo(s"[CLEANUP] Verdict posted for stage $stageId partition $partitionId - retrying stage cleanup")
    cleanupStage(stageId)
  }

  /**
   * Clean up all data structures for a completed stage to prevent memory leaks.
   * Should be called when a stage completes (success or failure).
   * @param stageId The stage ID to clean up
   */
  def cleanupStage(stageId: Int): Unit = {
    // Check if any verification is still pending for this stage
    val hasPendingVerification = verificationInProgress.exists(_._1 == stageId)
    val hasConsensusIssues = stageIndexToResultHash.keys.filter(_._1 == stageId).exists { case (sid, idx) =>
      val partitionId = idx / 2  // Convert taskIndex to partitionId
      !consensusFound.contains((sid, partitionId))
    }
    
    if (hasPendingVerification || hasConsensusIssues) {
      logWarning(s"[CLEANUP] Stage $stageId has pending verification or consensus issues - DELAYING cleanup")
      logWarning(s"[CLEANUP] Pending verification: $hasPendingVerification, Consensus issues: $hasConsensusIssues")
      logWarning(s"[CLEANUP] Task data will be retained for verification (prevents ShuffleMapTask detection failure)")
      return  // Don't cleanup yet!
    }
    
    logInfo(s"[CLEANUP] Starting cleanup for stage $stageId (no pending verification)")
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

    // Remove all verification in progress for this stage
    val verificationInProgressToRemove = verificationInProgress.filter(_._1 == stageId).toList
    verificationInProgressToRemove.foreach(verificationInProgress.remove)
    
    // Remove all pending verifications for this stage
    val pendingToRemove = pendingVerifications.keys.filter(_._1 == stageId).toList
    pendingToRemove.foreach(pendingVerifications.remove)
    
    // Remove all consensus flags for this stage
    val consensusToRemove = consensusFound.filter(_._1 == stageId).toList
    consensusToRemove.foreach(consensusFound.remove)

    // Remove all chunk digests for this stage
    chunkDigests.keys.filter(_._1 == stageId).toList.foreach(chunkDigests.remove)

    logInfo(s"[CLEANUP] Cleaned up all data structures for stage $stageId")
  }

  def addNewResultForTid(tid: Long, resultHash: String): Unit = {
    if(tidToStageIndexInfo.contains(tid)){
      val stageIndex = tidToStageIndexInfo(tid)
      stageIndexToResultHash(stageIndex) = resultHash
      logInfo(s"[HASH STORE] Stored result hash for task $tid (stage ${stageIndex._1}, index ${stageIndex._2}): $resultHash")
    } else {
      logInfo(s"[HASH STORE] Task $tid not found in running tasks, cannot store result hash")
    }
  }

  /** Store a replica's chunk digest. Called by TaskResultGetter threads, like addNewResultForTid. */
  def storeChunkDigest(tid: Long, d: DigestView): Unit =
    tidToStageIndexInfo.get(tid).foreach(si => chunkDigests.put(si, d))

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
              // Normal mode: hashes match - consensus found
              logInfo(s"[+] CONSENSUS: Valid result for stage $stageId, partition $partitionId (indexes ${if (index % 2 == 0) s"$index, $partnerIndex" else s"$partnerIndex, $index"})")
              consensusFound.add((stageId, partitionId))
              logInfo(s"[CONSENSUS FLAG] Set consensus flag for stage $stageId, partition $partitionId")
              // Remove from verificationInProgress since no verification needed (consensus reached)
              verificationInProgress.remove(partitionKey)
              cleanupPartitionTasks(stageId, index, partnerIndex)
            } else {
              // Hashes differ - verification needed
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
      logInfo(s"[!] Task $tid not found in running tasks during verification")
    }
  }

  /**
   * Dispatch verification to appropriate method based on configuration.
   * Two independent config axes:
   * - EXEC_VERIFICATION: false=driver, true=third-executor
   * - MERKLE_VERIFICATION: false=full task, true=chunk localization
   *
   * The replicas' chunk digests arrive with their task results, so the first differing chunk is
   * known at once; the verifier (driver or third executor) recomputes only that chunk.
   */
  private def dispatchVerification(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      taskScheduler: TaskScheduler): Unit = {
    
    logInfo(s"[VERIFICATION] Dispatching: EXEC_VERIFICATION=$useExecutorVerification, MERKLE_VERIFICATION=$useMerkleVerification")
    
    // Check if stage READS from shuffle (driver cannot execute such tasks due to null taskMemoryManager)
    val involvesShuffleOps = stageReadsFromShuffle(stageId)
    
    // Note: ShuffleMapTask (shuffle-WRITE stages) are handled separately:
    // - They might have hidden collapser transformations (e.g., sortByKey sampling)
    // - Chunk verification is skipped for them (isFinalStage)
    // - Full-task verification is used instead

    // Auto-switch to executor verification if driver verification was requested but stage READS from shuffle
    if (involvesShuffleOps && !useExecutorVerification) {
      logWarning(s"[DISPATCH] Stage $stageId reads from shuffle - driver verification NOT supported")
      logWarning(s"[DISPATCH] Reason: Driver TaskContext has taskMemoryManager=null, causing NullPointerException during shuffle read")
      logWarning(s"[DISPATCH] Auto-switching to THIRD EXECUTOR verification for safety")
      logWarning(s"[DISPATCH] To avoid this warning, set EXEC_VERIFICATION=true for stages with shuffle operations")
      // Cancel the driver timeout timer that was already started (executor verification uses different timeout mechanism)
      val cancelled = dagScheduler.cancelVerificationTimeout(stageId, partitionId)
      if (cancelled) {
        logInfo(s"[DISPATCH] Cancelled driver verification timeout after auto-switch to executor mode")
      }
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
   * If MERKLE_VERIFICATION=true: Localize the first differing chunk from the replicas' digests,
   *   then submit chunk verification to third executor (full task when no chunk is found).
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
    logDebug(s"[DISPATCH] Stage $stageId partition $partitionId: Looking up task for indexes ($index1, $index2)")
    val taskOpt = stageIndexToOriginalTask.get((stageId, index1))
      .orElse(stageIndexToOriginalTask.get((stageId, index2)))
    logDebug(s"[DISPATCH] Stage $stageId partition $partitionId: Task found = ${taskOpt.isDefined}")

    taskOpt match {
      case None =>
        logError(s"[X] DISPATCH-DEBUG: Task not found for stage $stageId partition $partitionId, falling back to driver")
        logError(s"[X] DISPATCH-DEBUG: stageIndexToOriginalTask keys for stage $stageId: ${stageIndexToOriginalTask.keys.filter(_._1 == stageId).take(5).toList}")
        // Restart timeout for driver verification (mark as last resort and restart timer)
        dagScheduler.restartTimeoutForDriverVerification(stageId, partitionId)
        verifyOnDriver(stageId, index1, index2, partitionId, taskScheduler)

      case Some(task) =>
        try {
          // Determine verification strategy based on stage position:
          // - Final stages: Use Merkle (safe, deterministic transformations only)
          // - Intermediate stages: Use full-task (may contain pseudo-deterministic ops like seeded sampling)
          val isFinalStage = dagScheduler.isFinalStage(stageId)
          val shouldUseMerkle = useMerkleVerification && isFinalStage
          
          logDebug(s"[DISPATCH] Stage $stageId partition $partitionId: Task type = ${task.getClass.getSimpleName}")
          logDebug(s"[DISPATCH] Stage $stageId partition $partitionId: isFinalStage = $isFinalStage")
          logDebug(s"[DISPATCH] Stage $stageId partition $partitionId: useMerkleVerification = $useMerkleVerification")
          logDebug(s"[DISPATCH] Stage $stageId partition $partitionId: shouldUseMerkle = $shouldUseMerkle")
          
          if (!isFinalStage && useMerkleVerification) {
            logInfo(s"[VERIFICATION] Stage $stageId is intermediate - using FULL-TASK verification")
          }
          
          if (shouldUseMerkle && ChunkConfig.useChunks) {
            // Chunk mode: localize with the replicas' digests (no tree round), then one verifier
            // recomputes only that chunk; anything unsure goes to full-task
            chunkLocalize(stageId, index1, index2, partitionId) match {
              case Some((lo, hi, boxA, boxB)) =>
                submitVerificationTaskToExecutor(task, index1, index2, excludedExecutors, taskScheduler,
                  targetRange = Some((lo, hi)), chunkBoxes = Some((boxA, boxB)))
              case None =>
                submitVerificationTaskToExecutor(task, index1, index2, excludedExecutors, taskScheduler)
            }
          } else {
            // Full task verification on third executor
            logDebug(s"[DISPATCH] Stage $stageId partition $partitionId: ✓ Using FULL-TASK verification (isFinalStage=$isFinalStage)")
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
      targetRange: Option[(Long, Long)] = None,
      chunkBoxes: Option[(Long, Long)] = None): Unit = {

    val modeStr = targetRange.map { case (lo, hi) => s"chunk (UIDs [$lo, $hi))" }.getOrElse("full-task")
    logInfo(s"[THIRD-EXECUTOR] Creating verification task (excluding: $excludedExecutors) - mode: $modeStr")

    // Get replica hashes to pass to executor
    val hash1 = stageIndexToResultHash.getOrElse((task.stageId, index1), "MISSING")
    val hash2 = stageIndexToResultHash.getOrElse((task.stageId, index2), "MISSING")

    logInfo(s"[THIRD-EXECUTOR] Passing replica hashes to executor: idx$index1=$hash1, idx$index2=$hash2")

    // Replica pair k = index / 2. Verifier bookkeeping is keyed by (stage, k), like
    // pendingVerificationResults and the timeouts; k differs from task.partitionId when the
    // TaskSet does not cover all partitions in order (take/first/lookup, partial retries).
    val pairIndex = math.min(index1, index2) / 2

    // Create verification task WITH HASHES (and the chunk range and boxes in chunk mode)
    val verificationTask = new VerificationTask(
      stageId = task.stageId,
      stageAttemptId = task.stageAttemptId,
      partitionId = task.partitionId,
      originalTask = task,
      pairIndex = pairIndex,
      excludedExecutors = excludedExecutors,
      replicaIndex1 = index1,
      replicaIndex2 = index2,
      replicaHash1 = hash1,
      replicaHash2 = hash2,
      localProperties = task.localProperties,
      // A fresh driver-registered TaskMetrics. Re-serializing the
      // driver's deserialized task.metrics fails on cluster executors with
      // "Accumulator must be registered before send to executor".
      serializedTaskMetrics = SparkEnv.get.closureSerializer.newInstance()
        .serialize(TaskMetrics.registered).array(),
      jobId = task.jobId,
      appId = task.appId,
      appAttemptId = task.appAttemptId,
      targetRange = targetRange,
      chunkBoxes = chunkBoxes
    )

    // Wrap in TaskSet with single task
    // CRITICAL FIX: Use a very high stageAttemptId to avoid collision with original TaskSet
    // Original TaskSet typically has stageAttemptId = 0
    // Verification TaskSets use stageAttemptId = 1000000 + k (replica pair) to ensure uniqueness
    // This prevents verification TaskSet completion from removing the original TaskSet from pool
    val verificationStageAttemptId = 1000000 + pairIndex
    
    val verificationProperties = new java.util.Properties()
    verificationProperties.putAll(task.localProperties.asInstanceOf[java.util.Map[_, _]])
    verificationProperties.setProperty("spark.internal.task.type", "verification")

    val verificationTaskSet = new TaskSet(
      tasks = Array(verificationTask),
      stageId = task.stageId,  // Use ORIGINAL stageId to stay part of the same stage
      stageAttemptId = verificationStageAttemptId,  // Unique attempt ID for verification
      priority = Int.MinValue,  // Highest priority - prioritise verification tasks to complete first
      properties = verificationProperties,
      resourceProfileId = 0,  // Default resource profile
      isAuxiliary = true  // Mark as auxiliary to prevent zombie conflicts
    )

    // Store metadata so we can process result when task completes
    val partitionKey = (task.stageId, pairIndex)
    pendingVerifications(partitionKey) = (index1, index2)

    logInfo(s"[THIRD-EXECUTOR] Submitting verification TaskSet for stage ${task.stageId}, partition ${task.partitionId} (pair $pairIndex)")
    logWarning(s"[TASK ID CONSUMPTION] Verification TaskSet will consume task IDs when scheduled (1 task for partition ${task.partitionId})")

    // Submit to scheduler (non-blocking)
    taskScheduler.submitTasks(verificationTaskSet)
  }

  /**
   * Complete a verification task and process its result.
   * Called by DAGScheduler when verification task completes.
   */
  def completeVerificationTask(stageId: Int, partitionId: Int, result: Any): Unit = {
    // partitionId here is the replica pair k (VerificationTask.pairIndex), not the RDD partition
    val partitionKey = (stageId, partitionId)
    pendingVerifications.get(partitionKey) match {
      case Some((index1, index2)) =>
        logInfo(s"[THIRD-EXECUTOR] Verification task for stage $stageId, partition $partitionId completed")
        // Remove the key BEFORE handling: on NEITHER_MATCH the handler resubmits a full-task
        // retry, which re-registers this key; removing it afterwards would drop the retry's verdict.
        pendingVerifications.remove(partitionKey)
        handleThirdExecutorResult(stageId, index1, index2, partitionId, result)
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
            // Third executor differs from both - this might be:
            // 1. Chunk verification of a non-pointwise stage (e.g., zipWithIndex, glom)
            // 2. Actual Byzantine fault in verifier
            // Try full-task verification before falling back to driver
            val wasFullTask = stagePartitionFullTaskRetry.contains((stageId, partitionId))
            if (!wasFullTask) {
              logWarning(s"[THIRD-EXECUTOR] NEITHER_MATCH - might be aggregation stage, retrying with full-task verification")
              stagePartitionFullTaskRetry.add((stageId, partitionId))
              // Resubmit as full-task verification
              stageIndexToOriginalTask.get((stageId, index1))
                .orElse(stageIndexToOriginalTask.get((stageId, index2)))
                .foreach { task =>
                  val (executor1, _) = stageIndexToExecutor.getOrElse((stageId, index1), ("unknown", "unknown"))
                  val (executor2, _) = stageIndexToExecutor.getOrElse((stageId, index2), ("unknown", "unknown"))
                  submitVerificationTaskToExecutor(task, index1, index2, Set(executor1, executor2), dagScheduler.taskScheduler)
                }
              fallbackToDriver = true  // Keep task references in case full-task also fails
            } else {
              // Full-task verification also failed - fall back to driver
              logWarning(s"[THIRD-EXECUTOR] NEITHER_MATCH on full-task verification - falling back to driver")
              dagScheduler.restartTimeoutForDriverVerification(stageId, partitionId)
              executeOnDriver(stageId, index1, index2, partitionId)
              fallbackToDriver = true
            }

          case _ =>
            // Report verdict to DAGScheduler (REPLICA_1_CORRECT, REPLICA_2_CORRECT, BOTH_MATCH)
            postVerdictAndRetryCleanup(stageId, index1, index2, partitionId, verdict.verdict, None)
        }

      case _ =>
        logError(s"[X] Unexpected result type from verification task: ${verifierResult.getClass}")
        logError(s"[X] Expected VerificationVerdict, got: $verifierResult")
        // Fall back to driver verification
        logWarning(s"[X] Falling back to driver verification due to unexpected result")
        // Cancel existing timeout and restart with longer duration for driver verification
        dagScheduler.restartTimeoutForDriverVerification(stageId, partitionId)
        executeOnDriver(stageId, index1, index2, partitionId)
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
   * If MERKLE_VERIFICATION=true: Localize the first differing chunk from the replicas' digests,
   *   then recompute only that chunk on driver (full task when no chunk is found or the check
   *   is inconclusive).
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

    // Determine verification strategy
    val taskOpt = stageIndexToOriginalTask.get((stageId, index1))
      .orElse(stageIndexToOriginalTask.get((stageId, index2)))
    val isShuffleMapTask = taskOpt.exists(_.isInstanceOf[ShuffleMapTask])
    val readsFromShuffle = stageReadsFromShuffle(stageId)
    val isFinalStage = dagScheduler.isFinalStage(stageId)
    val driverCanExecute = !isShuffleMapTask && !readsFromShuffle
    val shouldUseMerkle = useMerkleVerification && isFinalStage && driverCanExecute
    
    logDebug(s"[DRIVER] Stage $stageId partition $partitionId: isFinalStage = $isFinalStage")
    logDebug(s"[DRIVER] Stage $stageId partition $partitionId: isShuffleMapTask = $isShuffleMapTask")
    logDebug(s"[DRIVER] Stage $stageId partition $partitionId: readsFromShuffle = $readsFromShuffle")
    logDebug(s"[DRIVER] Stage $stageId partition $partitionId: driverCanExecute = $driverCanExecute")
    logDebug(s"[DRIVER] Stage $stageId partition $partitionId: shouldUseMerkle = $shouldUseMerkle")
    
    if (!isFinalStage && useMerkleVerification) {
      logInfo(s"[DRIVER] Stage $stageId is intermediate - using FULL-TASK verification")
    }
    if (!driverCanExecute) {
      logInfo(s"[DRIVER] Stage $stageId involves shuffle operations - delegating to executor")
    }

    if (shouldUseMerkle && ChunkConfig.useChunks) {
      chunkLocalize(stageId, index1, index2, partitionId) match {
        case Some((lo, hi, boxA, boxB)) =>
          executeChunkOnDriver(stageId, index1, index2, partitionId, lo, hi, boxA, boxB)
        case None =>
          executeOnDriver(stageId, index1, index2, partitionId)
      }
    } else {
      // Full task recomputation (MERKLE_VERIFICATION=false or NOT final stage or driver cannot execute)
      if (!driverCanExecute) {
        logWarning(s"[DRIVER] Driver cannot execute stage $stageId - delegating to executor full-task verification")
        logWarning(s"[DRIVER] Switching to executor for full-task verification")
        taskOpt.foreach { task =>
          val (executor1, _) = stageIndexToExecutor.getOrElse((stageId, index1), ("unknown", "unknown"))
          val (executor2, _) = stageIndexToExecutor.getOrElse((stageId, index2), ("unknown", "unknown"))
          submitVerificationTaskToExecutor(task, index1, index2, Set(executor1, executor2), taskScheduler)
        }
      } else {
        executeOnDriver(stageId, index1, index2, partitionId)
      }
    }
  }

  /**
   * Chunk mode: the first differing chunk of the two replicas' digests, as (lo, hi, box of index1,
   * box of index2), or None when it cannot be localized (a digest is missing, invalid or equal, or
   * the difference lies in a partly read stream); the caller then uses full-task verification.
   */
  private def chunkLocalize(stageId: Int, index1: Int, index2: Int,
      partitionId: Int): Option[(Long, Long, Long, Long)] = {
    val a = chunkDigests.get((stageId, index1))
    val b = chunkDigests.get((stageId, index2))
    val r = for (x <- a; y <- b; d <- ChunkDigest.firstDifference(x, y)) yield d
    logWarning(s"[CHUNK] stage $stageId partition $partitionId: " + (r match {
      case Some((lo, hi, _, _)) => s"first differing chunk = UIDs [$lo, $hi)"
      case None =>
        val why = if (a.isEmpty || b.isEmpty) "digest missing" else "equal/invalid/partial"
        s"no chunk ($why) -> full-task"
    }))
    r
  }

  /**
   * Chunk mode on the driver: recompute only UIDs [lo, hi) up to the logging level and compare the
   * box with both replicas' boxes. Never posts NEITHER_MATCH: an unsure result goes to the existing
   * full-task driver path, which commits a full result.
   */
  private def executeChunkOnDriver(stageId: Int, index1: Int, index2: Int, partitionId: Int,
      lo: Long, hi: Long, boxA: Long, boxB: Long): Unit = {
    val rt = stageIndexToOriginalTask.get((stageId, index1))
      .orElse(stageIndexToOriginalTask.get((stageId, index2))) match {
      case Some(r: ResultTask[_, _]) => r
      case _ =>
        executeOnDriver(stageId, index1, index2, partitionId)
        return
    }
    val tmm = new org.apache.spark.memory.TaskMemoryManager(SparkEnv.get.memoryManager, -1L)
    val ctx = new TaskContextImpl(
      stageId = rt.stageId,
      stageAttemptNumber = rt.stageAttemptId,
      partitionId = rt.partitionId,
      taskAttemptId = -1L,
      attemptNumber = 0,
      taskIndex = index1,
      taskMemoryManager = tmm,
      localProperties = rt.localProperties,
      metricsSystem = SparkEnv.get.metricsSystem,
      taskMetrics = TaskMetrics.empty,
      resources = Map.empty,
      targetRange = Some((lo, hi)))
    ctx.rangeBox = new org.apache.spark.verification.RangeBox(lo, hi)
    val t0 = System.nanoTime()
    val v: Option[Long] =
      try {
        TaskContext.setTaskContext(ctx)
        val it = rt.iteratorForVerification(ctx)
        while (it.hasNext) it.next()
        Some(ctx.rangeBox.sum)
      } catch {
        case NonFatal(e) =>
          logWarning(s"[CHUNK] driver chunk check failed: ${e.getMessage}")
          None
      } finally {
        try ctx.markTaskCompleted(None) catch {
          case NonFatal(e) => logWarning(s"[CHUNK] completion listeners failed: ${e.getMessage}")
        }
        TaskContext.unset()
        try tmm.cleanUpAllAllocatedMemory() catch {
          case NonFatal(e) => logWarning(s"[CHUNK] memory cleanup failed: ${e.getMessage}")
        }
      }
    logWarning(s"[PERF-CHUNK-VERIFY-DRIVER] Stage $stageId partition $partitionId: " +
      s"${(System.nanoTime() - t0) / 1e9}s (UIDs [$lo, $hi), ${ctx.rangeBox.count} outputs)")
    v match {
      case Some(x) if x == boxA && x != boxB =>
        logWarning(s"[VERDICT] Replica 1 (idx$index1) is CORRECT, Replica 2 (idx$index2) is BYZANTINE")
        postVerdictAndRetryCleanup(stageId, index1, index2, partitionId, "REPLICA_1_CORRECT", None)
        cleanupPartitionTasks(stageId, index1, index2)
      case Some(x) if x == boxB && x != boxA =>
        logWarning(s"[VERDICT] Replica 2 (idx$index2) is CORRECT, Replica 1 (idx$index1) is BYZANTINE")
        postVerdictAndRetryCleanup(stageId, index1, index2, partitionId, "REPLICA_2_CORRECT", None)
        cleanupPartitionTasks(stageId, index1, index2)
      case _ =>
        logWarning(s"[CHUNK] driver chunk check inconclusive -> full-task on the driver")
        executeOnDriver(stageId, index1, index2, partitionId)
    }
  }

  /**
   * Recompute the whole task on the driver and compare its result hash with both replicas.
   */
  private def executeOnDriver(stageId: Int, index1: Int, index2: Int, partitionId: Int): Unit = {

    logInfo(s"[DRIVER RECOMPUTE] Starting driver recomputation for stage $stageId, partition $partitionId (full task)")

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
            resources = Map.empty
          )
          driverTaskContext.fullTaskVerifier = true   // a verifier: writes no replica trace files

          // Set the context
          TaskContext.setTaskContext(driverTaskContext)

          // Start timer for driver task execution
          val driverTaskStartTime = System.nanoTime()

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

          // Log driver task execution time
          val driverTaskTime = (System.nanoTime() - driverTaskStartTime) / 1e9
          logWarning(s"[PERF-FULLTASK-DRIVER] Stage $stageId partition $partitionId: ${driverTaskTime}s")

          // Compare using serialized result hashes; ShuffleMapTask: block sizes, as Executor.scala
          val driverHash = if (task.isInstanceOf[ShuffleMapTask]) {
            val mapStatus = driverResult.asInstanceOf[MapStatus]
            // Exactly one size per reduce partition (see ShuffleMapTask.blockSizesForHash)
            val blockSizes = task.asInstanceOf[ShuffleMapTask].blockSizesForHash(mapStatus)
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
              logWarning(s"[VERDICT] Replica 1 (idx$index1) is CORRECT, Replica 2 (idx$index2) is BYZANTINE")
              postVerdictAndRetryCleanup(stageId, index1, index2, partitionId, "REPLICA_1_CORRECT", Some(driverResult))

            case (false, true) =>
              logWarning(s"[VERDICT] Replica 2 (idx$index2) is CORRECT, Replica 1 (idx$index1) is BYZANTINE")
              postVerdictAndRetryCleanup(stageId, index1, index2, partitionId, "REPLICA_2_CORRECT", Some(driverResult))

            case (true, true) =>
              logWarning(s"[?] UNEXPECTED: Both replicas match driver, but were reported as different - possible race condition")
              postVerdictAndRetryCleanup(stageId, index1, index2, partitionId, "BOTH_MATCH", Some(driverResult))

            case (false, false) =>
              logError(s"[X] CRITICAL: Driver result differs from BOTH replicas - system error or driver fault!")
              logError(s"[X] Using driver result as ground truth")
              postVerdictAndRetryCleanup(stageId, index1, index2, partitionId, "NEITHER_MATCH", Some(driverResult))
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
}