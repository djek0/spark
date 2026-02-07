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
  // Store Task objects for driver recomputation
  var stageIndexToTask = new HashMap[(Int, Int), Task[_]]()
  // Track which executor ran which task (for third-executor verification)
  var stageIndexToExecutor = new HashMap[(Int, Int), String]()

  // Configuration: Enable third-executor verification
  private val useExecutorVerification = envOrElse("EXEC_VERIFICATION", "false").toBoolean
  private val verificationTimeoutMs = envOrElse("VERIFICATION_TIMEOUT_MS", "5000").toInt

  logInfo(s"[CONFIG] Executor verification enabled: $useExecutorVerification")
  logInfo(s"[CONFIG] Verification timeout: ${verificationTimeoutMs}ms")

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
    stageIndexToTask(indexStage) = task
    stageIndexToExecutor(indexStage) = executorId
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

  def addNewResultForTid(tid: Long, resultHash: String): Unit = {
    if(tidToStageIndexInfo.contains(tid)){
      val stageIndex = tidToStageIndexInfo(tid)
      stageIndexToResultHash(stageIndex) = resultHash
      logDebug(s"Stored result hash for task $tid (stage ${stageIndex._1}, index ${stageIndex._2}): $resultHash")
    } else {
      logWarning(s"[!] Task $tid not found in running tasks, cannot store result hash")
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
          
          // Always check consensus (both replicas should check this)
          if(stageIndexToResultHash(stageIndex)==stageIndexToResultHash((stageId,partnerIndex))){
            logInfo(s"[+] CONSENSUS: Valid result for stage $stageId, partition $partitionId (indexes ${if (index % 2 == 0) s"$index, $partnerIndex" else s"$partnerIndex, $index"})")
          }
          else{
            logError(s"[X] BYZANTINE FAULT DETECTED: Hash mismatch for stage $stageId, partition $partitionId (indexes ${if (index % 2 == 0) s"$index, $partnerIndex" else s"$partnerIndex, $index"})")
            
            // Check if verification already initiated for this partition
            if (!verificationInProgress.contains(partitionKey)) {
              verificationInProgress += partitionKey
              // Dispatch to appropriate verification method based on configuration
              dispatchVerification(stageId, index, partnerIndex, partitionId, taskScheduler)
            } else {
              logDebug(s"[VERIFICATION] Already initiated for partition $partitionId, skipping duplicate")
            }
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
   * - If EXEC_VERIFICATION=true: Attempt third-executor verification with timeout fallback
   * - If EXEC_VERIFICATION=false: Use driver recomputation (default)
   */
  private def dispatchVerification(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      taskScheduler: TaskScheduler): Unit = {
    
    if (useExecutorVerification) {
      logInfo(s"[VERIFICATION] Using third-executor verification (EXEC_VERIFICATION=true)")
      verifyOnThirdExecutor(stageId, index1, index2, partitionId, taskScheduler)
    } else {
      logInfo(s"[VERIFICATION] Using driver verification (EXEC_VERIFICATION=false)")
      recomputeOnDriver(stageId, index1, index2, partitionId)
    }
  }

  /**
   * Attempt to verify task result on a third executor (not the ones that ran replicas).
   * Falls back to driver recomputation on timeout or failure.
   */
  private def verifyOnThirdExecutor(
      stageId: Int,
      index1: Int,
      index2: Int,
      partitionId: Int,
      taskScheduler: TaskScheduler): Unit = {
    
    logInfo(s"[THIRD-EXECUTOR] Starting verification for partition $partitionId")
    
    // Get executors that ran the original replicas
    val executor1 = stageIndexToExecutor.getOrElse((stageId, index1), "unknown")
    val executor2 = stageIndexToExecutor.getOrElse((stageId, index2), "unknown")
    val excludedExecutors = Set(executor1, executor2)
    
    logInfo(s"[THIRD-EXECUTOR] Excluded executors: $excludedExecutors")
    
    // Get the task object
    val taskOpt = stageIndexToTask.get((stageId, index1))
      .orElse(stageIndexToTask.get((stageId, index2)))
    
    taskOpt match {
      case None =>
        logError(s"[X] Task not found, falling back to driver")
        recomputeOnDriver(stageId, index1, index2, partitionId)
        
      case Some(task) =>
        try {
          // Submit verification task to scheduler (non-blocking)
          // Result will be captured in completeVerificationTask when task finishes
          submitVerificationTaskToExecutor(task, index1, index2, excludedExecutors, taskScheduler)
          logInfo(s"[THIRD-EXECUTOR] Verification task submitted, will process result when complete")
          
        } catch {
          case e: NotImplementedError =>
            logWarning(s"[!] ${e.getMessage}, falling back to driver")
            recomputeOnDriver(stageId, index1, index2, partitionId)
            
          case e: Exception =>
            logError(s"[X] Verification failed: ${e.getMessage}, falling back to driver")
            recomputeOnDriver(stageId, index1, index2, partitionId)
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
      taskScheduler: TaskScheduler): Unit = {
    
    logInfo(s"[THIRD-EXECUTOR] Creating verification task (excluding: $excludedExecutors)")
    
    // Get replica hashes to pass to executor
    val hash1 = stageIndexToResultHash.getOrElse((task.stageId, index1), "MISSING")
    val hash2 = stageIndexToResultHash.getOrElse((task.stageId, index2), "MISSING")
    
    logInfo(s"[THIRD-EXECUTOR] Passing replica hashes to executor: idx$index1=$hash1, idx$index2=$hash2")
    
    // Create verification task WITH HASHES
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
    
    // Clear verification tracking
    val partitionKey = (stageId, partitionId)
    verificationInProgress -= partitionKey
    logDebug(s"[VERIFICATION] Cleared verification tracking for partition $partitionId")
  }

  /**
   * Recompute task on driver to resolve Byzantine fault.
   * Runs the task locally on the driver and compares the result hash with both replicas.
   */
  private def recomputeOnDriver(stageId: Int, index1: Int, index2: Int, partitionId: Int): Unit = {
    logInfo(s"[DRIVER RECOMPUTE] Starting driver recomputation for stage $stageId, partition $partitionId")
    
    val stageIndex1 = (stageId, index1)
    val stageIndex2 = (stageId, index2)
    
    // Get the task object (use either replica's task - they compute same partition)
    val taskOpt = stageIndexToTask.get(stageIndex1).orElse(stageIndexToTask.get(stageIndex2))
    
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
            resources = Map.empty
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
          
          // Hash the driver result using shared utility
          val driverHash = computeTaskResultHash(driverResult)
          
          // Compare with replicas
          val hash1 = stageIndexToResultHash.getOrElse((stageId, index1), "MISSING")
          val hash2 = stageIndexToResultHash.getOrElse((stageId, index2), "MISSING")
          
          logInfo(s"[DRIVER RECOMPUTE] Driver hash: $driverHash")
          logInfo(s"[DRIVER RECOMPUTE] Replica 1 (idx$index1) hash: $hash1")
          logInfo(s"[DRIVER RECOMPUTE] Replica 2 (idx$index2) hash: $hash2")
          
          // Determine which replica is correct
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
          
          // Clear verification tracking
          val partitionKey = (stageId, partitionId)
          verificationInProgress -= partitionKey
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