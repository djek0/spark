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
  // Store Task objects for driver recomputation
  var stageIndexToTask = new HashMap[(Int, Int), Task[_]]()

  /**
   * Register a task for verification and store it for potential driver recomputation.
   * Merged registration method to avoid HashMap duplication.
   */
  def addNewRunningTask(tid: Int, indexStage: (Int, Int), task: Task[_]): Unit = {
    if(tidToStageIndexInfo.contains(tid)){
      logDebug(s"Task $tid already registered in verification manager")
      return
    }
    logDebug(s"Registered task $tid with stage ${indexStage._1}, index ${indexStage._2}")
    tidToStageIndexInfo(tid) = indexStage
    stageIndexToTask(indexStage) = task
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

  def verifyResult(tid: Long): Unit = {
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
            // Trigger driver recomputation to determine which replica is correct
            recomputeOnDriver(stageId, index, partnerIndex, partitionId)
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

  def addResultAndVerify(tid: Long, hash: String): Unit={
    addNewResultForTid(tid,hash)
    verifyResult(tid)
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