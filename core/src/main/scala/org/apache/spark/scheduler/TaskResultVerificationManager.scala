package org.apache.spark.scheduler

import org.apache.spark.SparkEnv

import java.io.File
import java.nio.file.Files
import scala.util.Properties.envOrElse
import scala.collection.mutable.HashMap
import org.apache.spark.internal.Logging


object TaskResultVerificationManager extends Logging {

  // Maps taskId -> (stageId, taskIndex) where taskIndex is the array index (not taskId!)
  var tidToStageIndexInfo = new HashMap[Long, (Int, Int)]
  // Maps (stageId, taskIndex) -> resultHash
  var stageIndexToResultHash = new HashMap[(Int, Int), String]

  def addNewRunningTask(tid: Int, indexStage: (Int, Int)): Unit = {
    if(tidToStageIndexInfo.contains(tid)){
      logDebug(s"Task $tid already registered in verification manager")
      return
    }
    logDebug(s"Registered task $tid with stage ${indexStage._1}, index ${indexStage._2}")
    tidToStageIndexInfo(tid) = indexStage
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
        if(index%2 == 0){
          val partnerIndex = index + 1
          if(stageIndexToResultHash.contains((stageId, partnerIndex))){
            // Both replicas completed - read and verify their output files
            val partitionId = index / 2
            verifyReplicaFiles(stageId, index, partnerIndex, partitionId)
            
            if(stageIndexToResultHash(stageIndex)==stageIndexToResultHash((stageId,partnerIndex))){
              logInfo(s"[+] CONSENSUS: Valid result for stage $stageId, partition $partitionId (indexes $index, $partnerIndex)")
            }
            else{
              logError(s"[X] BYZANTINE FAULT DETECTED: Hash mismatch for stage $stageId, partition $partitionId (indexes $index, $partnerIndex)")
            }
          } else {
            logDebug(s"[*] Waiting for partner task $partnerIndex to complete")
          }
        }
        else{
          val partnerIndex = index - 1
          if(stageIndexToResultHash.contains((stageId, partnerIndex))){
            // Both replicas completed - read and verify their output files
            val partitionId = index / 2
            verifyReplicaFiles(stageId, partnerIndex, index, partitionId)
            
            if(stageIndexToResultHash(stageIndex)==stageIndexToResultHash((stageId,partnerIndex))){
              logInfo(s"[+] CONSENSUS: Valid result for stage $stageId, partition $partitionId (indexes $partnerIndex, $index)")
            }
            else{
              logError(s"[X] BYZANTINE FAULT DETECTED: Hash mismatch for stage $stageId, partition $partitionId (indexes $partnerIndex, $index)")
            }
          } else {
            logDebug(s"[*] Waiting for partner task $partnerIndex to complete")
          }
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

  def clean(): Unit = {
    tidToStageIndexInfo = new HashMap[Long, (Int, Int)]
    stageIndexToResultHash = new HashMap[(Int, Int), String]
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