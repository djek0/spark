package org.apache.spark.scheduler

import scala.collection.mutable.HashMap

import org.apache.spark.internal.Logging

object TaskResultVerificationManager extends Logging {

  var tidToStageIndexInfo = new HashMap[Long, (Int, Int)]
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
        val index = stageIndex._2
        logDebug(s"Verifying task $tid (stage $stageId, index $index)")
        if(index%2 == 0){
          val partnerIndex = index + 1
          if(stageIndexToResultHash.contains((stageId, partnerIndex))){
            if(stageIndexToResultHash(stageIndex)==stageIndexToResultHash((stageId,partnerIndex))){
              logInfo(s"[+] CONSENSUS: Valid result for stage $stageId, partition ${index/2} (indexes $index, $partnerIndex)")
            }
            else{
              logError(s"[X] BYZANTINE FAULT DETECTED: Hash mismatch for stage $stageId, partition ${index/2} (indexes $index, $partnerIndex)")
            }
          } else {
            logDebug(s"[*] Waiting for partner task $partnerIndex to complete")
          }
        }
        else{
          val partnerIndex = index - 1
          if(stageIndexToResultHash.contains((stageId, partnerIndex))){
            if(stageIndexToResultHash(stageIndex)==stageIndexToResultHash((stageId,partnerIndex))){
              logInfo(s"[+] CONSENSUS: Valid result for stage $stageId, partition ${index/2} (indexes $partnerIndex, $index)")
            }
            else{
              logError(s"[X] BYZANTINE FAULT DETECTED: Hash mismatch for stage $stageId, partition ${index/2} (indexes $partnerIndex, $index)")
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

}