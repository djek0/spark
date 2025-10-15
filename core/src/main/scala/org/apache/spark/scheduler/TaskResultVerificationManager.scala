package org.apache.spark.scheduler


import scala.collection.mutable.{HashMap, HashSet}

object TaskResultVerificationManager{

  var tidToStageIndexInfo = new HashMap[Long, (Int, Int)]
  var stageIndexToResultHash = new HashMap[(Int, Int), String]

  def addNewRunningTask(tid: Int, indexStage: (Int, Int)): Unit = {
    if(tidToStageIndexInfo.contains(tid)){
      println(s"[VERIFICATION] ${tid} already added to running tasks")
      return
    }
    println(s"[VERIFICATION] added ${tid} to verification manager with stage ${indexStage}")
    tidToStageIndexInfo(tid) = indexStage
  }

  def addNewResultForTid(tid: Long, resultHash: String): Unit = {
    if(tidToStageIndexInfo.contains(tid)){
      println(s"[VERIFICATION] adding result for task ${tid} to verification manager")
      val stageIndex = tidToStageIndexInfo(tid)
      stageIndexToResultHash(stageIndex) = resultHash
      println(s"[VERIFICATION] stored hash ${resultHash} for stage ${stageIndex}")
    } else {
      println(s"[VERIFICATION] ERROR: task ${tid} not found in running tasks!")
    }
  }

  def verifyResult(tid: Long): Unit = {
    println(s"[VERIFICATION] Trying to verify result for task ${tid}")
    if(tidToStageIndexInfo.contains(tid)) {
      val stageIndex = tidToStageIndexInfo(tid)
      if(stageIndexToResultHash.contains(stageIndex)){
        val stageId = stageIndex._1
        val index = stageIndex._2
        println(s"[VERIFICATION] Verifying task ${tid} with stage ${stageId}, index ${index}")
        if(index%2 == 0){
          val partnerIndex = index + 1
          if(stageIndexToResultHash.contains((stageId, partnerIndex))){
            if(stageIndexToResultHash(stageIndex)==stageIndexToResultHash((stageId,partnerIndex))){
              println(s"[VERIFICATION] ✅ CONSENSUS: Valid result for stage ${stageId}, indexes ${index}, ${partnerIndex}")
            }
            else{
              println(s"[VERIFICATION] ❌ BYZANTINE FAULT: BAD result for stage ${stageId}, indexes ${index}, ${partnerIndex}")
            }
          } else {
            println(s"[VERIFICATION] ⏳ WAITING: Partner task ${partnerIndex} not completed yet")
          }
        }
        else{
          val partnerIndex = index - 1
          if(stageIndexToResultHash.contains((stageId, partnerIndex))){
            if(stageIndexToResultHash(stageIndex)==stageIndexToResultHash((stageId,partnerIndex))){
              println(s"[VERIFICATION] ✅ CONSENSUS: Valid result for stage ${stageId}, indexes ${index}, ${partnerIndex}")
            }
            else{
              println(s"[VERIFICATION] ❌ BYZANTINE FAULT: BAD result for stage ${stageId}, indexes ${index}, ${partnerIndex}")
            }
          } else {
            println(s"[VERIFICATION] ⏳ WAITING: Partner task ${partnerIndex} not completed yet")
          }
        }
      } else {
        println(s"[VERIFICATION] ⏳ WAITING: No result hash stored for task ${tid} yet")
      }
    } else {
      println(s"[VERIFICATION] ERROR: Task ${tid} not found in running tasks!")
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