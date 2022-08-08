package org.apache.spark.scheduler


import scala.collection.mutable.{HashMap, HashSet}

object TaskResultVerificationManager{

  var tidToStageIndexInfo = new HashMap[Long, (Int, Int)]
  var stageIndexToResultHash = new HashMap[(Int, Int), String]

  def addNewRunningTask(tid: Int, indexStage: (Int, Int)): Unit = {
    if(tidToStageIndexInfo.contains(tid)){
      //      println(s"${tid} already added to running tasks")
      return
    }
    //    println(s"added ${tid} to verification manager")
    tidToStageIndexInfo(tid) = indexStage
  }

  def addNewResultForTid(tid: Long, resultHash: String): Unit = {
    if(tidToStageIndexInfo.contains(tid)){
      //      println(s"adding task ${tid} to verification manager")
      val stageIndex = tidToStageIndexInfo(tid)
      stageIndexToResultHash(stageIndex) = resultHash
    }
  }

  def verifyResult(tid: Long): Unit = {
    //    println(s"Trying to verify result ${tid}")
    if(tidToStageIndexInfo.contains(tid)) {
      val stageIndex = tidToStageIndexInfo(tid)
      if(stageIndexToResultHash.contains(stageIndex)){
        val stageId = stageIndex._1
        val index = stageIndex._2
        if(index%2 == 0){
          if(stageIndexToResultHash.contains((stageId, index+1))){
            if(stageIndexToResultHash(stageIndex)==stageIndexToResultHash((stageId,index+1))){
              //              println(s"Valid result for task refering to stage ${stageId}, indexes ${index}, ${index+1}")
            }
            else{
              //              println(s"BAD result for task refering to stage ${stageId}, indexes ${index}, ${index+1}")
            }
          }
        }
        else{
          if(stageIndexToResultHash.contains((stageId, index-1))){
            if(stageIndexToResultHash(stageIndex)==stageIndexToResultHash((stageId,index-1))){
              //              println(s"Valid result for task refering to stage ${stageId}, indexes ${index}, ${index-1}")
            }
            else{
              //              println(s"BAD result for task refering to stage ${stageId}, indexes ${index}, ${index-1}")
            }
          }
        }
      }
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