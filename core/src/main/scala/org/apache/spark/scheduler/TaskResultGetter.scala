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

import java.nio.ByteBuffer
import java.util.concurrent.{ExecutorService, RejectedExecutionException}

import scala.language.existentials
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{LongAccumulator, ThreadUtils, Utils}
/**
 * Runs a thread pool that deserializes and remotely fetches (if necessary) task results.
 */
private[spark] class TaskResultGetter(sparkEnv: SparkEnv, scheduler: TaskSchedulerImpl)
  extends Logging {

  private val THREADS = sparkEnv.conf.getInt("spark.resultGetter.threads", 4)

  // Exposed for testing.
  protected val getTaskResultExecutor: ExecutorService =
    ThreadUtils.newDaemonFixedThreadPool(THREADS, "task-result-getter")

  // Exposed for testing.
  protected val serializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.closureSerializer.newInstance()
    }
  }

  protected val taskResultSerializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.serializer.newInstance()
    }
  }

  def enqueueSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      serializedData: ByteBuffer): Unit = {
    getTaskResultExecutor.execute(new Runnable {
      override def run(): Unit = Utils.logUncaughtExceptions {
        try {
          val (result, size) = serializer.get().deserialize[TaskResult[_]](serializedData) match {
            case directResult: DirectTaskResult[_] =>
              if (!taskSetManager.canFetchMoreResults(serializedData.limit())) {
                // kill the task so that it will not become zombie task
                scheduler.handleFailedTask(taskSetManager, tid, TaskState.KILLED, TaskKilled(
                  "Tasks result size has exceeded maxResultSize"))
                return
              }
              // A ResultTask's consensus hash is taken here, from the bytes the driver received,
              // before value() deserializes them (deserializing moves the buffer's position)
              val task = taskSetManager.tasks(taskSetManager.taskInfos(tid).index)
              val bytesHash = task match {
                case _: ResultTask[_, _] =>
                  org.apache.spark.verification.ResultHash.ofBytes(directResult.valueBytes)
                case _ => null
              }
              // deserialize "value" without holding any lock so that it won't block other threads.
              // We should call it here, so that when it's called again in
              // "TaskSetManager.handleSuccessfulTask", it does not need to deserialize the value.
              directResult.value(taskResultSerializer.get())
              val executorHash: Long = directResult.metricPeaks(directResult.metricPeaks.length-1)
              directResult.metricPeaks = directResult.metricPeaks.take(directResult.metricPeaks.length-1)
              // Record-sum block (W; map tasks) sits right before the hash, the chunk block before it
              val (recordSum, peaksWithoutSum) =
                org.apache.spark.verification.RecordSum.fromTrailer(directResult.metricPeaks)
              directResult.metricPeaks = peaksWithoutSum
              val (chunkDigest, peaksWithoutDigest) =
                org.apache.spark.verification.ChunkDigest.fromTrailer(directResult.metricPeaks)
              directResult.metricPeaks = peaksWithoutDigest
              // Executors send exactly numMetrics peaks: anything else is a trailer the driver misparsed
              checkTrailer(tid, directResult.metricPeaks)
//              logInfo("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
//              logInfo(s"[EXTRA LOG][TASK RESULT GETTER] deserialized result: for task ${tid}")

//              println(hashValueCandidate)
//              println(s"[EXTRA LOG] hash ${hashValueCandidate.hashCode().toString} FOR TID $tid, original byte array size $resultSize")

              recordReplicaResult(tid, task, bytesHash, directResult.value(), chunkDigest, recordSum,
                executorHash)
              (directResult, serializedData.limit())
            case IndirectTaskResult(blockId, size) =>
              if (!taskSetManager.canFetchMoreResults(size)) {
                // dropped by executor if size is larger than maxResultSize
                sparkEnv.blockManager.master.removeBlock(blockId)
                // kill the task so that it will not become zombie task
                scheduler.handleFailedTask(taskSetManager, tid, TaskState.KILLED, TaskKilled(
                  "Tasks result size has exceeded maxResultSize"))
                return
              }
              logDebug(s"Fetching indirect task result for ${taskSetManager.taskName(tid)}")
              scheduler.handleTaskGettingResult(taskSetManager, tid)
              val serializedTaskResult = sparkEnv.blockManager.getRemoteBytes(blockId)
              if (serializedTaskResult.isEmpty) {
                /* We won't be able to get the task result if the machine that ran the task failed
                 * between when the task ended and when we tried to fetch the result, or if the
                 * block manager had to flush the result. */
                scheduler.handleFailedTask(
                  taskSetManager, tid, TaskState.FINISHED, TaskResultLost)
                return
              }
              val deserializedResult = serializer.get().deserialize[DirectTaskResult[_]](
                serializedTaskResult.get.toByteBuffer)
              // The consensus hash of a ResultTask: from the received bytes, before value()
              val task = taskSetManager.tasks(taskSetManager.taskInfos(tid).index)
              val bytesHash = task match {
                case _: ResultTask[_, _] =>
                  org.apache.spark.verification.ResultHash.ofBytes(deserializedResult.valueBytes)
                case _ => null
              }
              // force deserialization of referenced value
              deserializedResult.value(taskResultSerializer.get())
              sparkEnv.blockManager.master.removeBlock(blockId)
//              logInfo("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
//              logInfo(s"[EXTRA LOG][TASK RESULT GETTER 2] deserialized result: for task ${tid}")
//              println(Arrays.toString(deserializedResult.valueBytes.array()).hashCode().toString)

              // The trailer of an IndirectTaskResult, popped the same way (large results)
              val executorHash: Long = deserializedResult.metricPeaks(deserializedResult.metricPeaks.length-1)
              deserializedResult.metricPeaks = deserializedResult.metricPeaks.take(deserializedResult.metricPeaks.length-1)
              val (recordSum, peaksWithoutSum) =
                org.apache.spark.verification.RecordSum.fromTrailer(deserializedResult.metricPeaks)
              deserializedResult.metricPeaks = peaksWithoutSum
              val (chunkDigest, peaksWithoutDigest) =
                org.apache.spark.verification.ChunkDigest.fromTrailer(deserializedResult.metricPeaks)
              deserializedResult.metricPeaks = peaksWithoutDigest
              checkTrailer(tid, deserializedResult.metricPeaks)
              recordReplicaResult(tid, task, bytesHash, deserializedResult.value(), chunkDigest,
                recordSum, executorHash)

              (deserializedResult, size)
          }

          // Set the task result size in the accumulator updates received from the executors.
          // We need to do this here on the driver because if we did this on the executors then
          // we would have to serialize the result again after updating the size.
          result.accumUpdates = result.accumUpdates.map { a =>
            if (a.name == Some(InternalAccumulator.RESULT_SIZE)) {
              val acc = a.asInstanceOf[LongAccumulator]
              assert(acc.sum == 0L, "task result size should not have been set on the executors")
              acc.setValue(size.toLong)
              acc
            } else {
              a
            }
          }

          scheduler.handleSuccessfulTask(taskSetManager, tid, result)
        } catch {
          case cnf: ClassNotFoundException =>
            val loader = Thread.currentThread.getContextClassLoader
            taskSetManager.abort("ClassNotFound with classloader: " + loader)
          // Matching NonFatal so we don't catch the ControlThrowable from the "return" above.
          case NonFatal(ex) =>
            logError("Exception while getting task result", ex)
            taskSetManager.abort("Exception while getting task result: %s".format(ex))
        }
      }
    })
  }

  private def checkTrailer(tid: Long, peaks: Array[Long]): Unit = {
    val expected = org.apache.spark.metrics.ExecutorMetricType.numMetrics
    if (peaks.length != expected) {
      logWarning(s"[TRAILER] task $tid: ${peaks.length} metric peaks after parsing, expected $expected")
    }
  }

  /**
   * A replica's result: store its chunk digest, then its consensus hash, computed here on the
   * driver (digest first, so whoever sees the hash also sees the digest). Other tasks: nothing.
   */
  private def recordReplicaResult(tid: Long, task: Task[_], bytesHash: String, value: Any,
      digest: Option[org.apache.spark.verification.DigestView],
      recordSum: Option[org.apache.spark.verification.RecordSumView], executorHash: Long): Unit = {
    val hash = task match {
      case _: ResultTask[_, _] => bytesHash
      case smt: ShuffleMapTask if smt.numReducePartitions > 0 =>
        org.apache.spark.verification.ResultHash.ofMapOutput(
          smt.blockSizesForHash(value.asInstanceOf[MapStatus]), digest, recordSum)
      case _: ShuffleMapTask =>
        // Never expected: DAGScheduler sets numReducePartitions when it creates map tasks
        logError(s"[HASH] map task $tid has no reduce partition count; using its reported hash")
        s"executor:$executorHash"
      case _ =>
        logDebug(s"[VERIFICATION] No result hash for ${task.getClass.getSimpleName} (taskId=$tid)")
        return
    }
    digest.foreach(TaskResultVerificationManager.storeChunkDigest(tid, _))
    TaskResultVerificationManager.addNewResultForTid(tid, hash)
  }

  def enqueueFailedTask(taskSetManager: TaskSetManager, tid: Long, taskState: TaskState,
    serializedData: ByteBuffer): Unit = {
    var reason : TaskFailedReason = UnknownReason
    try {
      getTaskResultExecutor.execute(() => Utils.logUncaughtExceptions {
        val loader = Utils.getContextOrSparkClassLoader
        try {
          if (serializedData != null && serializedData.limit() > 0) {
            reason = serializer.get().deserialize[TaskFailedReason](
              serializedData, loader)
          }
        } catch {
          case _: ClassNotFoundException =>
            // Log an error but keep going here -- the task failed, so not catastrophic
            // if we can't deserialize the reason.
            logError(
              "Could not deserialize TaskEndReason: ClassNotFound with classloader " + loader)
          case _: Exception => // No-op
        } finally {
          // If there's an error while deserializing the TaskEndReason, this Runnable
          // will die. Still tell the scheduler about the task failure, to avoid a hang
          // where the scheduler thinks the task is still running.
          scheduler.handleFailedTask(taskSetManager, tid, taskState, reason)
        }
      })
    } catch {
      case e: RejectedExecutionException if sparkEnv.isStopped =>
        // ignore it
    }
  }

  // This method calls `TaskSchedulerImpl.handlePartitionCompleted` asynchronously. We do not want
  // DAGScheduler to call `TaskSchedulerImpl.handlePartitionCompleted` directly, as it's
  // synchronized and may hurt the throughput of the scheduler.
  def enqueuePartitionCompletionNotification(stageId: Int, partitionId: Int): Unit = {
    getTaskResultExecutor.execute(() => Utils.logUncaughtExceptions {
      scheduler.handlePartitionCompleted(stageId, partitionId)
    })
  }

  def stop(): Unit = {
    getTaskResultExecutor.shutdownNow()
  }
}
