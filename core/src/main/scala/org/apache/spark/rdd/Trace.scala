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

package org.apache.spark.rdd

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicLong
import scala.util.Properties.envOrElse

import org.apache.spark.internal.Logging
import org.apache.spark.util.SafeWriter

/**
 * Sentinel values for special stage conditions.
 */
object UnsafeStageMarkers {
  // Marker for stages with no safe transformations (e.g., only ShuffledRDD)
  val UNSAFE_STAGE_UID: Long = -99999L
  val UNSAFE_STAGE_HASH: Int = Int.MinValue  // -2147483648
  
  // Marker for truly empty partitions
  val EMPTY_PARTITION_UID: Long = -1L
  val EMPTY_PARTITION_HASH: Int = 0
}

/**
 * Companion object for UID tracking and logging.
 */
private[spark] object Trace extends Logging {
  // Thread-local state for UID management
  private val uidCounterTL = new ThreadLocal[AtomicLong] {
    override def initialValue() = new AtomicLong(0L)
  }
  private val uidQueueTL = new ThreadLocal[java.util.ArrayDeque[Long]] {
    override def initialValue() = new java.util.ArrayDeque[Long]()
  }
  
  // Iterator depth tracking for detecting outermost iterator
  private val depthTL = new ThreadLocal[AtomicLong] {
    override def initialValue(): AtomicLong = new AtomicLong(0L)
  }

  // Backup iterator tracking for fallback logging
  // Stores the last safe iterator encountered during recursive evaluation
  private val backupIteratorTL = new ThreadLocal[Option[Iterator[Any]]] {
    override def initialValue(): Option[Iterator[Any]] = None
  }

  // Backup queue - snapshot of UID queue when backup iterator is set
  // This preserves the queue state at the last safe transformation point
  private val backupQueueTL = new ThreadLocal[ArrayDeque[Long]] {
    override def initialValue(): ArrayDeque[Long] = new ArrayDeque[Long]()
  }

  // Correlation broken flag - once set, UID correlation is permanently lost
  // for this iterator traversal and cannot be restored by downstream transformations
  private val correlationBrokenTL = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }

  // Thread-local accessors
  private def q = uidQueueTL.get()
  private def ctr = uidCounterTL.get()
  
  private[rdd] def enterIter(): Boolean = {
    val d = depthTL.get().getAndIncrement() // fetch old value, then increment
    if (d == 0) {
      // Starting new outermost traversal - reset correlation state
      correlationBrokenTL.set(false)
    }
    d == 0 // true ⇒ this is the outermost iterator
  }
  
  private[rdd] def exitIter(): Unit = {
    depthTL.get().decrementAndGet()
  }

  /**
   * Update the backup iterator if the current iterator is safe.
   * Called from RDD.iterator() when encountering a safe transformation.
   * Only updates if UID correlation hasn't been broken yet.
   * Also snapshots the current UID queue state for restoration during logging.
   */
  private[rdd] def updateBackupIterator[T](iterator: Iterator[T]): Unit = {
    if (!isCorrelationBroken) {
      backupIteratorTL.set(Some(iterator.asInstanceOf[Iterator[Any]]))
      // Snapshot the current queue state
      val backupQueue = new ArrayDeque[Long](q)
      backupQueueTL.set(backupQueue)
    }
  }

  /**
   * Get the backup iterator for fallback logging.
   * Returns None if no safe iterator has been encountered.
   */
  private[rdd] def getBackupIterator[T](): Option[Iterator[T]] = {
    backupIteratorTL.get().asInstanceOf[Option[Iterator[T]]]
  }

  /**
   * Clear the backup iterator reference.
   * Called at task initialization.
   */
  private[rdd] def clearBackupIterator(): Unit = {
    backupIteratorTL.set(None)
  }

  /**
   * Mark UID correlation as permanently broken for this iterator traversal.
   * Called when encountering a collapser transformation that destroys UID correspondence.
   * Once set, downstream transformations cannot restore UID correlation.
   */
  private[rdd] def markCorrelationBroken(): Unit = {
    correlationBrokenTL.set(true)
  }

  /**
   * Check if UID correlation has been broken in this iterator traversal.
   */
  private[rdd] def isCorrelationBroken: Boolean = {
    correlationBrokenTL.get()
  }

  /**
   * Restore the UID queue to the backup state.
   * Called before logging the backup iterator to ensure correct UID correlation.
   */
  private[rdd] def restoreBackupQueue(): Unit = {
    q.clear()
    backupQueueTL.get().forEach(q.add(_))
  }

  /**
   * Initialize UID tracking for a new task.
   * Must be called at the beginning of each task.
   */
  def initForTask(): Unit = {
    q.clear()
    ctr.set(0L)
    clearBackupIterator()
    backupQueueTL.set(new ArrayDeque[Long]())
    correlationBrokenTL.set(false)
  }

  /**
   * Clean up UID tracking state.
   * Called automatically on task completion.
   */
  def cleanupForTask(): Unit = {
    q.clear()
    ctr.set(0L)
    clearBackupIterator()
    correlationBrokenTL.set(false)
  }

  // UID management
  private[rdd] def enqueueUid(uid: Long): Unit = q.addLast(uid)
  
  private[rdd] def dequeueUid(): Long = {
    if (q.isEmpty) {
      throw new IllegalStateException("UID queue underflow - check your filter operations")
    }
    q.removeFirst()
  }
  
  private[rdd] def generateUid(): Long = ctr.getAndIncrement()

  // track the writers so we can commit their logs all at the end of the task
  // Changed to List to support multiple writers per thread (input + output)
  private val activeWriters = new java.util.concurrent.ConcurrentHashMap[String, java.util.List[SafeWriter]]()
  
  // Debug mode: set to true for human-readable text files (slower), false for binary (faster)
  private val DEBUG_MODE = envOrElse("DEBUG_MODE", "false").toBoolean

  /**
   * Create a new writer for input logging.
   * The writer must be explicitly closed when done.
   * Uses binary format by default for performance (2-5x faster).
   * Set -Dspark.trace.debugMode=true for human-readable text files.
   * Writes to .tmp file first, renamed to final on commit.
   */
  def createInputWriter(stageId: Int, partitionId: Int, taskIndex: Int, appName: String): SafeWriter = {
    val userHome = System.getProperty("user.home")
    val finalDir = if (DEBUG_MODE) "logs" else "bins"
    val dir = new File(s"$userHome/spark/spark-trace/$appName/$finalDir")
    dir.mkdirs()  // Ensure directory exists
    // Always write to .tmp first for atomic commit
    val file = new File(dir, s"spark_inputs_stage${stageId}_idx${taskIndex}_p${partitionId}.tmp")
    val writer = new SafeWriter(file, binary = !DEBUG_MODE)
    
    // Add writer to the list for this thread
    val threadName = Thread.currentThread().getName
    activeWriters.compute(threadName, (_, existing) => {
      val list = if (existing == null) new java.util.ArrayList[SafeWriter]() else existing
      list.add(writer)
      list
    })
    
    writer
  }

  /**
   * Create a new writer for output logging.
   * The writer must be explicitly closed when done.
   * Uses binary format by default for performance (2-5x faster).
   * Set -Dspark.trace.debugMode=true for human-readable text files.
   * Writes to .tmp file first, renamed to final on commit.
   */
  def createOutputWriter(stageId: Int, partitionId: Int, taskIndex: Int, appName: String): SafeWriter = {
    val userHome = System.getProperty("user.home")
    val finalDir = if (DEBUG_MODE) "logs" else "bins"
    val dir = new File(s"$userHome/spark/spark-trace/$appName/$finalDir")
    dir.mkdirs()  // Ensure directory exists
    // Always write to .tmp first for atomic commit
    val file = new File(dir, s"spark_finals_stage${stageId}_idx${taskIndex}_p${partitionId}.tmp")
    val writer = new SafeWriter(file, binary = !DEBUG_MODE)
    
    // Add writer to the list for this thread
    val threadName = Thread.currentThread().getName
    activeWriters.compute(threadName, (_, existing) => {
      val list = if (existing == null) new java.util.ArrayList[SafeWriter]() else existing
      list.add(writer)
      list
    })
    
    writer
  }

  /**
   * Create an empty finals file when no safe iterator exists for UID tracking.
   * This ensures verifier compatibility by creating the expected file structure.
   */
  def createEmptyFinalsFile(stageId: Int, partitionId: Int, taskIndex: Int, appName: String): Unit = {
    val writer = createOutputWriter(stageId, partitionId, taskIndex, appName)
    try {
      writer.safeClose()  // Close immediately to create empty file
      logInfo(s"[BACKUP ITERATOR] Created empty finals file for stage $stageId, partition $partitionId (no safe iterator)")
    } catch {
      case e: Exception =>
        logWarning(s"[BACKUP ITERATOR] Failed to create empty finals file: ${e.getMessage}")
    }
  }

  // Logging operations
  def logInput(
      writer: SafeWriter,
      stageId: Int,
      partitionId: Int,
      taskId: Long,
      attempt: Int,
      value: Any
  ): Unit = {
    val uid = generateUid()
    enqueueUid(uid)
    writer.writeEntry(uid, value)
  }

  def logOutput(
      writer: SafeWriter,
      stageId: Int,
      partitionId: Int,
      taskId: Long,
      attempt: Int,
      value: Any
  ): Unit = {
    val uid = dequeueUid()
    writer.writeEntry(uid, value)
  }

  /**
   * Commit the logs by atomically renaming .tmp to final file.
   * This ensures files only exist if task completed successfully.
   */
  def commitLogs(writer: SafeWriter): Unit = {
    val tmpFile = writer.targetFile
    if (!tmpFile.exists()) {
      logWarning(s"Temporary file ${tmpFile} does not exist, skipping commit")
      return
    }
    
    // Determine final extension based on mode
    val ext = if (DEBUG_MODE) ".log" else ".bin"
    val finalFile = new File(tmpFile.getPath.replace(".tmp", ext))
    
    try {
      // Atomic rename: .tmp → .bin/.log
      Files.move(
        tmpFile.toPath,
        finalFile.toPath,
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.ATOMIC_MOVE
      )
      logDebug(s"Committed ${tmpFile.getName} → ${finalFile.getName}")
    } catch {
      case _: UnsupportedOperationException =>
        // Fallback for filesystems that don't support atomic moves
        Files.move(
          tmpFile.toPath,
          finalFile.toPath,
          StandardCopyOption.REPLACE_EXISTING
        )
        logDebug(s"Committed ${tmpFile.getName} → ${finalFile.getName} (non-atomic)")
    }
  }

  /**
   * Commit all the logs
   * Useful to commit all the logs from the current active writers
   * if we are outside of where the writer was created we can still commit the logs
   */
  def commitAllLogs(): Unit = {
    val threadName = Thread.currentThread().getName
    Option(activeWriters.get(threadName)).foreach { writerList =>
      try {
        // Commit all writers for this thread
        val it = writerList.iterator()
        while (it.hasNext) {
          val writer = it.next()
          try {
            commitLogs(writer)
          } catch {
            case e: Exception =>
              logError(s"Failed to commit log writer for thread $threadName", e)
          }
        }
      } finally {
        activeWriters.remove(threadName)
      }
    }
  }

}
