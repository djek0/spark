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
import scala.util.Properties.envOrElse

import org.apache.spark.internal.Logging
import org.apache.spark.util.{FileFormatUtils, SafeWriter}

/**
 * Optional per-element trace files (DEBUG_MODE or CHUNK_KEEP_TRACE_FILES): the tap
 * (verification.TraceTap) writes uid|value records; ResultTask and ShuffleMapTask commit them.
 */
private[spark] object Trace extends Logging {
  // track the writers so we can commit their logs all at the end of the task
  // Changed to List to support multiple writers per thread (input + output)
  private val activeWriters = new java.util.concurrent.ConcurrentHashMap[String, java.util.List[SafeWriter]]()

  // Debug mode: set to true for human-readable text files (slower), false for binary (faster)
  private val DEBUG_MODE = envOrElse("DEBUG_MODE", "false").toBoolean

  /**
   * Create a new writer for output logging.
   * The writer must be explicitly closed when done.
   * Uses binary format by default for performance (2-5x faster).
   * Set -Dspark.trace.debugMode=true for human-readable text files.
   * Writes to .tmp file first, renamed to final on commit.
   */
  def createOutputWriter(stageId: Int, partitionId: Int, taskIndex: Int, appName: String): SafeWriter = {
    val finalDir = if (DEBUG_MODE) "logs" else "bins"
    val dir = new File(s"${FileFormatUtils.buildTraceBaseDir()}/$appName/$finalDir")
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
