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
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.internal.Logging
import org.apache.spark.util.SafeWriter

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

  // Thread-local accessors
  private def q = uidQueueTL.get()
  private def ctr = uidCounterTL.get()
  
  private[rdd] def enterIter(): Boolean = {
    val d = depthTL.get().getAndIncrement() // fetch old value, then increment
    d == 0 // true ⇒ this is the outermost iterator
  }
  
  private[rdd] def exitIter(): Unit = {
    depthTL.get().decrementAndGet()
  }

  /**
   * Initialize UID tracking for a new task.
   * Must be called at the beginning of each task.
   */
  def initForTask(): Unit = {
    q.clear()
    ctr.set(0L)
  }

  /**
   * Clean up UID tracking state.
   * Called automatically on task completion.
   */
  def cleanupForTask(): Unit = {
    q.clear()
    ctr.set(0L)
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
  private val activeWriters = new java.util.concurrent.ConcurrentHashMap[String, SafeWriter]()

  /**
   * Create a new writer for input logging.
   * The writer must be explicitly closed when done.
   */
  def createInputWriter(stageId: Int, partitionId: Int, attempt: Int, taskId: Long, appName: String): SafeWriter = {
    val userHome = System.getProperty("user.home")
    val dir = new File(s"$userHome/spark/spark-trace/$appName")
    dir.mkdirs()  // Ensure directory exists
    val file = new File(dir, s"spark_inputs_stage${stageId}_p${partitionId}_att${attempt}_taskId${taskId}.tmp")
    val writer = new SafeWriter(file)
    activeWriters.put(Thread.currentThread().getName, writer)
    writer
  }

  /**
   * Create a new writer for output logging.
   * The writer must be explicitly closed when done.
   */
  def createOutputWriter(stageId: Int, partitionId: Int, attempt: Int, taskId: Long, appName: String): SafeWriter = {
    val userHome = System.getProperty("user.home")
    val dir = new File(s"$userHome/spark/spark-trace/$appName")
    dir.mkdirs()  // Ensure directory exists
    val file = new File(dir, s"spark_finals_stage${stageId}_p${partitionId}_att${attempt}_taskId${taskId}.tmp")
    val writer = new SafeWriter(file)
    activeWriters.put(Thread.currentThread().getName, writer)
    writer
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
    writer.println(s"$uid|$value")
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
    val valueStr = value match {
      case arr: Array[_] => arr.mkString("[", ",", "]")
      case other => other.toString
    }
    writer.println(s"$uid|$valueStr")
  }

  /**
   * Commit the logs by renaming .tmp files to .log.
   * Should be called after the writer is closed.
   */
  def commitLogs(writer: SafeWriter): Unit = {
    if (!writer.tempFile.exists()) {
      logWarning(s"Temporary file ${writer.tempFile} does not exist")
      return
    }
    
    try {
      Files.move(
        writer.tempFile.toPath,
        writer.logFile.toPath,
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.ATOMIC_MOVE
      )
    } catch {
      case _: UnsupportedOperationException =>
        // Fallback for non-atomic filesystems
        Files.move(
          writer.tempFile.toPath,
          writer.logFile.toPath,
          StandardCopyOption.REPLACE_EXISTING
        )
    }
  }

  /**
   * Commit  all the logs
   * usefull to commit all the logs from the current active writers
   * if we are outside of where the writer was created we can still commit the logs
   */
  def commitAllLogs(): Unit = {
    val threadName = Thread.currentThread().getName
    Option(activeWriters.get(threadName)).foreach { writer =>
      try {
        commitLogs(writer)
      } catch {
        case e: Exception =>
          logError(s"Failed to commit log writer for thread $threadName", e)
      } finally {
        activeWriters.remove(threadName)
      }
    }
  }

}
