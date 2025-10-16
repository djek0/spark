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

package org.apache.spark.util

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import org.apache.spark.TaskContext

/**
 * A thread-safe writer that ensures resources are properly cleaned up.
 * Implements idempotent close and automatic cleanup on task completion.
 *
 * @param file the file to write to
 * @param autoFlush whether to auto-flush after each write
 * @param bufferSize buffer size in bytes (default: 8KB)
 */
private[spark] class SafeWriter(
    file: File,
    autoFlush: Boolean = false,
    bufferSize: Int = 8192) {
  
  private val writer = new PrintWriter(
    new BufferedWriter(
      new FileWriter(file, true),
      bufferSize
    ),
    autoFlush
  )
  
  private val closed = new java.util.concurrent.atomic.AtomicBoolean(false)
  
  // Register safety net
  Option(TaskContext.get()).foreach { ctx =>
    ctx.addTaskCompletionListener[Unit](_ => safeClose())
  }
  
  /**
   * Thread-safe idempotent close.
   * Safe to call multiple times.
   */
  def safeClose(): Unit = {
    if (closed.compareAndSet(false, true)) {
      writer.close()
    }
  }
  
  /**
   * Write a line to the file.
   * @throws IllegalStateException if the writer is already closed
   */
  def println(s: String): Unit = {
    if (closed.get()) {
      throw new IllegalStateException(s"Writer for $file is already closed")
    }
    writer.println(s)
  }
  
  /**
   * @return the temporary file being written to
   */
  def tempFile: File = file
  
  /**
   * @return the target log file path (after renaming)
   */
  def logFile: File = new File(file.getPath.replace(".tmp", ".log"))
  
  /**
   * @return whether the writer is closed
   */
  def isClosed: Boolean = closed.get()
}
