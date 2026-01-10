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

import java.io._

import org.apache.spark.TaskContext

/**
 * A thread-safe writer that ensures resources are properly cleaned up.
 * Implements idempotent close and automatic cleanup on task completion.
 * Supports both text and binary modes.
 * Writes to .tmp files that are renamed on commit or deleted on failure.
 *
 * @param file the file to write to (should be .tmp)
 * @param binary if true, uses DataOutputStream for binary; if false, uses PrintWriter for text
 */
private[spark] class SafeWriter(
    file: File,
    binary: Boolean = false) {
  
  // Buffer sizes optimized for different I/O modes
  private val BINARY_BUFFER_SIZE = 65536  // 64KB for fast binary I/O
  private val TEXT_BUFFER_SIZE = 8192     // 8KB for text mode
  
  private val bufferSize = if (binary) BINARY_BUFFER_SIZE else TEXT_BUFFER_SIZE
  
  private val outputStream: OutputStream = new BufferedOutputStream(
    new FileOutputStream(file, true),
    bufferSize
  )
  
  private val writer: Option[PrintWriter] = if (!binary) {
    Some(new PrintWriter(outputStream, false))  // autoFlush always false
  } else None
  
  private val dataOut: Option[DataOutputStream] = if (binary) {
    Some(new DataOutputStream(outputStream))
  } else None
  
  private val closed = new java.util.concurrent.atomic.AtomicBoolean(false)
  
  // Register safety net
  Option(TaskContext.get()).foreach { ctx =>
    ctx.addTaskCompletionListener[Unit](_ => safeClose())
  }
  
  /**
   * Thread-safe idempotent close.
   * Safe to call multiple times.
   * If close fails and file is .tmp, deletes it to prevent corrupt files.
   */
  def safeClose(): Unit = {
    if (closed.compareAndSet(false, true)) {
      var closeFailed = false
      try {
        if (binary) {
          dataOut.foreach(_.flush())
          dataOut.foreach(_.close())
        } else {
          writer.foreach(_.flush())
          writer.foreach(_.close())
        }
      } catch {
        case e: Exception =>
          closeFailed = true
          throw e
      } finally {
        try {
          outputStream.close()
        } catch {
          case e: Exception =>
            closeFailed = true
            throw e
        } finally {
          // If close failed and file is .tmp, delete it
          if (closeFailed && file.exists() && file.getName.endsWith(".tmp")) {
            try {
              file.delete()
            } catch {
              case _: Exception => () // Best effort cleanup
            }
          }
        }
      }
    }
  }

  /**
   * Write a line to the file (text mode only).
   * @throws IllegalStateException if the writer is already closed
   */
  def println(s: String): Unit = {
    if (closed.get()) {
      throw new IllegalStateException(s"Writer for $file is already closed")
    }
    writer.foreach(_.println(s))
  }
  
  /**
   * Write a UID and value in the appropriate format.
   * Binary mode: writes Long + UTF-8 string bytes
   * Text mode: writes "UID|VALUE\n"
   */
  def writeEntry(uid: Long, value: Any): Unit = {
    if (closed.get()) {
      throw new IllegalStateException(s"Writer for $file is already closed")
    }
    
    if (binary) {
      dataOut.foreach { out =>
        out.writeLong(uid)
        val str = value match {
          case arr: Array[_] => arr.mkString("[", ",", "]")
          case other => other.toString
        }
        val bytes = str.getBytes("UTF-8")
        out.writeInt(bytes.length)
        out.write(bytes)
      }
    } else {
      val valueStr = value match {
        case arr: Array[_] => arr.mkString("[", ",", "]")
        case other => other.toString
      }
      writer.foreach(_.println(s"$uid|$valueStr"))
    }
  }
  
  /**
   * @return the file being written to
   */
  def targetFile: File = file
}
