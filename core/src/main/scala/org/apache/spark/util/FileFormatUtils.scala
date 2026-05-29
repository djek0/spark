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

/**
 * Utility object for file format configuration based on DEBUG_MODE.
 * Centralizes the logic for choosing between binary (.bin) and text (.log) formats.
 */
object FileFormatUtils {
  
  /**
   * Get file format configuration based on debug mode flag.
   * 
   * @param debugMode whether debug mode is enabled
   * @return (isBinary, extension, directory)
   *   - isBinary: true for binary format (.bin), false for text format (.log)
   *   - extension: ".bin" or ".log"
   *   - directory: "bins" or "logs"
   */
  def getFileFormat(debugMode: Boolean): (Boolean, String, String) = {
    val isBinary = !debugMode
    val ext = if (debugMode) ".log" else ".bin"
    val dir = if (debugMode) "logs" else "bins"
    (isBinary, ext, dir)
  }
  
  /**
   * Build finals file path for a given task.
   * 
   * @param appName application name
   * @param stageId stage ID
   * @param taskIndex task index
   * @param partitionId partition ID
   * @param debugMode whether debug mode is enabled
   * @return absolute path to finals file
   */
  def buildFinalsPath(appName: String, stageId: Int, taskIndex: Int, partitionId: Int, debugMode: Boolean): String = {
    val userHome = System.getProperty("user.home")
    val (_, ext, dir) = getFileFormat(debugMode)
    s"$userHome/spark/spark-trace/$appName/$dir/spark_finals_stage${stageId}_idx${taskIndex}_p${partitionId}${ext}"
  }
}
