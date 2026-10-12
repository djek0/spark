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

package org.apache.spark.verification

import java.nio.ByteBuffer
import java.security.MessageDigest

import org.apache.spark.SparkEnv

/**
 * The consensus hash of a task result, computed by whoever checks the result from the result
 * itself, never taken from the replica's report: the driver for replicas (TaskResultGetter), the
 * verifier for its recompute. SHA-256: a different result with the same hash needs a second
 * preimage. Hex, as stored by TaskResultVerificationManager.
 */
private[spark] object ResultHash {
  private def hex(bytes: Array[Byte]): String = {
    val sb = new StringBuilder(bytes.length * 2)
    bytes.foreach { b =>
      sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16))
    }
    sb.toString
  }

  /** Result bytes from the buffer's position to its limit; the buffer itself does not move. */
  def ofBytes(bytes: ByteBuffer): String = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(bytes.duplicate())
    hex(md.digest())
  }

  /** A recomputed result, serialized the way executors serialize results (spark.serializer). */
  def ofValue(value: Any): String =
    ofBytes(SparkEnv.get.serializer.newInstance().serialize(value))

  /**
   * A map task: one size per reduce partition (lossy, as MapStatus reports them); when its stage
   * is traced, its chunk digest (A4b); and its record sum (W), which covers every record the
   * shuffle writer received, in every mode. Each part is tagged, and variable-length parts carry
   * their length, so two different inputs never hash the same bytes.
   */
  def ofMapOutput(sizes: Seq[Long], digest: Option[DigestView], sum: Option[RecordSumView]): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val b = ByteBuffer.allocate(8)
    def put(x: Long): Unit = { b.clear(); b.putLong(x); md.update(b.array()) }
    put(sizes.length)
    sizes.foreach(put)
    digest match {
      case Some(d) =>
        put(1L); put(d.shift); put(if (d.exhausted) 1L else 0L); put(if (d.invalid) 1L else 0L)
        put(d.lastUid); put(d.boxes.length); d.boxes.foreach(put)
      case None => put(0L)
    }
    sum match {
      case Some(s) => put(2L); put(s.sum); put(s.count); put(s.flags)
      case None => put(0L)
    }
    hex(md.digest())
  }
}
