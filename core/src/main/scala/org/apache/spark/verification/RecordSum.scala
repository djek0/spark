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

import java.util.Locale

import org.apache.spark.{TaskContext, TaskContextImpl}

/**
 * W, a map task's record sum: the wrap-around sum of one 64-bit hash per record its shuffle
 * writer received, plus their count. It goes into the map-stage consensus hash
 * (ResultHash.ofMapOutput), in full-task and chunks mode alike, wherever the chunk digest does
 * not already cover those records (MAP_RECORD_SUM). Only computing every record produces it.
 *
 * Order-free twice over: the sum ignores the order of the records, and collections inside a
 * record are hashed order-free (recordHash), because a record may hold a collection filled in
 * shuffle-read order (groupByKey's values), which honest replicas in a cluster can read in a
 * different order. Used only by the task's own thread.
 */
private[spark] final class RecordSum {
  var sum = 0L
  var count = 0L
  /** The writer read to the end; false only if it stopped early (never, for Spark's writers). */
  var exhausted = false
  /** The top RDD's iterator was asked for twice in one task: the sum covers only the first. */
  var invalid = false

  def flags: Long = (if (exhausted) 1L else 0L) | (if (invalid) 2L else 0L)

  /** [sum, count, flags, MAGIC]: goes right before the executor's hash in the trailer. */
  def toTrailer: Array[Long] = Array(sum, count, flags, RecordSum.MAGIC)

  def view: RecordSumView = RecordSumView(sum, count, flags)
}

/** A record sum as the driver reads it from a trailer, or as a verifier hashes its own. */
private[spark] final case class RecordSumView(sum: Long, count: Long, flags: Long)

private[spark] object RecordSum {
  /** Ends the record-sum block; distinct from ChunkDigest.MAGIC. */
  val MAGIC: Long = 0x5245434F52445355L

  /**
   * MAP_RECORD_SUM (executors; set it the same on all of them):
   *  - covered (default, or true): W wherever the chunk digest does not already cover exactly
   *    what the shuffle writer receives
   *  - always: W in every map task, digest or not
   *  - off (or false): no W; only to measure the system without it (the sizes-only gap)
   */
  private lazy val mode: String =
    scala.util.Properties.envOrElse("MAP_RECORD_SUM", "covered").trim.toLowerCase(Locale.ROOT) match {
      case "off" | "false" => "off"
      case "always" => "always"
      case _ => "covered"
    }
  lazy val enabled: Boolean = mode != "off"
  lazy val always: Boolean = mode == "always"

  /**
   * One record's term. ElementHash.hash64, except that a collection is hashed as a multiset: the
   * sum of its elements' mixed hashes, plus its size. Products are walked here too, so a
   * collection nested in a tuple (a groupByKey record) is reached. Iterable before Product: a
   * List is both.
   */
  def recordHash(v: Any): Long = v match {
    case c: Iterable[_] =>
      var s = 0L; var n = 0L; val i = c.iterator
      while (i.hasNext) { s += ElementHash.mix64(recordHash(i.next()) ^ 0xB1L); n += 1 }
      ElementHash.mix64(s ^ ElementHash.mix64(n ^ 0xB2L))
    case p: Product =>
      val n = p.productArity; var h = 0xB3L ^ n; var i = 0
      while (i < n) { h = ElementHash.mix64(h ^ recordHash(p.productElement(i))); i += 1 }
      h
    case other => ElementHash.hash64(other)
  }

  /** TraceTap.leave, at a map task's top RDD: count and sum what the shuffle writer pulls. */
  def wrap[T](it: Iterator[T], ctx: TaskContextImpl): Iterator[T] = {
    if (!enabled) it
    else if (ctx.recordSum != null) {
      ctx.recordSum.invalid = true   // never expected: the writer asks for the iterator once
      it
    } else {
      val s = new RecordSum
      ctx.recordSum = s
      new RecordSumIterator(it, s)
    }
  }

  /** The executor's trailer block; empty when the task has no record sum. */
  def trailerFor(context: TaskContext): Array[Long] = context match {
    case impl: TaskContextImpl if impl.recordSum != null => impl.recordSum.toTrailer
    case _ => Array.emptyLongArray
  }

  /** The driver: split a trailer (without the executor hash) into the record sum and the rest. */
  def fromTrailer(arr: Array[Long]): (Option[RecordSumView], Array[Long]) = {
    val n = arr.length
    if (n < 4 || arr(n - 1) != MAGIC) (None, arr)
    else (Some(RecordSumView(arr(n - 4), arr(n - 3), arr(n - 2))), arr.take(n - 4))
  }
}

private final class RecordSumIterator[T](it: Iterator[T], s: RecordSum) extends Iterator[T] {
  override def hasNext: Boolean = {
    val h = it.hasNext
    if (!h) s.exhausted = true
    h
  }
  override def next(): T = {
    val r = it.next()
    // Hashed as it passes: the writer may reuse the record's object after this call
    s.sum += RecordSum.recordHash(r)
    s.count += 1
    r
  }
}
