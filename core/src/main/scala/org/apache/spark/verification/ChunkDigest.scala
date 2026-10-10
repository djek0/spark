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

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.{TaskContext, TaskContextImpl}

/**
 * Chunk verification building blocks.
 *
 * Each replica sums one 64-bit term per element into "boxes", one box per range of 2^shift UIDs.
 * The boxes travel in the task result; on a hash mismatch the driver finds the first differing
 * box and a verifier recomputes only that UID range. Boxes are plain sums (with wrap-around),
 * so merging two neighbouring boxes is an addition and a digest merged early equals one merged
 * late: honest replicas always end with the same layout, and two digests at different
 * resolutions can be lined up exactly.
 */

/** 64-bit fingerprint of one element, built from its structure (never from text). */
object ElementHash {
  /** SplitMix64 finalizer. */
  def mix64(z0: Long): Long = {
    var z = z0 * 0x9E3779B97F4A7C15L
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL
    z ^ (z >>> 31)
  }

  def hash64(v: Any): Long = v match {
    case null => 0x6E756C6CL
    case x: Long => mix64(x)
    case x: Int => mix64(x.toLong ^ 0x100000000L)
    case x: Double => mix64(java.lang.Double.doubleToLongBits(x) ^ 0x200000000L)
    case x: Float => mix64(java.lang.Float.floatToIntBits(x).toLong ^ 0x300000000L)
    case x: Boolean => if (x) 0x7FA1L else 0x7FA0L
    case x: Short => mix64(x.toLong ^ 0x400000000L)
    case x: Byte => mix64(x.toLong ^ 0x500000000L)
    case x: Char => mix64(x.toLong ^ 0x600000000L)
    case s: String => hashString(s)
    case a: Array[Double] =>
      var h = 0xA1L ^ a.length; var i = 0
      while (i < a.length) { h = mix64(h ^ java.lang.Double.doubleToLongBits(a(i))); i += 1 }
      h
    case a: Array[Long] =>
      var h = 0xA2L ^ a.length; var i = 0
      while (i < a.length) { h = mix64(h ^ a(i)); i += 1 }
      h
    case a: Array[Int] =>
      var h = 0xA3L ^ a.length; var i = 0
      while (i < a.length) { h = mix64(h ^ a(i).toLong); i += 1 }
      h
    case a: Array[_] =>
      var h = 0xA5L ^ a.length; var i = 0
      while (i < a.length) { h = mix64(h ^ hash64(a(i))); i += 1 }
      h
    // Before Product: a List is also a Product, and walking it in a loop avoids deep recursion.
    case it: Iterable[_] =>
      var h = 0xA6L; var n = 0L; val i = it.iterator
      while (i.hasNext) { h = mix64(h ^ hash64(i.next())); n += 1 }
      mix64(h ^ n)
    // Tuples, case classes, Option.
    case p: Product =>
      val n = p.productArity; var h = 0xA7L ^ n; var i = 0
      while (i < n) { h = mix64(h ^ hash64(p.productElement(i))); i += 1 }
      h
    // Same fallback as formatValueWithArrays: the element's text.
    case other => mix64(other.toString.hashCode.toLong ^ (0xA8L << 32))
  }

  /** FNV-1a over the chars, then mixed. */
  private def hashString(s: String): Long = {
    var h = 0xCBF29CE484222325L; var i = 0
    while (i < s.length) { h = (h ^ s.charAt(i)) * 0x100000001B3L; i += 1 }
    mix64(h ^ s.length)
  }
}

/**
 * One element's contribution to its box. `ordinal` counts the outputs of one UID (0, 1, ... for
 * flatMap), so reordering the outputs of one input changes the sum.
 */
object ChunkTerm {
  private val K1 = 0x9E3779B97F4A7C15L
  private val K2 = 0xC2B2AE3D27D4EB4FL
  def apply(uid: Long, ordinal: Long, h: Long): Long =
    ElementHash.mix64(h ^ (uid * K1) ^ (ordinal * K2))
}

/** Replica side: the boxes of one task. Used only by the task's own thread. */
final class ChunkDigest(val maxChunks: Int) {
  require(maxChunks >= 2 && Integer.bitCount(maxChunks) == 1,
    "maxChunks must be a power of two >= 2")

  private var shift = 0                 // chunk size = 2^shift UIDs
  private val boxes = new Array[Long](maxChunks)
  private var used = 0                  // highest chunk index + 1
  private var ordinal = 0L
  var lastUid = -1L
  var exhausted = false                 // the consumer read the stream to the end
  var invalid = false                   // something unexpected happened: never use this digest

  def add(uid: Long, elem: Any): Unit = {
    if (invalid) return
    // UIDs at the logging point never decrease; anything else means the numbering is broken.
    if (uid < 0 || uid > (1L << 50) || uid < lastUid) { invalid = true; return }
    try {
      val h = ElementHash.hash64(elem)
      if (uid == lastUid) ordinal += 1 else { ordinal = 0; lastUid = uid }
      while ((uid >>> shift) >= maxChunks) fold()
      val c = (uid >>> shift).toInt
      boxes(c) += ChunkTerm(uid, ordinal, h)
      if (c + 1 > used) used = c + 1
    } catch {
      case NonFatal(_) => invalid = true  // hashing must never fail the task
    }
  }

  /** Halve the resolution: box j becomes the sum of boxes 2j and 2j+1. */
  private def fold(): Unit = {
    val half = maxChunks >>> 1
    var j = 0
    while (j < half) { boxes(j) = boxes(2 * j) + boxes(2 * j + 1); j += 1 }
    java.util.Arrays.fill(boxes, half, maxChunks, 0L)
    shift += 1
    used = (used + 1) >>> 1
  }

  /** Fault injection only: make the box of the last element wrong (a wrong value there). */
  def corruptForTest(): Unit =
    if (!invalid && lastUid >= 0) boxes((lastUid >>> shift).toInt) += 0x5DEECE66DL

  /** [boxes 0 .. used-1, lastUid, header, MAGIC]: appended to metricPeaks before the hash. */
  def toTrailer: Array[Long] = {
    val out = new Array[Long](used + 3)
    System.arraycopy(boxes, 0, out, 0, used)
    out(used) = lastUid
    out(used + 1) = ChunkDigest.header(used, shift, exhausted, invalid)
    out(used + 2) = ChunkDigest.MAGIC
    out
  }
}

/** Driver side: an immutable copy of one replica's digest. */
final case class DigestView(
    shift: Int,
    boxes: Array[Long],
    lastUid: Long,
    exhausted: Boolean,
    invalid: Boolean)

object ChunkDigest {
  /** Negative, so it can never be confused with an executor metric peak. */
  val MAGIC: Long = 0xC4D16E57C4D16E57L
  private val MaxUsed = 1 << 20

  def header(used: Int, shift: Int, exhausted: Boolean, invalid: Boolean): Long =
    (if (invalid) 1L << 41 else 0L) | (if (exhausted) 1L << 40 else 0L) |
      (shift.toLong << 32) | used.toLong

  /**
   * Input: metricPeaks with the result hash already removed. Output: the digest if a valid block
   * ends the array, and the peaks without it. No block, or a malformed one: (None, input).
   */
  def fromTrailer(arr: Array[Long]): (Option[DigestView], Array[Long]) = {
    val n = arr.length
    if (n < 3 || arr(n - 1) != MAGIC) return (None, arr)
    val h = arr(n - 2)
    val used = h & 0xFFFFFFFFL
    val shift = ((h >>> 32) & 0xFF).toInt
    if (h < 0 || used > MaxUsed || used > n - 3 || shift > 62) return (None, arr)
    val start = n - 3 - used.toInt
    val view = DigestView(shift, java.util.Arrays.copyOfRange(arr, start, n - 3), arr(n - 3),
      exhausted = (h & (1L << 40)) != 0, invalid = (h & (1L << 41)) != 0)
    (Some(view), java.util.Arrays.copyOfRange(arr, 0, start))
  }

  private def boxAt(b: Array[Long], i: Int): Long = if (i < b.length) b(i) else 0L

  /** Merge boxes pairwise from resolution `from` down to `to` (exact: boxes are sums). */
  private def foldTo(boxes: Array[Long], from: Int, to: Int): Array[Long] = {
    var cur = boxes; var s = from
    while (s < to) {
      val next = new Array[Long]((cur.length + 1) / 2); var i = 0
      while (i < cur.length) { next(i / 2) += cur(i); i += 1 }
      cur = next; s += 1
    }
    cur
  }

  /**
   * First differing chunk as (lo, hi, boxA, boxB) over UIDs [lo, hi). None means the difference
   * cannot be localized and the caller must use full-task verification: a digest is invalid,
   * the digests are equal (the fault is not in the traced values), or the first difference lies
   * where a stream was only partly read (take/first).
   */
  def firstDifference(a: DigestView, b: DigestView): Option[(Long, Long, Long, Long)] = {
    if (a.invalid || b.invalid) return None
    val s = math.max(a.shift, b.shift)
    val x = foldTo(a.boxes, a.shift, s); val y = foldTo(b.boxes, b.shift, s)
    val n = math.max(x.length, y.length)
    var c = 0
    while (c < n && boxAt(x, c) == boxAt(y, c)) c += 1
    if (c == n) return None
    def limit(d: DigestView): Long =
      if (d.exhausted) Long.MaxValue else math.max(d.lastUid, 0L) >>> s
    if (c >= math.min(limit(a), limit(b))) return None
    val lo = c.toLong << s
    Some((lo, lo + (1L << s), boxAt(x, c), boxAt(y, c)))
  }
}

/** Verifier side: one box for UIDs [lo, hi), with the same term and ordinal rule. */
final class RangeBox(val lo: Long, val hi: Long) {
  var sum = 0L
  var count = 0L
  private var lastUid = -1L
  private var ordinal = 0L

  def add(uid: Long, elem: Any): Unit = {
    if (uid < lo || uid >= hi) return   // cannot happen: the root lets only [lo, hi) through
    val h = ElementHash.hash64(elem)
    if (uid == lastUid) ordinal += 1 else { ordinal = 0; lastUid = uid }
    sum += ChunkTerm(uid, ordinal, h)
    count += 1
  }
}

/** Settings, read once per JVM from the environment. In local mode driver and executors share them. */
object ChunkConfig {
  private def env(k: String, d: String): String = scala.util.Properties.envOrElse(k, d).trim
  private def bool(k: String, d: Boolean): Boolean = Try(env(k, d.toString).toBoolean).getOrElse(d)

  lazy val merkleOn: Boolean = bool("MERKLE_VERIFICATION", true)
  /** Chunks are the only localization since the Merkle trees were removed. */
  lazy val useChunks: Boolean = merkleOn
  lazy val maxChunks: Int = {
    val v = Try(env("CHUNK_MAX", "1024").toInt).getOrElse(1024)
    Integer.highestOneBit(math.min(math.max(v, 2), 1 << 16))
  }
  /** In chunks mode no code reads Case A trace files, so they are off unless asked for. */
  lazy val keepTraceFiles: Boolean = bool("CHUNK_KEEP_TRACE_FILES", false)
  lazy val writeTraceFiles: Boolean = !useChunks || keepTraceFiles || bool("DEBUG_MODE", false)

  /** Called by the logging point. Returns the task's new digest, or null when digests are off. */
  def newDigest(context: TaskContext): ChunkDigest = context match {
    case impl: TaskContextImpl if merkleOn =>
      if (impl.chunkDigest != null) {
        impl.chunkDigest.invalid = true   // a second logging point in one task: never trust it
        null
      } else {
        val d = new ChunkDigest(maxChunks); impl.chunkDigest = d; d
      }
    case _ => null                        // Merkle off, or a barrier task
  }

  /** The block to put before the result hash; empty when the task has no digest. */
  def trailerFor(context: TaskContext, corruptForTest: Boolean): Array[Long] = context match {
    case impl: TaskContextImpl if impl.chunkDigest != null =>
      if (corruptForTest) impl.chunkDigest.corruptForTest()
      impl.chunkDigest.toTrailer
    case _ => Array.emptyLongArray
  }
}
