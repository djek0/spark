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

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.{InterruptibleIterator, OneToOneDependency, Partition, ShuffleDependency,
  SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{MapPartitionsRDD, ParallelCollectionPartition, RDD, Trace}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{SafeWriter, Utils}

/**
 * Where a traced task numbers and fingerprints its elements (chunk verification).
 *
 * Computed once per task, at its first RDD.iterator call (always the stage's top RDD), from the
 * lineage that ships with the task, so replicas and verifiers compute the same plan. The root
 * counts its elements (TaskContextImpl.rootPos); the tap, the highest RDD of the run of pointwise
 * operations right above the root, reads its element's UID as rootPos - 1, because each pointwise
 * operation pulls one input per step. Anything above the tap may read ahead, sample or
 * aggregate: it is never traced.
 */
private[spark] final class TracePlan(
    val eligible: Boolean,
    val topId: Int,
    val rootId: Int,
    val tapId: Int,
    val description: String)

private[spark] object TracePlan {
  private def name(r: RDD[_]): String = s"${r.getClass.getSimpleName}#${r.id}"

  private def ineligible(top: RDD[_], why: String): TracePlan =
    new TracePlan(false, top.id, -1, -1, why)

  /** Its compute reads the parent iterator on another thread (rule R3'). */
  private def readsOnAnotherThread(r: RDD[_]): Boolean = {
    val n = r.getClass.getSimpleName
    n == "PipedRDD" || n == "PythonRDD" || n.endsWith("RRDD")
  }

  private def isPointwise(r: RDD[_]): Boolean = r match {
    case m: MapPartitionsRDD[_, _] => m.isPointwise
    case _ => false
  }

  /** Rules R1-R4 over the stage's lineage, from its top RDD down to its root. */
  def of(top: RDD[_]): TracePlan = {
    val chain = new ArrayBuffer[RDD[_]]   // chain(0) = top, chain.last = root
    var r: RDD[_] = top
    var atRoot = false
    while (!atRoot) {
      chain += r
      // R4: a persisted or checkpointed RDD may be read from storage, and then the root never runs
      if (r.getStorageLevel != StorageLevel.NONE || r.checkpointData.isDefined) {
        return ineligible(top, s"persisted or checkpointed ${name(r)} (R4)")
      }
      if (readsOnAnotherThread(r)) {
        return ineligible(top, s"${name(r)} reads its parent on another thread (R3')")
      }
      val deps = r.dependencies
      if (deps.forall(_.isInstanceOf[ShuffleDependency[_, _, _]])) {
        atRoot = true   // no dependencies, or only shuffle dependencies: the stage root
      } else if (deps.size == 1 && deps.head.isInstanceOf[OneToOneDependency[_]]) {
        r = deps.head.rdd   // R1: exactly one parent, one to one
      } else {
        return ineligible(top, s"${name(r)} is not a single one-to-one child (R1)")
      }
    }
    val root = chain.last
    // R2: a shuffle root's read order must be the same in both replicas and the verifier. Local
    // mode reads every block locally, in map order; elsewhere only an aggregated, sorted read is
    // ordered. (Not root.outputDeterministicLevel: on executors it can reach a transient dep.rdd.)
    val shuffles = root.dependencies.collect { case d: ShuffleDependency[_, _, _] => d }
    if (shuffles.nonEmpty && !Utils.isLocalMaster(SparkEnv.get.conf) &&
        !shuffles.forall(d => d.aggregator.isDefined && d.keyOrdering.isDefined)) {
      return ineligible(top, s"unordered shuffle root ${name(root)} (R2)")
    }
    // R3: the tap is the highest of the pointwise RDDs directly above the root
    var i = chain.length - 2
    while (i >= 0 && isPointwise(chain(i))) i -= 1
    val tap = chain(i + 1)
    if ((tap eq root) && chain.length > 1) {
      return ineligible(top, s"no pointwise operation directly above the root ${name(root)} (R3)")
    }
    new TracePlan(true, top.id, root.id, tap.id, s"root=${name(root)} tap=${name(tap)}")
  }
}

/** The hooks RDD.iterator and RDD.computeOrReadCheckpoint call; untraced tasks pass through. */
private[spark] object TraceTap extends Logging {

  /** RDD.iterator, before computing: a task's first call (its stage's top RDD) computes its plan. */
  def enter(rdd: RDD[_], context: TaskContext): Unit = context match {
    case ctx: TaskContextImpl if ctx.tracePlan == null && ChunkConfig.merkleOn =>
      ctx.tracePlan = planOf(rdd, ctx)
    case _ =>
  }

  /** computeOrReadCheckpoint: the plan's root counts its elements. */
  def atRoot[T](rdd: RDD[T], split: Partition, context: TaskContext, it: Iterator[T]): Iterator[T] =
    context match {
      case ctx: TaskContextImpl if ctx.tracePlan != null && ctx.tracePlan.eligible &&
          ctx.tracePlan.rootId == rdd.id =>
        rootIterator(split, ctx, it)
      case _ => it
    }

  /** RDD.iterator, after computing: the plan's tap fingerprints its elements as they flow. */
  def leave[T](rdd: RDD[T], split: Partition, context: TaskContext, it: Iterator[T]): Iterator[T] =
    context match {
      case ctx: TaskContextImpl if ctx.tracePlan != null && ctx.tracePlan.eligible &&
          ctx.tracePlan.tapId == rdd.id =>
        tapIterator(split, ctx, it)
      case _ => it
    }

  /** A verification recompute (full-task or chunk): it must not read data a replica produced. */
  def isVerifier(context: TaskContext): Boolean = context match {
    case ctx: TaskContextImpl => ctx.fullTaskVerifier || ctx.targetRange.isDefined
    case _ => false
  }

  private def planOf(top: RDD[_], ctx: TaskContextImpl): TracePlan = {
    val plan =
      try TracePlan.of(top) catch {
        // Tracing must never fail a task: an unexpected lineage is simply not traced
        case NonFatal(e) => new TracePlan(false, top.id, -1, -1, s"plan failed: $e")
      }
    logInfo(s"[TRACE-PLAN] stage ${ctx.stageId} task ${ctx.taskIndex}" +
      (if (ctx.targetRange.isDefined) " (chunk verifier)" else "") +
      (if (plan.eligible) s": eligible ${plan.description}" else s": ineligible: ${plan.description}"))
    plan
  }

  private def rootIterator[T](split: Partition, ctx: TaskContextImpl, it: Iterator[T]): Iterator[T] = {
    // A parallelize root: its size is known, and a chunk verifier can start at its chunk (A5)
    val values: IndexedSeq[Any] = split match {
      case p: ParallelCollectionPartition[_] => p.values match {
        case s: IndexedSeq[_] => s
        case _ => null
      }
      case _ => null
    }
    if (values != null) ctx.rootSize = values.length
    ctx.targetRange match {
      case Some((lo, hi)) if values != null =>
        val from = math.min(lo, values.length.toLong).toInt
        val until = math.min(hi, values.length.toLong).toInt
        ctx.rootPos = from
        val chunk = values.slice(from, until).iterator.asInstanceOf[Iterator[T]]
        new RootCounter(new InterruptibleIterator(ctx, chunk), ctx)
      case Some((lo, hi)) => new RangeRootCounter(it, ctx, lo, hi)
      case None => new RootCounter(it, ctx)
    }
  }

  private def tapIterator[T](split: Partition, ctx: TaskContextImpl, it: Iterator[T]): Iterator[T] = {
    val chunkVerifier = ctx.targetRange.isDefined
    // Replicas (and full-task verifiers) fill a digest, chunk verifiers their range box. Only
    // replicas write trace files: a verifier's file would overwrite a replica's.
    val digest = if (chunkVerifier) null else ChunkConfig.newDigest(ctx)
    val box = if (chunkVerifier) ctx.rangeBox else null
    val writer =
      if (!chunkVerifier && !ctx.fullTaskVerifier && ChunkConfig.writeTraceFiles) {
        val appName = Option(SparkEnv.get)
          .flatMap(e => Option(e.conf.get("spark.app.name", "unknown"))).getOrElse("unknown")
        val w = Trace.createOutputWriter(ctx.stageId, split.index, ctx.taskIndex, appName)
        ctx.addTaskCompletionListener[Unit](_ => try w.safeClose() catch { case _: Throwable => () })
        w
      } else null
    new TapIterator(it, ctx, digest, box, writer)
  }
}

/** Counts the root's elements: rootPos - 1 is then the UID of the element the tap sees. */
private final class RootCounter[T](base: Iterator[T], ctx: TaskContextImpl) extends Iterator[T] {
  override def hasNext: Boolean = base.hasNext
  override def next(): T = {
    val v = base.next()
    ctx.rootPos += 1
    v
  }
}

/** A chunk verifier at a root that cannot seek: skip to lo, stop at hi (nothing after is read). */
private final class RangeRootCounter[T](base: Iterator[T], ctx: TaskContextImpl, lo: Long, hi: Long)
  extends Iterator[T] {
  override def hasNext: Boolean = {
    while (ctx.rootPos < lo && base.hasNext) {
      base.next()
      ctx.rootPos += 1
    }
    ctx.rootPos < hi && base.hasNext
  }
  override def next(): T = {
    if (!hasNext) throw new NoSuchElementException("end of the chunk")
    val v = base.next()
    ctx.rootPos += 1
    v
  }
}

/** The tap: each element's term goes into the digest (replica) or the range box (verifier). */
private final class TapIterator[T](it: Iterator[T], ctx: TaskContextImpl, digest: ChunkDigest,
    box: RangeBox, writer: SafeWriter) extends Iterator[T] {
  override def hasNext: Boolean = {
    val h = it.hasNext
    if (!h && digest != null) digest.exhausted = true
    h
  }
  override def next(): T = {
    val elem = it.next()
    val uid = ctx.rootPos - 1
    if (digest != null) digest.add(uid, elem)
    if (box != null) box.add(uid, elem)
    if (writer != null) writer.writeEntry(uid, elem)
    elem
  }
}
