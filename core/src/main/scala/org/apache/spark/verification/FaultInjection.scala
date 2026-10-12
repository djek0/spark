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

import scala.util.Try

import org.apache.spark.{SparkEnv, TaskContextImpl}
import org.apache.spark.internal.Logging

/** One task's fault; `applied` once its element has been dropped or changed. */
private[spark] final class Fault(val drop: Boolean, val position: String) {
  var applied = false
}

/**
 * Fault injection for experiments (HONEST=False on an executor): a faulty replica really computes
 * a wrong result. At the tap one element is dropped (default) or replaced by a different value of
 * the same type, so the replica's result, digest and trace all carry the fault; in a stage
 * without a tap the first element of the stage's output is. Verification contexts never are.
 *
 * Environment, read once per JVM:
 *  - BYZANTINE_PROBABILITY = N: faulty partitions are those with partitionId % N == 0 (default
 *    10); in local mode only the even replica of each pair
 *  - BYZANTINE_FAULT = drop | value
 *  - BYZANTINE_POSITION = last | first | middle | random (middle and random need the root's size,
 *    known for parallelize roots; otherwise last)
 *  - BYZANTINE_STAGES = all (default) | result
 */
private[spark] object FaultInjection extends Logging {
  private def env(k: String, d: String): String =
    sys.env.getOrElse(k, d).trim.toLowerCase(Locale.ROOT)

  /** The interval N, or 0 when this JVM is honest (the Executor's former parsing). */
  private lazy val interval: Int =
    if (sys.env.getOrElse("HONEST", "True") != "False") 0
    else sys.env.get("BYZANTINE_PROBABILITY") match {
      case Some(s) => Try(s.trim.toInt).map(n => if (n > 0) n else 0).getOrElse(10)
      case None => 10
    }

  lazy val enabled: Boolean = interval > 0
  private lazy val drop: Boolean = env("BYZANTINE_FAULT", "drop") != "value"
  private lazy val position: String = env("BYZANTINE_POSITION", "last") match {
    case p @ ("first" | "middle" | "random" | "last") => p
    case _ => "last"
  }
  private lazy val allStages: Boolean = env("BYZANTINE_STAGES", "all") == "all"

  /** This task's fault, or null: an honest JVM, a verification context, or not selected. */
  def forTask(ctx: TaskContextImpl): Fault = {
    if (!enabled || ctx.targetRange.isDefined || ctx.fullTaskVerifier) return null
    if (ctx.isMapTask && !allStages) return null
    val local = SparkEnv.get.conf.get("spark.master", "local").startsWith("local")
    val selected = ctx.partitionId % interval == 0 && (!local || ctx.taskIndex % 2 == 0)
    if (selected) new Fault(drop, position) else null
  }

  /** The first UID the fault may hit; the last element is hit when the stream ends first. */
  def threshold(f: Fault, ctx: TaskContextImpl): Long = {
    val n = ctx.rootSize
    f.position match {
      case "first" => 0L
      case "middle" if n > 0 => n / 2
      case "random" if n > 0 =>
        java.lang.Math.floorMod(ElementHash.mix64((ctx.stageId.toLong << 32) | ctx.partitionId), n)
      case _ => Long.MaxValue
    }
  }

  /** A different value of the same type, or None: the element is dropped instead. */
  def corrupt(v: Any): Option[Any] = v match {
    case x: Int => Some(x + 1)
    case x: Long => Some(x + 1L)
    case x: Double => Some(if (x.isNaN || x.isInfinite) 0.0 else Math.nextUp(x))
    case x: Float => Some(if (x.isNaN || x.isInfinite) 0.0f else Math.nextUp(x))
    case x: String => Some(x + "!")
    case x: Boolean => Some(!x)
    case (a, b) => corrupt(a).map(c => (c, b)).orElse(corrupt(b).map(c => (a, c)))
    case _ => None
  }

  def logInjected(ctx: TaskContextImpl, what: String): Unit =
    logWarning(s"[BYZANTINE INJECTION] stage ${ctx.stageId} partition ${ctx.partitionId} " +
      s"idx ${ctx.taskIndex}: $what")
}
