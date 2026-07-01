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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 *
 * @param prev the parent RDD.
 * @param f The function used to map a tuple of (TaskContext, partition index, input iterator) to
 *          an output iterator.
 * @param preservesPartitioning Whether the input function preserves the partitioner, which should
 *                              be `false` unless `prev` is a pair RDD and the input function
 *                              doesn't modify the keys.
 * @param isFromBarrier Indicates whether this RDD is transformed from an RDDBarrier, a stage
 *                      containing at least one RDDBarrier shall be turned into a barrier stage.
 * @param isOrderSensitive whether or not the function is order-sensitive. If it's order
 *                         sensitive, it may return totally different result when the input order
 *                         is changed. Mostly stateful functions are order-sensitive.
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false,
    isFilterOperation: Boolean = false,
    isExpanderOperation: Boolean = false)  // For flatMap, flatMapValues (1→M)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    val inputIter = firstParent[T].iterator(split, context)

    // Handle different transformation categories with UID tracking
    if (isFilterOperation) {
      // Category 1: 1→0/1 (Droppers) - filter operations
      // Dequeue UID per input; requeue only if kept
      inputIter.flatMap { value =>
        if (Trace.isCorrelationBroken) {
          // No UID tracking - just apply filter
          val result = f(context, split.index, Iterator(value))
          if (result.hasNext) Some(result.next()) else None
        } else {
          val currentUid = Trace.dequeueUid()
          val result = f(context, split.index, Iterator(value))
          if (result.hasNext) {
            Trace.enqueueUid(currentUid)
            Some(result.next())
          } else {
            None
          }
        }
      }
    } else if (isExpanderOperation) {
      // Category 2: 1→M (Expanders) - flatMap, flatMapValues operations  
      // Each output element gets the same UID from its input (group-based semantics, lazy)
      inputIter.flatMap { value =>
        if (Trace.isCorrelationBroken) {
          // No UID tracking - just apply flatMap
          f(context, split.index, Iterator(value))
        } else {
          val currentUid = Trace.dequeueUid()
          val result = f(context, split.index, Iterator(value))
          result.map { element =>
            Trace.enqueueUid(currentUid)  // Enqueue UID for each output element lazily
            element
          }
        }
      }
    } else {
      // Category 3: 1→1 (Safe Pass-through) - map, mapValues, keyBy
      // No UID queue changes needed - just pass through
      f(context, split.index, inputIter)
    }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  @transient protected lazy override val isBarrier_ : Boolean =
    dependencies.exists(_.rdd.isBarrier())

  override protected def getOutputDeterministicLevel = {
    if (isOrderSensitive && prev.outputDeterministicLevel == DeterministicLevel.UNORDERED) {
      DeterministicLevel.INDETERMINATE
    } else {
      super.getOutputDeterministicLevel
    }
  }
}
