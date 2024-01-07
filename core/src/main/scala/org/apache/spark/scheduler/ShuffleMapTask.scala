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

package org.apache.spark.scheduler

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

// Modification: Import Necessary Data Structure
import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer}

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.rdd.RDD

/**
 * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency).
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 * @param isBarrier whether this task belongs to a barrier stage. Spark must launch all the tasks
 *                  at the same time for a barrier stage.
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None,
    isBarrier: Boolean = false)
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, localProperties,
    serializedTaskMetrics, jobId, appId, appAttemptId, isBarrier)
  with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) = {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, new Properties, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.distinct
  }

  // Modification: Added Compute Time Map per RDD
  def getComputeTimeMap(finalRdd: RDD[_]): HashMap[Int, Long] = {
    val result = new HashMap[Int, Long]()
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += finalRdd
    def visit(rdd: RDD[_]): Unit = {
      for (dep <- rdd.dependencies) {
        if (dep.rdd != null) {
          waitingForVisit.prepend(dep.rdd)
        }
      }

      if (rdd.tBegan != -1 && rdd.tEnded != -1) {
        val tTotal = rdd.tEnded - rdd.tBegan
        if (!result.contains(rdd.id)) {
          result.put(rdd.id, math.max(0, tTotal))
        }
        else if (tTotal > 0) {
          result.put(rdd.id, tTotal)
        }
      }
      else if (!result.contains(rdd.id)) {
        result.put(rdd.id, 0)
      }
    }
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.remove(0))
    }
    result
  }
  // End of Modification

  // instrument code
  def calculateTrueTime(finalRdd: RDD[_]): mutable.HashMap[Int, Long] = {
    val result = new mutable.HashMap[Int, Long]()
    val toVisit = new mutable.Queue[RDD[_]]()
    val dependenciesToCalculate = new mutable.Stack[RDD[_]]()
    var rdd = finalRdd
    toVisit.enqueue(rdd)
    while (toVisit.nonEmpty) {
      rdd = toVisit.dequeue()
      dependenciesToCalculate.push(rdd)
      for (depRdd <- rdd.dependencies) {
        if (depRdd.rdd != null) {
          if (!dependenciesToCalculate.contains(depRdd.rdd)) {
            dependenciesToCalculate.push(depRdd.rdd)
            toVisit.enqueue(depRdd.rdd)
          }
        }
      }
    }
    while (dependenciesToCalculate.nonEmpty) {
      rdd = dependenciesToCalculate.pop()
      if (rdd.partitionCost != -1) {
      }
      else if (rdd.dependencies.isEmpty) {
        rdd.partitionCost = rdd.timestampEnd - rdd.timestampStart
        result.put(rdd.id, rdd.partitionCost)
      }
      // val startTimeSet: mutable.Set[Long] = mutable.Set[Long](rdd.timestampStart)
      var startTime: Long = rdd.timestampStart
      for (depRdd <- rdd.dependencies) {
        if (depRdd.rdd != null) {
          // startTimeSet.add(depRdd.rdd.timestampEnd)
          if (depRdd.rdd.timestampEnd > startTime) {
            startTime = depRdd.rdd.timestampEnd
          }
        }
      }
      // val startTime = startTimeSet.max
      if (startTime > rdd.timestampEnd) {
        rdd.partitionCost = 0
        result.put(rdd.id, rdd.partitionCost)
      }
      else {
        rdd.partitionCost = rdd.timestampEnd - startTime
        result.put(rdd.id, rdd.partitionCost)
      }
    }
    result
  }
  // instrument code end

  // Modification: Change function signature to accomodate RDD_id, Added Computation time data
  override def runTask(context: TaskContext): (MapStatus, Int) = {
  // End of Modification
    // Deserialize the RDD using the broadcast variable.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTimeNs = System.nanoTime()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val rddAndDep = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTimeNs = System.nanoTime() - deserializeStartTimeNs
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    val rdd = rddAndDep._1
    val dep = rddAndDep._2
    // While we use the old shuffle fetch protocol, we use partitionId as mapId in the
    // ShuffleBlockId construction.
    val mapId = if (SparkEnv.get.conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)) {
      partitionId
    } else context.taskAttemptId()
    // dep.shuffleWriterProcessor.write(rdd, dep, mapId, context, partition)
    // Modification: Returns RDD_id, also added time measurement to send to the memorystore
    val result = dep.shuffleWriterProcessor.write(rdd, dep, mapId, context, partition)
    val computeTimeMap = getComputeTimeMap(rdd)
    SparkEnv.get.blockManager.memoryStore.updateCostData(partition.index, computeTimeMap)
    if (SparkEnv.get.blockManager.memoryStore.replacementPolicy == 1) {
      // LRC
      for (d <- rdd.dependencies) {
        if (d.rdd != null) {
          SparkEnv.get.blockManager.memoryStore.decrementReferenceCount(d.rdd.id, partition.index)
        }
      }
    }

    // TODO: Add Guard for LPW only
    // LPW
    val map = calculateTrueTime(rdd)
    SparkEnv.get.blockManager.memoryStore.updateCost(partition.index, map)

    (result, rdd.id)
    // End of Modification
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
