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

package org.apache.spark.storage.memory

import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.LinkedHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{
  AUTO_RDD_CACHING,
  CACHE_MODE,
  REPLACEMENT_POLICY,
  STORAGE_UNROLL_MEMORY_THRESHOLD,
  UNROLL_MEMORY_CHECK_PERIOD,
  UNROLL_MEMORY_GROWTH_FACTOR,
  VANILLA_W_CUSTOM_COMPUTATION
}
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.serializer.{SerializationStream, SerializerManager}
import org.apache.spark.storage._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

private sealed trait MemoryEntry[T] {
  def size: Long
  def memoryMode: MemoryMode
  def classTag: ClassTag[T]
}
private case class DeserializedMemoryEntry[T](
    value: Array[T],
    size: Long,
    memoryMode: MemoryMode,
    classTag: ClassTag[T]
) extends MemoryEntry[T] {}
private case class SerializedMemoryEntry[T](
    buffer: ChunkedByteBuffer,
    memoryMode: MemoryMode,
    classTag: ClassTag[T]
) extends MemoryEntry[T] {
  def size: Long = buffer.size
}

private[storage] trait BlockEvictionHandler {

  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
   *
   * The caller of this method must hold a write lock on the block before calling this method. This
   * method does not release the write lock.
   *
   * @return
   *   the block's new effective StorageLevel.
   */
  private[storage] def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]
  ): StorageLevel
}

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as serialized
 * ByteBuffers.
 */
private[spark] class MemoryStore(
    conf: SparkConf,
    blockInfoManager: BlockInfoManager,
    serializerManager: SerializerManager,
    memoryManager: MemoryManager,
    blockEvictionHandler: BlockEvictionHandler
) extends Logging {
  private val cacheMode: Int = conf.get(CACHE_MODE)
  // these are only used when cacheMode == 4
  private val autoRDDCaching: Boolean = conf.get(AUTO_RDD_CACHING)
  // private val useCustomReplacementPolicy: Boolean =
  //   conf.get(CUSTOM_REPLACEMENT_POLICY)
  val replacementPolicy: Integer = conf.get(REPLACEMENT_POLICY)
  private val vanillaWCustomComputation: Boolean =
    conf.get(VANILLA_W_CUSTOM_COMPUTATION)

  // Note: all changes to memory allocations, notably putting blocks, evicting blocks, and
  // acquiring or releasing unroll memory, must be synchronized on `memoryManager`!

  // Modification: Added a map between RDD signature and Block
  //               and data structures for reference data
  private val entries = new LinkedHashMap[BlockId, MemoryEntry[_]](32, 0.75f, true)
  private val blockIds = new mutable.HashMap[Tuple2[Int, Int], BlockId]()
  private val costData = new mutable.HashMap[Tuple2[Int, Int], Long]()
  private var refData = new mutable.HashMap[Int, mutable.HashSet[Int]]()
  private val allRefData = new mutable.HashMap[Int, mutable.HashSet[Int]]()
  private val lastRef = new mutable.HashMap[Int, Int]()
  private var latestJobId: Int = 0

  // instrument code
  // key = rdd id, value = ref distance
  private var refDistance = new mutable.HashMap[Int, Seq[Int]]()
  private val keyToBlockId = new mutable.HashMap[String, BlockId]()
  // instrument code end

  // instrument code

  // private val weights = new mutable.HashMap[String, WholeBlockId]()
  private val keyToBlockIdLPW = new mutable.HashMap[String, BlockId]()
  // key = rdd id, value = ref count
  private var refCount = new mutable.HashMap[Int, Int]()

  private val computationCost = new mutable.HashMap[Tuple2[Int, Int], Long]()
  // key = [rddId, partitionId]
  private val pastRefInfo = new mutable.HashMap[Int, mutable.HashMap[Int, Int]]()

  def updateRefLPW(
    jobId: Int,
    partitionCount: Int,
    refCountByJob: mutable.HashMap[Int, Int]
  ): Unit = {
    refCount.synchronized {
      refCount = refCountByJob.clone()
    }
  }

  def updateCost(partitionId: Int, map: mutable.HashMap[Int, Long]): Unit = {
    val iterator = map.iterator
    val curTime = System.currentTimeMillis()
    for (item <- iterator) {
      computationCost.synchronized {
        if (!computationCost.contains((item._1, partitionId))) {
          computationCost.put((item._1, partitionId), item._2)
        }
      }
      calculateWeight(partitionId, item._1)
    }
  }

  def calculateWeight(partitionId: Int, rddid: Int): Unit = {
    // I decided to update weight here instead of doing so in another function
    val blockId = keyToBlockIdLPW.get(rddid.toString + "_" + partitionId.toString)
    blockId match {
      case None =>

      case Some(x) =>
        val size = getSize(x).toDouble
        val cost = computationCost.synchronized {
          computationCost.get((rddid, partitionId)) match {
            case Some(x) =>
              x.toDouble
            case None =>
              0.0
          }
        }
        val ref: Double = refCount.synchronized {
          refCount.get(rddid) match {
            case Some(x) =>
              x.toDouble
            case None =>
              0.0
          }
        }
        val pastmod = pastRefInfo.synchronized {
          var pastCount = 0
          for (item <- pastRefInfo) {
            if (item._2.contains(rddid)) {
              pastCount += 1
            }
          }
          if (pastRefInfo.size == 0) {
            1.toDouble
          }
          else {
            1.toDouble + (pastCount.toDouble / pastRefInfo.size.toDouble)
          }
        }
        val inMem = entries.synchronized {
          entries.get(x).memoryMode match {
            case MemoryMode.ON_HEAP =>
              1.toDouble
            case _ =>
              0.toDouble
          }
        }
        val weight = (inMem * cost * ref * pastmod) / size
        x.updateWeight(weight)
        keyToBlockIdLPW.update(rddid.toString + "_" + partitionId.toString, x)
    }
  }

  def putOrUpdateBlockLPW(id: String, partition: BlockId): Unit = {
    keyToBlockIdLPW.synchronized {
      if (id.split("_")(0) != "rdd") {
        return
      }

      if (entries.get(partition).memoryMode != MemoryMode.ON_HEAP) {
        partition.updateWeight(0)
      }
      keyToBlockIdLPW.put(id.split("_")(1) + "_" + id.split("_")(2), partition)
    }
  }

  def decreaseRefCount(jobId: Int): Unit = {
    pastRefInfo.synchronized {
      pastRefInfo.put(jobId, refCount)
    }
  }
  // instrument code end

  // Modification: added Reference Count and BlockId refcount
  private val referenceCount = new mutable.HashMap[Int, Int]()
  private val blockIdToRC = new mutable.HashMap[BlockId, Int]()

  def updateAndAddBlockId(blockId: BlockId): Unit = blockIdToRC.synchronized {
    val rddId = getRddId(blockId)
    rddId match {
      case Some(id) =>
        referenceCount.synchronized {
          blockIdToRC.put(blockId, referenceCount.getOrElse(id, 0))
        }
      case None =>
        // Not an RDD
    }
  }

  def decrementReferenceCount(rddId: Int, partition: Int): Unit = blockIdToRC.synchronized {
    val blockId = RDDBlockId(rddId, partition)
    if (blockIdToRC.getOrElse(blockId, 0) > 0) {
      blockIdToRC(blockId) -= 1
    }
  }

  def updateReferenceCount(
    lineage: mutable.HashMap[Int, mutable.HashSet[Int]]): Unit = referenceCount.synchronized {
    referenceCount.keys.foreach(id => referenceCount(id) = 0)
    for (tuple <- lineage.iterator) {
      referenceCount.put(tuple._1, tuple._2.size)
    }
    blockIdToRC.synchronized {
      for (blockId <- blockIdToRC.keys.iterator) {
        val rddId = getRddId(blockId)
        rddId match {
          case Some(id) =>
            blockIdToRC.put(blockId, referenceCount.getOrElse(id, 0))
          case None =>
            // Not an RDD
        }
      }
    }
  }
  // End of Modification

  // A mapping from taskAttemptId to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `memoryManager`
  private val onHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()
  // Note: off-heap unroll memory is only used in putIteratorAsBytes() because off-heap caching
  // always stores serialized values.
  private val offHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()

  // Initial memory to request before unrolling any block
  private val unrollMemoryThreshold: Long =
    conf.get(STORAGE_UNROLL_MEMORY_THRESHOLD)


  /** Total amount of memory available for storage, in bytes. */
  private def maxMemory: Long = {
    memoryManager.maxOnHeapStorageMemory + memoryManager.maxOffHeapStorageMemory
  }

  if (maxMemory < unrollMemoryThreshold) {
    logWarning(
      s"Max memory ${Utils.bytesToString(maxMemory)} is less than the initial memory " +
        s"threshold ${Utils.bytesToString(unrollMemoryThreshold)} needed to store a block in " +
        s"memory. Please configure Spark with more memory."
    )
  }

  logInfo(
    "MemoryStore started with capacity %s".format(
      Utils.bytesToString(maxMemory)
    )
  )

  /** Total storage memory used including unroll memory, in bytes. */
  private def memoryUsed: Long = memoryManager.storageMemoryUsed

  /**
   * Amount of storage memory, in bytes, used for caching blocks. This does not include memory used
   * for unrolling.
   */
  private def blocksMemoryUsed: Long = memoryManager.synchronized {
    memoryUsed - currentUnrollMemory
  }

  def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  // instrument code
  def updateRef(refDistanceMaster: mutable.HashMap[Int, Seq[Int]]): Unit = {
    refDistance.synchronized {
      refDistance = refDistanceMaster.clone()
    }
  }

  def putOrUpdateBlock(id: String, partition: BlockId): Unit = {
    keyToBlockId.synchronized {
      if (id.split("_")(0) != "rdd") {
        return
      }
      keyToBlockId.put(id.split("_")(1) + "_" + id.split("_")(2), partition)
    }
  }

  def calculatePriority(stageIdNow: Int): Unit = {
    if (refDistance.isEmpty) {
      return
    }
    var refDistanceClone = refDistance.clone()
    var refDistanceLow = new mutable.HashMap[Int, Int]
    // determine priorities
    for ((rddId, stageIdRefs) <- refDistanceClone) {
      var temp = Seq[Int]()
      for (stageIdRef <- stageIdRefs) {
        var diff = stageIdRef - stageIdNow
        if (diff >= 0) {
          temp = temp :+ diff
        }
      }
      if (!temp.isEmpty) {
        refDistanceLow(rddId) = temp.min
      } else {
        refDistanceLow(rddId) = 99
      }
    }
    var rddsPriority = refDistanceLow.toList.sortBy(_._2).reverse
    // update priority to keyToBlockId
    keyToBlockId.synchronized {
      for (((rddIdPrior, _), index) <- rddsPriority.zipWithIndex) {
        for ((key, blockId) <- keyToBlockId) {
          var rddIdKey = key.split("_")(0).toInt
          if (rddIdKey == rddIdPrior) {
            blockId.updatePriority((index + 1).toDouble)
          }
        }
      }
    }
  }
  // instrument code end

  // Modification: Added Weight Calculation, should be called from the Executor
  //               Also added RPC endpoint
  def updateReferenceData(jobId: Int,
                          refDataUpdated: mutable.HashMap[Int, mutable.HashSet[Int]]): Unit = {
    latestJobId = jobId
    refData.synchronized {
      refData = refDataUpdated.clone()
      allRefData.synchronized {
        lastRef.synchronized {
          for (ref <- refData) {
              allRefData.getOrElseUpdate(ref._1, new mutable.HashSet[Int]()) ++= ref._2
              lastRef.put(ref._1, jobId)
          }
        }
      }
    }
  }

  def updateAfterJobSuccess(jobId: Int): Unit = {
    latestJobId = jobId
    blockIds.synchronized {
      val blockIter = blockIds.keysIterator
      for (key <- blockIter) {
        updatePartitionWeight(key)
      }
    }
  }

def updateCostData(partition: Int, costMap: mutable.HashMap[Int, Long]): Unit = {
    costData.synchronized {
      for (pair <- costMap) {
        if (!costData.contains((pair._1, partition))) {
          costData.put((pair._1, partition), pair._2)
        }
        else if (pair._2 != 0) {
          costData.put((pair._1, partition), pair._2)
        }
      }
    }
  }

  def updatePartitionWeight(rddSignature: Tuple2[Int, Int]): Unit = {
    val blockIdOpt = blockIds.synchronized {
      blockIds.get(rddSignature)
    }

    blockIdOpt match {
      case Some(blockId) =>
        val onHeap = entries.synchronized {
          entries.get(blockId).memoryMode match {
            case MemoryMode.ON_HEAP =>
              true
            case _ =>
              false
          }
        }
        if (onHeap) {
          val size = getSize(blockId)
          val scaledSize = size == 0L match {
            case true =>
              1.0
            case false =>
              math.min(
                8.0, // Maximum value: (if scaledSize > 8, set to 8)
                math.max(
                  1.0, // Minimum value: (if scaledSize < 1, set to 1)
                  (scaleByLog(16.0, size.toDouble / 1024) + 1.0)))
          }
          val computeTime = costData.synchronized {
            costData.getOrElse(rddSignature, 0L)
          }
          val scaledComputeTime = computeTime == 0L match {
            case true =>
              1.0
            case false =>
              math.min(
                8.0, // Maximum value: (if scaledComputeTime > 8, set to 8)
                math.max(
                  1.0, // Minimum value: (if scaledComputeTime < 1, set to 1)
                  (math.log10(computeTime.toDouble) + 1.0)))
          }
          val thisRefCount = refData.synchronized {
            refData.getOrElseUpdate(rddSignature._1, new mutable.HashSet[Int]()).size.toDouble
          }
          val thisPastRef = allRefData.synchronized {
            allRefData.getOrElseUpdate(rddSignature._1,
                                       new mutable.HashSet[Int]()).size.toDouble - thisRefCount
          }
          val thisDistance = lastRef.synchronized {
            (latestJobId - lastRef.getOrElse(rddSignature._1, latestJobId - 1)).toDouble
          }
          // Take whichever is larger between refcount and heuristic weight
          val weight = math.max(thisRefCount,
                                ((scaledComputeTime * thisPastRef) /
                                (scaledSize * thisDistance)))
          if (weight > 0.0) {
            blockId.setWeight(weight)
            blockIds.synchronized {
              blockIds.put(rddSignature, blockId)
            }
          }
          else {
            // Update Weight to 0 if somehow the weight is less than 0
            blockId.setWeight(0.0)
            blockIds.synchronized {
              blockIds.put(rddSignature, blockId)
            }
          }
        }
        else {
          // Update Weight to 0 if it is not on heap, probably went straight to disc
          blockId.setWeight(0.0)
          blockIds.synchronized {
            blockIds.put(rddSignature, blockId)
          }
        }
      case None =>
        // This means the RDD partition isn't stored in the memory store
        // or was stored but was removed
        // TODO: Use this space to update a computation time, for unstored
        //       RDDs
    }
  }

  def scaleByLog(base: Double, num: Double): Double = {
    math.log10(num) / math.log10(base)
  }
  // End of Modification

  /**
   * Use `size` to test if there is enough space in MemoryStore. If so, create the ByteBuffer and
   * put it into MemoryStore. Otherwise, the ByteBuffer won't be created.
   *
   * The caller should guarantee that `size` is correct.
   *
   * @return
   *   true if the put() succeeded, false otherwise.
   */
  def putBytes[T: ClassTag](
      blockId: BlockId,
      size: Long,
      memoryMode: MemoryMode,
      _bytes: () => ChunkedByteBuffer
  ): Boolean = {
    require(
      !contains(blockId),
      s"Block $blockId is already present in the MemoryStore"
    )
    if (memoryManager.acquireStorageMemory(blockId, size, memoryMode)) {
      // We acquired enough memory for the block, so go ahead and put it
      val bytes = _bytes()
      assert(bytes.size == size)
      val entry =
        new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
      entries.synchronized {
        entries.put(blockId, entry)
        // instrument code
        // if (cacheMode == 3) {
        //   putOrUpdateBlock(blockId.name, blockId)
        // }
        if (replacementPolicy == 1) {
          // LRC
          updateAndAddBlockId(blockId)
        } else if (replacementPolicy == 2) {
          // LPW
          putOrUpdateBlockLPW(blockId.name, blockId)
        }
        // instrument code end
      }
      // Modification: Add Block to name map, for ease of updating
      // We only want RDD blocks
      // if ((cacheMode == 4 && useCustomReplacementPolicy) || vanillaWCustomComputation) {
      //   getRddSignature(blockId).map(signature => blockIds.synchronized {
      //     blockIds.put(signature, blockId)
      //   })
      // }
      // End of Modification
      logInfo(
        "Block %s stored as bytes in memory (estimated size %s, free %s)"
          .format(
            blockId,
            Utils.bytesToString(size),
            Utils.bytesToString(maxMemory - blocksMemoryUsed)
          )
      )
      true
    } else {
      false
    }
  }

  /**
   * Attempt to put the given block in memory store as values or bytes.
   *
   * It's possible that the iterator is too large to materialize and store in memory. To avoid OOM
   * exceptions, this method will gradually unroll the iterator while periodically checking whether
   * there is enough free memory. If the block is successfully materialized, then the temporary
   * unroll memory used during the materialization is "transferred" to storage memory, so we won't
   * acquire more memory than is actually needed to store the block.
   *
   * @param blockId
   *   The block id.
   * @param values
   *   The values which need be stored.
   * @param classTag
   *   the [[ClassTag]] for the block.
   * @param memoryMode
   *   The values saved memory mode(ON_HEAP or OFF_HEAP).
   * @param valuesHolder
   *   A holder that supports storing record of values into memory store as values or bytes.
   * @return
   *   if the block is stored successfully, return the stored data size. Else return the memory has
   *   reserved for unrolling the block (There are two reasons for store failed: First, the block is
   *   partially-unrolled; second, the block is entirely unrolled and the actual stored data size is
   *   larger than reserved, but we can't request extra memory).
   */
  private def putIterator[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T],
      memoryMode: MemoryMode,
      valuesHolder: ValuesHolder[T]
  ): Either[Long, Long] = {
    require(
      !contains(blockId),
      s"Block $blockId is already present in the MemoryStore"
    )

    // Number of elements unrolled so far
    var elementsUnrolled = 0
    // Whether there is still enough memory for us to continue unrolling this block
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes).
    val initialMemoryThreshold = unrollMemoryThreshold
    // How often to check whether we need to request more memory
    val memoryCheckPeriod = conf.get(UNROLL_MEMORY_CHECK_PERIOD)
    // Memory currently reserved by this task for this particular unrolling operation
    var memoryThreshold = initialMemoryThreshold
    // Memory to request as a multiple of current vector size
    val memoryGrowthFactor = conf.get(UNROLL_MEMORY_GROWTH_FACTOR)
    // Keep track of unroll memory used by this particular block / putIterator() operation
    var unrollMemoryUsedByThisBlock = 0L

    // Request enough memory to begin unrolling
    keepUnrolling = reserveUnrollMemoryForThisTask(
      blockId,
      initialMemoryThreshold,
      memoryMode
    )

    if (!keepUnrolling) {
      logWarning(
        s"Failed to reserve initial memory threshold of " +
          s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory."
      )
    } else {
      unrollMemoryUsedByThisBlock += initialMemoryThreshold
    }

    // Unroll this block safely, checking whether we have exceeded our threshold periodically
    while (values.hasNext && keepUnrolling) {
      valuesHolder.storeValue(values.next())
      if (elementsUnrolled % memoryCheckPeriod == 0) {
        val currentSize = valuesHolder.estimatedSize()
        // If our vector's size has exceeded the threshold, request more memory
        if (currentSize >= memoryThreshold) {
          val amountToRequest =
            (currentSize * memoryGrowthFactor - memoryThreshold).toLong
          keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
          if (keepUnrolling) {
            unrollMemoryUsedByThisBlock += amountToRequest
          }
          // New threshold is currentSize * memoryGrowthFactor
          memoryThreshold += amountToRequest
        }
      }
      elementsUnrolled += 1
    }

    // Make sure that we have enough memory to store the block. By this point, it is possible that
    // the block's actual memory usage has exceeded the unroll memory by a small amount, so we
    // perform one final call to attempt to allocate additional memory if necessary.
    if (keepUnrolling) {
      val entryBuilder = valuesHolder.getBuilder()
      val size = entryBuilder.preciseSize
      if (size > unrollMemoryUsedByThisBlock) {
        val amountToRequest = size - unrollMemoryUsedByThisBlock
        keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
        if (keepUnrolling) {
          unrollMemoryUsedByThisBlock += amountToRequest
        }
      }

      if (keepUnrolling) {
        val entry = entryBuilder.build()
        // Synchronize so that transfer is atomic
        memoryManager.synchronized {
          releaseUnrollMemoryForThisTask(
            memoryMode,
            unrollMemoryUsedByThisBlock
          )
          val success =
            memoryManager.acquireStorageMemory(blockId, entry.size, memoryMode)
          assert(success, "transferring unroll memory to storage memory failed")
        }

        entries.synchronized {
          entries.put(blockId, entry)
          if (replacementPolicy == 1) {
            // LRC
            updateAndAddBlockId(blockId)
          } else if (replacementPolicy == 2) {
            // LPW
            putOrUpdateBlockLPW(blockId.name, blockId)
          }
        }
        // Modification: Add Block to name map, for ease of updating
        // We only want RDD blocks
        // if (cacheMode == 4 && useCustomReplacementPolicy || vanillaWCustomComputation) {
        //   getRddSignature(blockId).map(signature => blockIds.synchronized {
        //     blockIds.put(signature, blockId)
        //   })
        // }
        // End of Modification
        logInfo(
          "Block %s stored as values in memory (estimated size %s, free %s)"
            .format(
              blockId,
              Utils.bytesToString(entry.size),
              Utils.bytesToString(maxMemory - blocksMemoryUsed)
            )
        )
        Right(entry.size)
      } else {
        // We ran out of space while unrolling the values for this block
        logUnrollFailureMessage(blockId, entryBuilder.preciseSize)
        Left(unrollMemoryUsedByThisBlock)
      }
    } else {
      // We ran out of space while unrolling the values for this block
      logUnrollFailureMessage(blockId, valuesHolder.estimatedSize())
      Left(unrollMemoryUsedByThisBlock)
    }
  }

  /**
   * Attempt to put the given block in memory store as values.
   *
   * @return
   *   in case of success, the estimated size of the stored data. In case of failure, return an
   *   iterator containing the values of the block. The returned iterator will be backed by the
   *   combination of the partially-unrolled block and the remaining elements of the original input
   *   iterator. The caller must either fully consume this iterator or call `close()` on it in order
   *   to free the storage memory consumed by the partially-unrolled block.
   */
  private[storage] def putIteratorAsValues[T](
      blockId: BlockId,
      values: Iterator[T],
      memoryMode: MemoryMode,
      classTag: ClassTag[T]
  ): Either[PartiallyUnrolledIterator[T], Long] = {

    val valuesHolder = new DeserializedValuesHolder[T](classTag, memoryMode)

    putIterator(blockId, values, classTag, memoryMode, valuesHolder) match {
      case Right(storedSize) => Right(storedSize)
      case Left(unrollMemoryUsedByThisBlock) =>
        val unrolledIterator = if (valuesHolder.vector != null) {
          valuesHolder.vector.iterator
        } else {
          valuesHolder.arrayValues.toIterator
        }

        Left(
          new PartiallyUnrolledIterator(
            this,
            memoryMode,
            unrollMemoryUsedByThisBlock,
            unrolled = unrolledIterator,
            rest = values
          )
        )
    }
  }

  /**
   * Attempt to put the given block in memory store as bytes.
   *
   * @return
   *   in case of success, the estimated size of the stored data. In case of failure, return a
   *   handle which allows the caller to either finish the serialization by spilling to disk or to
   *   deserialize the partially-serialized block and reconstruct the original input iterator. The
   *   caller must either fully consume this result iterator or call `discard()` on it in order to
   *   free the storage memory consumed by the partially-unrolled block.
   */
  private[storage] def putIteratorAsBytes[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T],
      memoryMode: MemoryMode
  ): Either[PartiallySerializedBlock[T], Long] = {

    require(
      !contains(blockId),
      s"Block $blockId is already present in the MemoryStore"
    )

    // Initial per-task memory to request for unrolling blocks (bytes).
    val initialMemoryThreshold = unrollMemoryThreshold
    val chunkSize =
      if (initialMemoryThreshold > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
        logWarning(
          s"Initial memory threshold of ${Utils.bytesToString(initialMemoryThreshold)} " +
            s"is too large to be set as chunk size. Chunk size has been capped to " +
            s"${Utils.bytesToString(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)}"
        )
        ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH
      } else {
        initialMemoryThreshold.toInt
      }

    val valuesHolder = new SerializedValuesHolder[T](
      blockId,
      chunkSize,
      classTag,
      memoryMode,
      serializerManager
    )

    putIterator(blockId, values, classTag, memoryMode, valuesHolder) match {
      case Right(storedSize) => Right(storedSize)
      case Left(unrollMemoryUsedByThisBlock) =>
        Left(
          new PartiallySerializedBlock(
            this,
            serializerManager,
            blockId,
            valuesHolder.serializationStream,
            valuesHolder.redirectableStream,
            unrollMemoryUsedByThisBlock,
            memoryMode,
            valuesHolder.bbos,
            values,
            classTag
          )
        )
    }
  }

  def getBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    val entry = entries.synchronized { entries.get(blockId) }
    entry match {
      case null => None
      case e: DeserializedMemoryEntry[_] =>
        throw new IllegalArgumentException(
          "should only call getBytes on serialized blocks"
        )
      case SerializedMemoryEntry(bytes, _, _) => Some(bytes)
    }
  }

  def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val entry = entries.synchronized { entries.get(blockId) }
    entry match {
      case null => None
      case e: SerializedMemoryEntry[_] =>
        throw new IllegalArgumentException(
          "should only call getValues on deserialized blocks"
        )
      case DeserializedMemoryEntry(values, _, _, _) =>
        val x = Some(values)
        x.map(_.iterator)
    }
  }

  def freeMemoryEntry[T <: MemoryEntry[_]](entry: T): Unit = {
    entry match {
      case SerializedMemoryEntry(buffer, _, _) => buffer.dispose()
      case e: DeserializedMemoryEntry[_] =>
        e.value.foreach {
          case o: AutoCloseable =>
            try {
              o.close()
            } catch {
              case NonFatal(e) =>
                logWarning("Fail to close a memory entry", e)
            }
          case _ =>
        }
    }
  }

  def remove(blockId: BlockId): Boolean = memoryManager.synchronized {
    val entry = entries.synchronized {
      entries.remove(blockId)
    }
    if (entry != null) {
      freeMemoryEntry(entry)
      memoryManager.releaseStorageMemory(entry.size, entry.memoryMode)
      logDebug(
        s"Block $blockId of size ${entry.size} dropped " +
          s"from memory (free ${maxMemory - blocksMemoryUsed})"
      )
      if (replacementPolicy == 1) {
        // LRC
        val _ = blockIdToRC.synchronized {
          blockIdToRC.remove(blockId)
        }
      } else if (replacementPolicy == 2) {
        if (blockId.name.split("_")(0) == "rdd") {
          keyToBlockIdLPW.synchronized {
            keyToBlockIdLPW.remove(blockId.name.split("_")(1) + "_" + blockId.name.split("_")(2))
          }
        }
      }
      // Modification: Remove block from name map
      // We only want RDD blocks
      // if (cacheMode == 4 && useCustomReplacementPolicy || vanillaWCustomComputation) {
      //   getRddSignature(blockId).map(signature => blockIds.synchronized {
      //     blockIds.remove(signature)
      //   })
      // }
      // End of Modification
      // instrument code
      // if (cacheMode == 3) {
      //   assert(false)
      //   if (blockId.name.split("_")(0) == "rdd") {
      //     keyToBlockId.synchronized {
      //       keyToBlockId.remove(
      //         blockId.name.split("_")(1) + "_" + blockId.name.split("_")(2)
      //       )
      //     }
      //   }
      // }
      // instrument code end
      true
    } else {
      false
    }
  }

  def clear(): Unit = memoryManager.synchronized {
    entries.synchronized {
      entries.values.asScala.foreach(freeMemoryEntry)
      entries.clear()
      // instrument code
      // if (cacheMode == 3) {
      //   keyToBlockId.clear()
      // }
      // instrument code end
    }
    onHeapUnrollMemoryMap.clear()
    offHeapUnrollMemoryMap.clear()
    memoryManager.releaseAllStorageMemory()
    logInfo("MemoryStore cleared")
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  // Modification: Add Get RDD Signature From block
  private def getRddSignature(blockId: BlockId): Option[Tuple2[Int, Int]] = {
    blockId.asRDDId.map(rddBlock => (rddBlock.rddId, rddBlock.splitIndex))
  }
  // End of Modification

  /**
   * Try to evict blocks to free up a given amount of space to store a particular block. Can fail if
   * either the block is bigger than our memory or it would require replacing another block from the
   * same RDD (which leads to a wasteful cyclic replacement pattern for RDDs that don't fit into
   * memory that we want to avoid).
   *
   * @param blockId
   *   the ID of the block we are freeing space for, if any
   * @param space
   *   the size of this block
   * @param memoryMode
   *   the type of memory to free (on- or off-heap)
   * @return
   *   the amount of memory (in bytes) freed by eviction
   */
  private[spark] def evictBlocksToFreeSpace(
      blockId: Option[BlockId],
      space: Long,
      memoryMode: MemoryMode
  ): Long = {
    assert(space > 0)
    logInfo("Evicting blocks to free space")
    // instrument code
    val stageId = TaskContext.get().stageId()
    // instrument code end
    memoryManager.synchronized {
      var freedMemory = 0L
      val rddToAdd = blockId.flatMap(getRddId)
      val selectedBlocks = new ArrayBuffer[BlockId]
      val fakeSelectedBlocks = new ArrayBuffer[BlockId]
      def blockIsEvictable(blockId: BlockId, entry: MemoryEntry[_]): Boolean = {
        entry.memoryMode == memoryMode && (rddToAdd.isEmpty || rddToAdd != getRddId(
          blockId
        ))
      }
      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      entries.synchronized {
          // Modification: Sort by least weight
    // if (cacheMode == 4 && useCustomReplacementPolicy) {
    //   val iterator = blockIds.synchronized {
    //   val blockIter = blockIds.keysIterator
    //   for (key <- blockIter) {
    //     updatePartitionWeight(key)
    //   }
    //   blockIds.toList.sortBy(_._2.weight).iterator
    //   }
    //   // Original:
    //   // val iterator = entries.entrySet().iterator()
    //   // End of Modification
    //   while (freedMemory < space && iterator.hasNext) {
    //     val pair = iterator.next()
    //     // Modification: Changed to use Scala Syntax from Java as we now use a Scala iterator
    //     val blockId = pair._2
    //     val entry = entries.get(blockId)
    //     // End of Modification
    //     if (blockIsEvictable(blockId, entry)) {
    //       // We don't want to evict blocks which are currently being read, so we need to obtain
    //       // an exclusive write lock on blocks which are candidates for eviction. We perform a
    //       // non-blocking "tryLock" here in order to ignore blocks which are locked for reading:
    //       if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
    //         selectedBlocks += blockId
    //         freedMemory += entry.size
    //       }
    //     }
    //   }
    // }
    // // End of Modification
    // else if (cacheMode == 3) {
    //   assert(false)
    //   // instrument code
    //   // val iterator = entries.entrySet().iterator()
    //   calculatePriority(stageId)
    //   val iterator = keyToBlockId.toList.sortBy(_._2.priority).iterator
    //   // instrument code end
    //   while (freedMemory < space && iterator.hasNext) {
    //     // instrument code
    //     // val pair = iterator.next()
    //     // val blockId = pair.getKey
    //     // val entry = pair.getValue
    //     val pair = iterator.next()
    //     val blockId = pair._2
    //     val entry = entries.get(blockId)
    //     // instrument code end
    //     if (blockIsEvictable(blockId, entry)) {
    //       // We don't want to evict blocks which are currently being read, so we need to obtain
    //       // an exclusive write lock on blocks which are candidates for eviction. We perform a
    //       // non-blocking "tryLock" here in order to ignore blocks which are locked for reading:
    //       if (
    //         blockInfoManager
    //           .lockForWriting(blockId, blocking = false)
    //           .isDefined
    //       ) {
    //         selectedBlocks += blockId
    //         freedMemory += entry.size
    //         // scalastyle:off println
    //         // print(s"Evicting block ${blockId.name}")
    //         // print(s" of priority ${blockId.priority}")
    //         // print(s" size ${entry.size}")
    //         // println()
    //         // scalastyle:on println
    //       }
    //     }
    //   }
    // } else if (cacheMode == 5) {
    //   val iterator = keyToBlockId.toList.sortBy(_._2.weight).iterator
    //   while (freedMemory < space && iterator.hasNext) {
    //     // modification code
    //     val pair = iterator.next()
    //     val partitionId = pair._1
    //     val blockId = pair._2
    //     val entry = entries.get(blockId)
    //     if (blockIsEvictable(blockId, entry)) {
    //       if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
    //         selectedBlocks += blockId
    //         freedMemory += entry.size
    //       }
    //     }
    //     // modification code end
    //   }
        if (replacementPolicy == 1) {
          // LRC
          blockIdToRC.synchronized {
            val iterator = blockIdToRC.toList.sortBy(_._2).iterator
            // val iterator = entries.entrySet().iterator()
            while (freedMemory < space && iterator.hasNext) {
              val pair = iterator.next()
              val blockId = pair._1
              val entry = entries.get(blockId)
              // val blockId = pair.getKey
              // val entry = pair.getValue
              if (blockIsEvictable(blockId, entry)) {
            // We don't want to evict blocks which are currently being read, so we need to obtain
            // an exclusive write lock on blocks which are candidates for eviction. We perform a
            // non-blocking "tryLock" here in order to ignore blocks which are locked for reading:
                if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
                  selectedBlocks += blockId
                  freedMemory += entry.size
                }
              }
            }
          }
        } else {
      // if (vanillaWCustomComputation) {
      //   var fakeFreedMemory = 0L
      //   val iterator = blockIds.synchronized {
      //   val blockIter = blockIds.keysIterator
      //   for (key <- blockIter) {
      //     updatePartitionWeight(key)
      //   }
      //   blockIds.toList.sortBy(_._2.weight).iterator
      //   }
      //   // Original:
      //   // val iterator = entries.entrySet().iterator()
      //   // End of Modification
      //   while (freedMemory < space && iterator.hasNext) {
      //     val pair = iterator.next()
      //     // Modification: Changed to use Scala Syntax from Java as we now use a Scala iterator
      //     val blockId = pair._2
      //     val entry = entries.get(blockId)
      //     // End of Modification
      //     if (blockIsEvictable(blockId, entry)) {
      //       if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
      //         fakeSelectedBlocks += blockId
      //         fakeFreedMemory += entry.size
      //       }
      //     }
      //   }
      // }
          val iterator = entries.entrySet().iterator()
          while (freedMemory < space && iterator.hasNext) {
            val pair = iterator.next()
            val blockId = pair.getKey
            val entry = pair.getValue
            if (blockIsEvictable(blockId, entry)) {
              // We don't want to evict blocks which are currently being read, so we need to obtain
              // an exclusive write lock on blocks which are candidates for eviction. We perform a
              // non-blocking "tryLock" here in order to ignore blocks which are locked for reading:
              if (
                blockInfoManager
                  .lockForWriting(blockId, blocking = false)
                  .isDefined
              ) {
                selectedBlocks += blockId
                freedMemory += entry.size
              }
            }
          }
        }
        // logInfo(s"Cached entries before eviction: $entries.")
        // logInfo(s"Selected blocks: $selectedBlocks.")
        // logInfo(s"Amount to be freed: $freedMemory.")
      }

      def dropBlock[T](blockId: BlockId, entry: MemoryEntry[T]): Unit = {
        val data = entry match {
          case DeserializedMemoryEntry(values, _, _, _) => Left(values)
          case SerializedMemoryEntry(buffer, _, _) => Right(buffer)
        }
        val newEffectiveStorageLevel =
          blockEvictionHandler.dropFromMemory(blockId, () => data)(
            entry.classTag
          )
        if (newEffectiveStorageLevel.isValid) {
          // The block is still present in at least one store, so release the lock
          // but don't delete the block info
          blockInfoManager.unlock(blockId)
        } else {
          // The block isn't present in any store, so delete the block info so that the
          // block can be stored again
          blockInfoManager.removeBlock(blockId)
        }
      }

      if (freedMemory >= space) {
        var lastSuccessfulBlock = -1
        try {
          logInfo(
            s"${selectedBlocks.size} blocks selected for dropping " +
              s"(${Utils.bytesToString(freedMemory)} bytes)"
          )
          (0 until selectedBlocks.size).foreach { idx =>
            val blockId = selectedBlocks(idx)
            val entry = entries.synchronized {
              entries.get(blockId)
            }
            // This should never be null as only one task should be dropping
            // blocks and removing entries. However the check is still here for
            // future safety.
            if (entry != null) {
              dropBlock(blockId, entry)
              afterDropAction(blockId)
            }
            lastSuccessfulBlock = idx
          }
          // logInfo(s"Cached entries after eviction: $entries.")
          // logInfo(
          //   s"After dropping ${selectedBlocks.size} blocks, " +
          //     s"free memory is ${Utils.bytesToString(maxMemory - blocksMemoryUsed)}"
          // )
          freedMemory
        } finally {
          // like BlockManager.doPut, we use a finally rather than a catch to avoid having to deal
          // with InterruptedException
          if (lastSuccessfulBlock != selectedBlocks.size - 1) {
            // the blocks we didn't process successfully are still locked, so we have to unlock them
            (lastSuccessfulBlock + 1 until selectedBlocks.size).foreach { idx =>
              val blockId = selectedBlocks(idx)
              blockInfoManager.unlock(blockId)
            }
          }
        }
      } else {
        blockId.foreach { id =>
          logInfo(s"Will not store $id . Can free $freedMemory but asked for $space")
        }
        selectedBlocks.foreach { id =>
          blockInfoManager.unlock(id)
        }
        0L
      }
    }
  }

  // hook for testing, so we can simulate a race
  protected def afterDropAction(blockId: BlockId): Unit = {}

  def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }

  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Reserve memory for unrolling the given block for this task.
   *
   * @return
   *   whether the request is granted.
   */
  def reserveUnrollMemoryForThisTask(
      blockId: BlockId,
      memory: Long,
      memoryMode: MemoryMode
  ): Boolean = {
    memoryManager.synchronized {
      val success =
        memoryManager.acquireUnrollMemory(blockId, memory, memoryMode)
      if (success) {
        val taskAttemptId = currentTaskAttemptId()
        val unrollMemoryMap = memoryMode match {
          case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
          case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
        }
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
      success
    }
  }

  /**
   * Release memory used by this task for unrolling blocks. If the amount is not specified, remove
   * the current task's allocation altogether.
   */
  def releaseUnrollMemoryForThisTask(
      memoryMode: MemoryMode,
      memory: Long = Long.MaxValue
  ): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    memoryManager.synchronized {
      val unrollMemoryMap = memoryMode match {
        case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
        case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
      }
      if (unrollMemoryMap.contains(taskAttemptId)) {
        val memoryToRelease = math.min(memory, unrollMemoryMap(taskAttemptId))
        if (memoryToRelease > 0) {
          unrollMemoryMap(taskAttemptId) -= memoryToRelease
          memoryManager.releaseUnrollMemory(memoryToRelease, memoryMode)
        }
        if (unrollMemoryMap(taskAttemptId) == 0) {
          unrollMemoryMap.remove(taskAttemptId)
        }
      }
    }
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks across all tasks.
   */
  def currentUnrollMemory: Long = memoryManager.synchronized {
    onHeapUnrollMemoryMap.values.sum + offHeapUnrollMemoryMap.values.sum
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks by this task.
   */
  def currentUnrollMemoryForThisTask: Long = memoryManager.synchronized {
    onHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L) +
      offHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L)
  }

  /**
   * Return the number of tasks currently unrolling blocks.
   */
  private def numTasksUnrolling: Int = memoryManager.synchronized {
    (onHeapUnrollMemoryMap.keys ++ offHeapUnrollMemoryMap.keys).toSet.size
  }

  /**
   * Log information about current memory usage.
   */
  private def logMemoryUsage(): Unit = {
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemoryUsed)} (blocks) + " +
        s"${Utils.bytesToString(currentUnrollMemory)} (scratch space shared across " +
        s"$numTasksUnrolling tasks(s)) = ${Utils.bytesToString(memoryUsed)}. " +
        s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
  }

  /**
   * Log a warning for failing to unroll a block.
   *
   * @param blockId
   *   ID of the block we are trying to unroll.
   * @param finalVectorSize
   *   Final size of the vector before unrolling failed.
   */
  private def logUnrollFailureMessage(
      blockId: BlockId,
      finalVectorSize: Long
  ): Unit = {
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
        s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }
}

private trait MemoryEntryBuilder[T] {
  def preciseSize: Long
  def build(): MemoryEntry[T]
}

private trait ValuesHolder[T] {
  def storeValue(value: T): Unit
  def estimatedSize(): Long

  /**
   * Note: After this method is called, the ValuesHolder is invalid, we can't store data and get
   * estimate size again.
   * @return
   *   a MemoryEntryBuilder which is used to build a memory entry and get the stored data size.
   */
  def getBuilder(): MemoryEntryBuilder[T]
}

/**
 * A holder for storing the deserialized values.
 */
private class DeserializedValuesHolder[T](
    classTag: ClassTag[T],
    memoryMode: MemoryMode
) extends ValuesHolder[T] {
  // Underlying vector for unrolling the block
  var vector = new SizeTrackingVector[T]()(classTag)
  var arrayValues: Array[T] = null

  override def storeValue(value: T): Unit = {
    vector += value
  }

  override def estimatedSize(): Long = {
    vector.estimateSize()
  }

  override def getBuilder(): MemoryEntryBuilder[T] = new MemoryEntryBuilder[T] {
    // We successfully unrolled the entirety of this block
    arrayValues = vector.toArray
    vector = null

    override val preciseSize: Long = SizeEstimator.estimate(arrayValues)

    override def build(): MemoryEntry[T] =
      DeserializedMemoryEntry[T](arrayValues, preciseSize, memoryMode, classTag)
  }
}

/**
 * A holder for storing the serialized values.
 */
private class SerializedValuesHolder[T](
    blockId: BlockId,
    chunkSize: Int,
    classTag: ClassTag[T],
    memoryMode: MemoryMode,
    serializerManager: SerializerManager
) extends ValuesHolder[T] {
  val allocator = memoryMode match {
    case MemoryMode.ON_HEAP => ByteBuffer.allocate _
    case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
  }

  val redirectableStream = new RedirectableOutputStream
  val bbos = new ChunkedByteBufferOutputStream(chunkSize, allocator)
  redirectableStream.setOutputStream(bbos)
  val serializationStream: SerializationStream = {
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val ser = serializerManager.getSerializer(classTag, autoPick).newInstance()
    ser.serializeStream(
      serializerManager.wrapForCompression(blockId, redirectableStream)
    )
  }

  override def storeValue(value: T): Unit = {
    serializationStream.writeObject(value)(classTag)
  }

  override def estimatedSize(): Long = {
    bbos.size
  }

  override def getBuilder(): MemoryEntryBuilder[T] = new MemoryEntryBuilder[T] {
    // We successfully unrolled the entirety of this block
    serializationStream.close()

    override def preciseSize(): Long = bbos.size

    override def build(): MemoryEntry[T] =
      SerializedMemoryEntry[T](bbos.toChunkedByteBuffer, memoryMode, classTag)
  }
}

/**
 * The result of a failed [[MemoryStore.putIteratorAsValues()]] call.
 *
 * @param memoryStore
 *   the memoryStore, used for freeing memory.
 * @param memoryMode
 *   the memory mode (on- or off-heap).
 * @param unrollMemory
 *   the amount of unroll memory used by the values in `unrolled`.
 * @param unrolled
 *   an iterator for the partially-unrolled values.
 * @param rest
 *   the rest of the original iterator passed to [[MemoryStore.putIteratorAsValues()]].
 */
private[storage] class PartiallyUnrolledIterator[T](
    memoryStore: MemoryStore,
    memoryMode: MemoryMode,
    unrollMemory: Long,
    private[this] var unrolled: Iterator[T],
    rest: Iterator[T]
) extends Iterator[T] {

  private def releaseUnrollMemory(): Unit = {
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
    // SPARK-17503: Garbage collects the unrolling memory before the life end of
    // PartiallyUnrolledIterator.
    unrolled = null
  }

  override def hasNext: Boolean = {
    if (unrolled == null) {
      rest.hasNext
    } else if (!unrolled.hasNext) {
      releaseUnrollMemory()
      rest.hasNext
    } else {
      true
    }
  }

  override def next(): T = {
    if (unrolled == null || !unrolled.hasNext) {
      rest.next()
    } else {
      unrolled.next()
    }
  }

  /**
   * Called to dispose of this iterator and free its memory.
   */
  def close(): Unit = {
    if (unrolled != null) {
      releaseUnrollMemory()
    }
  }
}

/**
 * A wrapper which allows an open [[OutputStream]] to be redirected to a different sink.
 */
private[storage] class RedirectableOutputStream extends OutputStream {
  private[this] var os: OutputStream = _
  def setOutputStream(s: OutputStream): Unit = { os = s }
  override def write(b: Int): Unit = os.write(b)
  override def write(b: Array[Byte]): Unit = os.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit =
    os.write(b, off, len)
  override def flush(): Unit = os.flush()
  override def close(): Unit = os.close()
}

/**
 * The result of a failed [[MemoryStore.putIteratorAsBytes()]] call.
 *
 * @param memoryStore
 *   the MemoryStore, used for freeing memory.
 * @param serializerManager
 *   the SerializerManager, used for deserializing values.
 * @param blockId
 *   the block id.
 * @param serializationStream
 *   a serialization stream which writes to [[redirectableOutputStream]].
 * @param redirectableOutputStream
 *   an OutputStream which can be redirected to a different sink.
 * @param unrollMemory
 *   the amount of unroll memory used by the values in `unrolled`.
 * @param memoryMode
 *   whether the unroll memory is on- or off-heap
 * @param bbos
 *   byte buffer output stream containing the partially-serialized values.
 *   [[redirectableOutputStream]] initially points to this output stream.
 * @param rest
 *   the rest of the original iterator passed to [[MemoryStore.putIteratorAsValues()]].
 * @param classTag
 *   the [[ClassTag]] for the block.
 */
private[storage] class PartiallySerializedBlock[T](
    memoryStore: MemoryStore,
    serializerManager: SerializerManager,
    blockId: BlockId,
    private val serializationStream: SerializationStream,
    private val redirectableOutputStream: RedirectableOutputStream,
    val unrollMemory: Long,
    memoryMode: MemoryMode,
    bbos: ChunkedByteBufferOutputStream,
    rest: Iterator[T],
    classTag: ClassTag[T]
) {

  private lazy val unrolledBuffer: ChunkedByteBuffer = {
    bbos.close()
    bbos.toChunkedByteBuffer
  }

  // If the task does not fully consume `valuesIterator` or otherwise fails to consume or dispose of
  // this PartiallySerializedBlock then we risk leaking of direct buffers, so we use a task
  // completion listener here in order to ensure that `unrolled.dispose()` is called at least once.
  // The dispose() method is idempotent, so it's safe to call it unconditionally.
  Option(TaskContext.get()).foreach { taskContext =>
    taskContext.addTaskCompletionListener[Unit] { _ =>
      // When a task completes, its unroll memory will automatically be freed. Thus we do not call
      // releaseUnrollMemoryForThisTask() here because we want to avoid double-freeing.
      unrolledBuffer.dispose()
    }
  }

  // Exposed for testing
  private[storage] def getUnrolledChunkedByteBuffer: ChunkedByteBuffer =
    unrolledBuffer

  private[this] var discarded = false
  private[this] var consumed = false

  private def verifyNotConsumedAndNotDiscarded(): Unit = {
    if (consumed) {
      throw new IllegalStateException(
        "Can only call one of finishWritingToStream() or valuesIterator() and can only call once."
      )
    }
    if (discarded) {
      throw new IllegalStateException(
        "Cannot call methods on a discarded PartiallySerializedBlock"
      )
    }
  }

  /**
   * Called to dispose of this block and free its memory.
   */
  def discard(): Unit = {
    if (!discarded) {
      try {
        // We want to close the output stream in order to free any resources associated with the
        // serializer itself (such as Kryo's internal buffers). close() might cause data to be
        // written, so redirect the output stream to discard that data.
        redirectableOutputStream.setOutputStream(ByteStreams.nullOutputStream())
        serializationStream.close()
      } finally {
        discarded = true
        unrolledBuffer.dispose()
        memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
      }
    }
  }

  /**
   * Finish writing this block to the given output stream by first writing the serialized values and
   * then serializing the values from the original input iterator.
   */
  def finishWritingToStream(os: OutputStream): Unit = {
    verifyNotConsumedAndNotDiscarded()
    consumed = true
    // `unrolled`'s underlying buffers will be freed once this input stream is fully read:
    ByteStreams.copy(unrolledBuffer.toInputStream(dispose = true), os)
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
    redirectableOutputStream.setOutputStream(os)
    while (rest.hasNext) {
      serializationStream.writeObject(rest.next())(classTag)
    }
    serializationStream.close()
  }

  /**
   * Returns an iterator over the values in this block by first deserializing the serialized values
   * and then consuming the rest of the original input iterator.
   *
   * If the caller does not plan to fully consume the resulting iterator then they must call
   * `close()` on it to free its resources.
   */
  def valuesIterator: PartiallyUnrolledIterator[T] = {
    verifyNotConsumedAndNotDiscarded()
    consumed = true
    // Close the serialization stream so that the serializer's internal buffers are freed and any
    // "end-of-stream" markers can be written out so that `unrolled` is a valid serialized stream.
    serializationStream.close()
    // `unrolled`'s underlying buffers will be freed once this input stream is fully read:
    val unrolledIter = serializerManager.dataDeserializeStream(
      blockId,
      unrolledBuffer.toInputStream(dispose = true)
    )(classTag)
    // The unroll memory will be freed once `unrolledIter` is fully consumed in
    // PartiallyUnrolledIterator. If the iterator is not consumed by the end of the task then any
    // extra unroll memory will automatically be freed by a `finally` block in `Task`.
    new PartiallyUnrolledIterator(
      memoryStore,
      memoryMode,
      unrollMemory,
      unrolled = unrolledIter,
      rest = rest
    )
  }
}
