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

// scalastyle:off println
package org.apache.spark.examples

/**
 * Transitive closure on a graph.
 */
import org.apache.spark.sql.SparkSession

object SparkWordCountCacheAll {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: SparkWordCount <file> <AppName>")
      System.exit(1)
    }
    val spark = SparkSession
      .builder
      .appName(args(args.length-1))
      .getOrCreate()
    val sc = spark.sparkContext
    var lines = spark.read.textFile(args(0)).rdd.cache()
    val words = lines.flatMap(s => s.split(" ")).cache()
    val ones = words.map(w => (w, 1)).cache()
    val counts = ones.reduceByKey(_ + _).cache()
    val out = counts.collect()
    spark.stop()
  }
}
// scalastyle:on println
