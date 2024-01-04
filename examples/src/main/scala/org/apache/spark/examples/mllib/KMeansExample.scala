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
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
// import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
// $example off$

object KMeansExample {

  def main(args: Array[String]): Unit = {

    // Kmeans <input> <clusters> <iters> <appName>

    if (args.length != 2) {
      println("KMeansExample <input> <clusters> <iters> <appName>")
    }

    val conf = new SparkConf().setAppName(args(args.length-1))
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data
    // val data = sc.textFile("data/mllib/kmeans_data.txt")
    val data = sc.textFile(args(0))
    val parsedData = data
      .filter(_.nonEmpty)
      .map(s => Vectors.dense(s.split("\\s+").map(_.toDouble)))
      .cache()

    // Cluster the data into two classes using KMeans
    val numClusters = args(1).toInt
    val numIterations = args(2).toInt
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Save and load model
    // clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    // val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
