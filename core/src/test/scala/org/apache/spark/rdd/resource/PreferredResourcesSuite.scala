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

package org.apache.spark.rdd.resource

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.scheduler.ResourceInformation

class PreferredResourcesSuite extends SparkFunSuite with SharedSparkContext {

  test("basic preferred resources usage") {
    val rdd1 = sc.parallelize(1 to 10, 2)
      .withResources()
      .prefer("/fpga/*", 2, "/gpu/k80")
      .require("/cpu", 1)
      .build()

    val partitions = rdd1.partitions
    val preferredResources = partitions.map(rdd1.getPreferredResources)

    assert(preferredResources.length == 2)

    val res1 = preferredResources(0).asMap()
    val res2 = preferredResources(1).asMap()
    assert(res1 === res2)
    assert(res1("/fpga") === ((2, Some("/gpu/k80"))))
    assert(res1("/cpu") === ((1, None)))

    rdd1.clearResources()
    val clearedResources = partitions.map(rdd1.getPreferredResources)
    assert(clearedResources(0) === PreferredResources.EMPTY)
  }

  test("handle conflicted resources") {
    val res1 = new PreferredResources(Map("/gpu" -> ((1, Some("/cpu"))), "/cpu" -> ((2, None))))
    val res2 = res1
    val merged1 = res1.mergeOther(res2)
    assert(merged1 === res1 && merged1 === res2)

    val res3 = new PreferredResources(Map("/gpu" -> ((1, Some("/cpu")))))
    assert(res3.mergeOther(PreferredResources.EMPTY) === res3)
    assert(PreferredResources.EMPTY.mergeOther(res3) === res3)
    assert(
      PreferredResources.EMPTY.mergeOther(PreferredResources.EMPTY) === PreferredResources.EMPTY)

    val res4 = new PreferredResources(Map("/gpu" -> ((1, Some("/cpu")))))
    val res5 = new PreferredResources(Map("/fpga" -> ((1, Some("/gpu")))))
    intercept[IllegalStateException](res4.mergeOther(res5))
  }

  test("chained resource preferences") {
    // resource information will be derived in partition-wise within one stage
    val rdd1 = sc.parallelize(1 to 10, 2)
      .withResources()
      .require("/gpu/*", 2)
      .build()
      .map(_ + 1)
    val preferredResources = rdd1.partitions.map(rdd1.getPreferredResources)
    assert(preferredResources(0).asMap() === Map("/gpu" -> ((2, None))))

    // resource information will not be derived between stages.
    val shuffledRdd = rdd1.map(i => (i, i)).reduceByKey(_ + _)
      .withResources().prefer("/gpu/k80/*", 2, "/gpu/*").build()
    val resForShuffledRdd = shuffledRdd.partitions.map(shuffledRdd.getPreferredResources)
    assert(resForShuffledRdd(0).asMap() === Map("/gpu/k80" -> ((2, Some("/gpu")))))

    // unioned RDDs will have different resource preferences derived from different parent RDDs.
    val rdd2 = sc.parallelize(1 to 10, 2)
    val unionRdd = rdd1.union(rdd2)
    val resForUnionRdd = unionRdd.partitions.map(unionRdd.getPreferredResources)
    assert(resForUnionRdd.length == 4)
    assert(resForUnionRdd.toSet.size == 2)
    resForUnionRdd.toSet === Set(
      new PreferredResources(Map("/gpu" -> ((2, None)))), PreferredResources.EMPTY)

    // cartesian RDDs will merge resource preferences from parent RDDs.
    val rdd3 = sc.parallelize(1 to 10, 2)
      .withResources()
      .require("/fpga/*", 2)
      .build()
    val cartesianRdd = rdd1.cartesian(rdd3)
    // merge will be failed because rdd1 and rdd3 has different resource preferences.
    intercept[IllegalStateException](
      cartesianRdd.partitions.map(cartesianRdd.getPreferredResources))
  }

  test("preferred resources is satisfied") {
    val resArray = Array(
      ResourceInformation("/gpu/k80"),
      ResourceInformation("/gpu/p100"),
      ResourceInformation("/fpga"))

    // The provided resources can satisfy required needs.
    val preferredRes1 = new PreferredResources(Map(
      "/gpu/k80" -> ((1, None))
    ))
    assert(preferredRes1.isSatisfied(resArray))
    val occupiedRes1 = resArray.filter(_.occupiedByTask == ResourceInformation.RESERVED)
    assert(occupiedRes1.length == 1)
    assert(occupiedRes1(0).tpe == "/gpu/k80")
    resArray.foreach(_.occupiedByTask = ResourceInformation.UNUSED)

    // The provided resources can satisfy the optional needs.
    val preferredRes2 = new PreferredResources(Map(
      "/gpu/p1000" -> ((1, Some("/gpu")))
    ))
    assert(preferredRes2.isSatisfied(resArray))
    val occupiedRes2 = resArray.filter(_.occupiedByTask == ResourceInformation.RESERVED)
    assert(occupiedRes2.length == 1)
    assert(occupiedRes2(0).tpe.startsWith("/gpu"))
    resArray.foreach(_.occupiedByTask = ResourceInformation.UNUSED)

    // The provided resources can satisfy the optional needs.
    val preferredRes3 = new PreferredResources(Map(
      "/gpu/p1000" -> ((2, Some("/gpu")))
    ))
    assert(preferredRes3.isSatisfied(resArray))
    val occupiedRes3 = resArray.filter(_.occupiedByTask == ResourceInformation.RESERVED)
    assert(occupiedRes3.length == 2)
    assert(occupiedRes3.forall(_.tpe.startsWith("/gpu")))
    resArray.foreach(_.occupiedByTask = ResourceInformation.UNUSED)

    // The provided resources cannot satisfy the needs.
    val preferredRes4 = new PreferredResources(Map(
      "/gpu/k80" -> ((3, None))
    ))
    assert(!preferredRes4.isSatisfied(resArray))
    val occupiedRes4 = resArray.filter(_.occupiedByTask == ResourceInformation.RESERVED)
    assert(occupiedRes4.length == 0)
    resArray.foreach(_.occupiedByTask = ResourceInformation.UNUSED)
  }
}
