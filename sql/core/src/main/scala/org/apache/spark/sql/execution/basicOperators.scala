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

package org.apache.spark.sql.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{PartitionwiseSampledRDD, RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator
import org.apache.spark.util.random.PoissonSampler
import org.apache.spark.util.{BoundedPriorityQueue, Stats, CompletionIterator, MutablePair}
import org.apache.spark.{util, HashPartitioner, SparkEnv}

import scala.collection.AbstractIterator
import scala.collection.Iterator._
import scala.collection.parallel.mutable

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Project(projectList: Seq[NamedExpression], child: SparkPlan) extends UnaryNode {
  //zengdan
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override private[sql] lazy val metrics = Map(
      "numRows" -> SQLMetrics.createLongMetric(sparkContext, "number of rows"))

  @transient lazy val buildProjection = newMutableProjection(projectList, child.output)

  protected override def doExecute(): RDD[InternalRow] = {
    val numRows = longMetric("numRows")
    child.execute().mapPartitions { iter =>
        val reusableProjection = buildProjection()
        //zengdan iter.map => mapWithReuse
        mapWithReuse(iter, { row: InternalRow =>
          numRows += 1
          reusableProjection(row)
        })
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}


/**
 * A variant of [[Project]] that returns [[UnsafeRow]]s.
 */
case class TungstenProject(projectList: Seq[NamedExpression], child: SparkPlan) extends UnaryNode {

  override private[sql] lazy val metrics = Map(
    "numRows" -> SQLMetrics.createLongMetric(sparkContext, "number of rows"))

  override def outputsUnsafeRows: Boolean = true
  override def canProcessUnsafeRows: Boolean = true
  override def canProcessSafeRows: Boolean = true

  //zengdan
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  /** Rewrite the project list to use unsafe expressions as needed. */
  protected val unsafeProjectList = projectList.map(_ transform {
    case CreateStruct(children) => CreateStructUnsafe(children)
    case CreateNamedStruct(children) => CreateNamedStructUnsafe(children)
  })

  protected override def doExecute(): RDD[InternalRow] = {
    val numRows = longMetric("numRows")
    child.execute().mapPartitions { iter =>
      val project = UnsafeProjection.create(unsafeProjectList, child.output)
      //zengdan iter.map => mapWithReuse
      mapWithReuse[InternalRow](iter, { row =>
        numRows += 1
        project(row)
      })
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}


/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Filter(condition: Expression, child: SparkPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  private[sql] override lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitions { iter =>
        val predicate = newPredicate(condition, child.output)
        //zengdan iter.filter => filterWithReuse
        filterWithReuse(iter, { row =>
          numInputRows += 1
          val r = predicate(row)
          if (r) numOutputRows += 1
          r
        })
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputsUnsafeRows: Boolean = child.outputsUnsafeRows

  override def canProcessUnsafeRows: Boolean = true

  override def canProcessSafeRows: Boolean = true
}

/**
 * :: DeveloperApi ::
 * Sample the dataset.
 * @param lowerBound Lower-bound of the sampling probability (usually 0.0)
 * @param upperBound Upper-bound of the sampling probability. The expected fraction sampled
 *                   will be ub - lb.
 * @param withReplacement Whether to sample with replacement.
 * @param seed the random seed
 * @param child the QueryPlan
 */
@DeveloperApi
case class Sample(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long,
    child: SparkPlan)
  extends UnaryNode
{
  override def output: Seq[Attribute] = child.output

  override def outputsUnsafeRows: Boolean = child.outputsUnsafeRows
  override def canProcessUnsafeRows: Boolean = true
  override def canProcessSafeRows: Boolean = true

  protected override def doExecute(): RDD[InternalRow] = {
    if (withReplacement) {
      // Disable gap sampling since the gap sampling method buffers two rows internally,
      // requiring us to copy the row, which is more expensive than the random number generator.
      new PartitionwiseSampledRDD[InternalRow, InternalRow](
        child.execute(),
        new PoissonSampler[InternalRow](upperBound - lowerBound, useGapSamplingIfPossible = false),
        preservesPartitioning = true,
        seed)
    } else {
      child.execute().randomSampleWithRange(lowerBound, upperBound, seed)
    }
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Union(children: Seq[SparkPlan]) extends SparkPlan {
  // TODO: attributes output by union should be distinct for nullability purposes
  override def output: Seq[Attribute] = children.head.output
  override def outputsUnsafeRows: Boolean = children.forall(_.outputsUnsafeRows)
  override def canProcessUnsafeRows: Boolean = true
  override def canProcessSafeRows: Boolean = true
  protected override def doExecute(): RDD[InternalRow] = {
    val rdd = sparkContext.union(children.map(_.execute()))
    rdd.mapPartitions(iter => mapWithReuse[InternalRow](iter,row => row)) //zengdan
  }
}

/**
 * :: DeveloperApi ::
 * Take the first limit elements. Note that the implementation is different depending on whether
 * this is a terminal operator or not. If it is terminal and is invoked using executeCollect,
 * this operator uses something similar to Spark's take method on the Spark driver. If it is not
 * terminal or is invoked using execute, we first take the limit on each partition, and then
 * repartition all the data to a single partition to compute the global limit.
 */
@DeveloperApi
case class Limit(limit: Int, child: SparkPlan)
  extends UnaryNode {
  // TODO: Implement a partition local limit, and use a strategy to generate the proper limit plan:
  // partition local limit -> exchange into one partition -> partition local limit again

  override def operatorMatch(plan: SparkPlan):Boolean = plan match{
    case lm: Limit => this.limit == lm.limit
    case _ => false
  }

  /** We must copy rows when sort based shuffle is on */
  private def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = SinglePartition

  override def executeCollect(): Array[Row] = child.executeTake(limit)

  protected override def doExecute(): RDD[InternalRow] = {
    val rdd: RDD[_ <: Product2[Boolean, InternalRow]] = if (sortBasedShuffleOn) {
      child.execute().mapPartitions { iter =>
        //iter.take(limit).map(row => row.copy())
        val shouldCollect = nodeRef.isDefined && nodeRef.get.collect
        if (shouldCollect) {
          var start: Long = 0

          new Iterator[Product2[Boolean, InternalRow]] {
            private var remaining = limit

            override def hasNext = {
              start = System.nanoTime()
              val has = remaining > 0 && iter.hasNext
              time += (System.nanoTime() - start)
              if (!has) {
                logDebug(s"Limit before Shuffle ${nodeRef.get.id}: $time, $rowCount, $avgSize")
                val materializationTime = Stats.statistics.get.get(0).getOrElse(Array(0))(0)
                //生成iterator的时间
                val initializeTime = Stats.initialTimes.get.get(nodeRef.get.id).get
                Stats.statistics.get.put(nodeRef.get.id,
                  Array((time / 1e6).toInt + initializeTime + materializationTime, 0))
              }
              has
            }

            override def next = {
              start = System.nanoTime()
              val takeRet = if (remaining > 0) {
                remaining -= 1
                iter.next()
              }
              else empty.next()
              val result = (false, takeRet.copy())
              time += (System.nanoTime() - start)
              result
            }
          }
        } else {
          iter.take(limit).map(row => (false, row.copy()))
        }
      }
    } else {
      child.execute().mapPartitions { iter =>

        val mutablePair = new MutablePair[Boolean, InternalRow]()
        //iter.take(limit).map(row => mutablePair.update(false, row))
        val shouldCollect = nodeRef.isDefined && nodeRef.get.collect
        if(shouldCollect){
          var start: Long = 0

          new Iterator[Product2[Boolean, InternalRow]]{
            private var remaining = limit

            override def hasNext = {
              start = System.nanoTime()
              val has = remaining > 0 && iter.hasNext
              time += (System.nanoTime() - start)
              if (!has) {
                logDebug(s"Limit before Shuffle ${nodeRef.get.id}: $time, $rowCount, $avgSize")
                val materializationTime = Stats.statistics.get.get(0).getOrElse(Array(0))(0)
                //生成iterator的时间
                val initializeTime = Stats.initialTimes.get.get(nodeRef.get.id).get
                Stats.statistics.get.put(nodeRef.get.id,
                  Array((time / 1e6).toInt + initializeTime + materializationTime, 0))
              }
              has
            }

            override def next = {
              start = System.nanoTime()
              val takeRet = if (remaining > 0) {
                remaining -= 1
                iter.next()
              }
              else empty.next()
              val result = mutablePair.update(false, takeRet)
              time += (System.nanoTime() - start)
              result
            }
          }
        }else{
          iter.take(limit).map(row => mutablePair.update(false, row))
        }
      }
    }

    //zengdan
    if(nodeRef.isDefined && nodeRef.get.collect)
      rdd.collectID = Some(nodeRef.get.id)

    val part = new HashPartitioner(1)
    val shuffled = new ShuffledRDD[Boolean, InternalRow, InternalRow](rdd, part)
    shuffled.setSerializer(new SparkSqlSerializer(child.sqlContext.sparkContext.getConf))
    shuffled.mapPartitions(_.take(limit).map(_._2))
      .mapPartitions{iter => mapWithReuse[InternalRow](iter, {row: InternalRow => row})} //zengdan
  }
}

/**
 * :: DeveloperApi ::
 * Take the first limit elements as defined by the sortOrder, and do projection if needed.
 * This is logically equivalent to having a [[Limit]] operator after a [[Sort]] operator,
 * or having a [[Project]] operator between them.
 * This could have been named TopK, but Spark's top operator does the opposite in ordering
 * so we name it TakeOrdered to avoid confusion.
 */
@DeveloperApi
case class TakeOrderedAndProject(
    limit: Int,
    sortOrder: Seq[SortOrder],
    projectList: Option[Seq[NamedExpression]],
    child: SparkPlan) extends UnaryNode {

  override def operatorMatch(plan: SparkPlan):Boolean = plan match{
    case to: TakeOrderedAndProject =>
      this.limit == to.limit && !sortOrder.zipWithIndex.exists(x => !x._1.equals(to.sortOrder(x._2))) &&
        this.compareOptionExpressions(projectList, to.projectList)
    case _ => false
  }

  def compareOptionExpressions(expr1: Option[Seq[Expression]], expr2: Option[Seq[Expression]]): Boolean = {
    if(expr1.isDefined && expr2.isDefined){
      compareExpressions(expr1.get.map(_.transformExpression()), expr2.get.map(_.transformExpression()))
    }else{
      !expr1.isDefined && !expr2.isDefined
    }
  }


  override def output: Seq[Attribute] = {
    val projectOutput = projectList.map(_.map(_.toAttribute))
    projectOutput.getOrElse(child.output)
  }

  override def outputPartitioning: Partitioning = SinglePartition

  // We need to use an interpreted ordering here because generated orderings cannot be serialized
  // and this ordering needs to be created on the driver in order to be passed into Spark core code.
  private val ord: InterpretedOrdering = new InterpretedOrdering(sortOrder, child.output)

  // TODO: remove @transient after figure out how to clean closure at InsertIntoHiveTable.
  @transient private val projection = projectList.map(new InterpretedProjection(_, child.output))

  private def collectData(): Array[InternalRow] = {
    val childRdd = child.execute().map(_.copy())
    //val data = child.execute().map(_.copy()).takeOrdered(limit)(ord)
    //projection.map(data.map(_)).getOrElse(data)

    val mapRDDs = childRdd.mapPartitions { items =>
      // Priority keeps the largest elements, so let's reverse the ordering.
      val queue = new BoundedPriorityQueue[InternalRow](limit)(ord.reverse)
      val shouldCollect = nodeRef.isDefined && nodeRef.get.collect
      if(shouldCollect){
        val start =  System.nanoTime()
        queue ++= util.collection.Utils.takeOrdered(items, limit)(ord)
        val ret = Iterator.single(queue)
        time += (System.nanoTime() - start)
        val materializationTime = Stats.statistics.get.get(0).getOrElse(Array(0))(0)
        Stats.statistics.get.put(nodeRef.get.id,
          Array((time / 1e6).toInt + materializationTime, avgSize * rowCount))
        Stats.statistics.get.put(0,
          Array((time / 1e6).toInt + materializationTime, 0))

        ret
      }else{
        queue ++= util.collection.Utils.takeOrdered(items, limit)(ord)
        Iterator.single(queue)
      }
    }
    if (mapRDDs.partitions.length == 0) {
      Array.empty
    } else {
      val arr = mapRDDs.reduce { (queue1, queue2) =>
        queue1 ++= queue2
        queue1
      }.toArray//.sorted(ord)
      val shouldCollect = nodeRef.isDefined && nodeRef.get.collect
      if(shouldCollect) {
        val start =  System.nanoTime()
        val data = arr.sorted(ord)
        val ret = projection.map(data.map(_)).getOrElse(data)
        time += (System.nanoTime() - start)
        ret
      } else {
        arr.sorted(ord)
      }
    }
  }

  /*
  zengdan delete it
  override def executeCollect(): Array[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    collectData().map(converter(_).asInstanceOf[Row])
  }
  */

  // TODO: Terminal split should be implemented differently from non-terminal split.
  // TODO: Pick num splits based on |limit|.
  protected override def doExecute(): RDD[InternalRow] =
    sparkContext.makeRDD(collectData(), 1).mapPartitions(iter => mapWithReuse[InternalRow](iter, row => row))//zengdan

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def simpleString: String = {
    val orderByString = sortOrder.mkString("[", ",", "]")
    val outputString = output.mkString("[", ",", "]")

    s"TakeOrderedAndProject(limit=$limit, orderBy=$orderByString, output=$outputString)"
  }
}

/**
 * :: DeveloperApi ::
 * Return a new RDD that has exactly `numPartitions` partitions.
 */
@DeveloperApi
case class Repartition(numPartitions: Int, shuffle: Boolean, child: SparkPlan)
  extends UnaryNode {

  override def operatorMatch(plan: SparkPlan):Boolean = plan match{
    case rp: Repartition => this.numPartitions == rp.numPartitions &&
      this.shuffle == rp.shuffle
    case _ => false
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    if (numPartitions == 1) SinglePartition
    else UnknownPartitioning(numPartitions)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute().map(_.copy()).coalesce(numPartitions, shuffle)
  }
}


/**
 * :: DeveloperApi ::
 * Returns a table with the elements from left that are not in right using
 * the built-in spark subtract function.
 */
@DeveloperApi
case class Except(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output

  protected override def doExecute(): RDD[InternalRow] = {
    left.execute().map(_.copy()).subtract(right.execute().map(_.copy()))
  }
}

/**
 * :: DeveloperApi ::
 * Returns the rows in left that also appear in right using the built in spark
 * intersection function.
 */
@DeveloperApi
case class Intersect(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = children.head.output

  protected override def doExecute(): RDD[InternalRow] = {
    left.execute().map(_.copy()).intersection(right.execute().map(_.copy()))
  }
}

/**
 * :: DeveloperApi ::
 * A plan node that does nothing but lie about the output of its child.  Used to spice a
 * (hopefully structurally equivalent) tree from a different optimization sequence into an already
 * resolved tree.
 */
@DeveloperApi
case class OutputFaker(output: Seq[Attribute], child: SparkPlan) extends SparkPlan {
  def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] =
    child.execute().mapPartitions(iter => mapWithReuse[InternalRow](iter, row => row))
}
