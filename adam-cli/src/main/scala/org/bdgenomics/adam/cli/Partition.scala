/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.cli

import java.io.{ ObjectInputStream, ObjectOutputStream, IOException, File }

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.rdd.MetricsContext._
import org.bdgenomics.adam.util.HadoopUtil
import org.bdgenomics.utils.instrumentation.Metrics
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat
import org.kitesdk.data.spi.{ StorageKey, DataModelUtil, EntityAccessor, PartitionStrategyParser }
import org.kitesdk.data.{ PartitionStrategy, DatasetDescriptor, Datasets, Formats }
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }
import parquet.avro.AvroParquetInputFormat
import parquet.hadoop.util.ContextUtil

import scala.util.control.NonFatal

object Partition extends ADAMCommandCompanion {
  val commandName = "partition"
  val commandDescription = "Partition an ADAM format file into Hive-compatible partitions"

  def apply(cmdLine: Array[String]) = {
    new Partition(Args4j[PartitionArgs](cmdLine))
  }
}

class PartitionArgs extends Args4jBase with ParquetSaveArgs with Serializable {
  @Args4jOption(required = true, name = "-partition_strategy_file",
    usage = "A JSON file containing the partition strategy")
  var partitionStrategyFile: String = _
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM file to partition",
    index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write the " +
    "partitioned files to", index = 1)
  var outputPath: String = null
}

class Partition(val args: PartitionArgs) extends ADAMSparkCommand[PartitionArgs]
    with Logging with Serializable {
  val companion = Partition

  def run(sc: SparkContext, job: Job) {

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(
      args.inputPath,
      classOf[AvroParquetInputFormat],
      classOf[Void],
      classOf[IndexedRecord],
      ContextUtil.getConfiguration(job)
    )
    if (Metrics.isRecording) records.instrument() else records

    val schema: Schema = records.first()._2.getSchema

    val fs = FileSystem.get(sc.hadoopConfiguration)

    val desc = new DatasetDescriptor.Builder()
      .schema(schema)
      .partitionStrategy(fs.open(new Path(args.partitionStrategyFile)))
      .format(Formats.PARQUET)
      .build
    val dataset: org.kitesdk.data.View[IndexedRecord] =
      Datasets.create("dataset:" + args.outputPath, desc, classOf[IndexedRecord])
    DatasetKeyOutputFormat.configure(job).writeTo(dataset)
    records.map(r => r.swap)
      .partitionBy(new KitePartitioner(sc.defaultParallelism, dataset.getType, schema,
        desc.getPartitionStrategy))
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

}

class KitePartitioner(partitions: Int,
                      datasetType: Class[IndexedRecord],
                      @transient var schema: Schema,
                      @transient var partitionStrategy: PartitionStrategy) extends Partitioner {

  @transient var accessor: EntityAccessor[IndexedRecord] =
    DataModelUtil.accessor(datasetType, schema)
  @transient var key: StorageKey = new StorageKey(partitionStrategy)

  override def numPartitions: Int = partitions

  override def getPartition(k: Any): Int = {
    val record = k.asInstanceOf[IndexedRecord]
    accessor.keyFor(record, null, key)
    nonNegativeMod(key.hashCode(), numPartitions)
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    try {
      out.defaultWriteObject()
      out.writeUTF(schema.toString)
      out.writeUTF(partitionStrategy.toString)
    } catch {
      case e: IOException => throw e
      case NonFatal(t)    => throw new IOException(t)
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    try {
      in.defaultReadObject()
      schema = Schema.parse(in.readUTF())
      partitionStrategy = PartitionStrategyParser.parse(in.readUTF())
      accessor = DataModelUtil.accessor(datasetType, schema)
      key = new StorageKey(partitionStrategy)
    } catch {
      case e: IOException => throw e
      case NonFatal(t)    => throw new IOException(t)
    }
  }
}