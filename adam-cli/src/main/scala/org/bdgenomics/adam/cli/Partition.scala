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

import java.io.File

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.MetricsContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.HadoopUtil
import org.bdgenomics.utils.instrumentation.Metrics
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat
import org.kitesdk.data.{ DatasetDescriptor, Datasets, Formats }
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }
import parquet.avro.AvroParquetInputFormat
import parquet.hadoop.util.ContextUtil

object Partition extends ADAMCommandCompanion {
  val commandName = "partition"
  val commandDescription = "Partition an ADAM format file into Hive-compatible partitions"

  def apply(cmdLine: Array[String]) = {
    new Partition(Args4j[PartitionArgs](cmdLine))
  }
}

class PartitionArgs extends Args4jBase with ParquetSaveArgs {
  @Args4jOption(required = true, name = "-partition_strategy_file",
    usage = "A JSON file containing the partition strategy")
  var partitionStrategyFile: File = _
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM file to partition",
    index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write the " +
    "partitioned files to", index = 1)
  var outputPath: String = null
}

class Partition(val args: PartitionArgs) extends ADAMSparkCommand[PartitionArgs]
    with Logging {
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

    val desc = new DatasetDescriptor.Builder()
      .schema(schema)
      .partitionStrategy(args.partitionStrategyFile)
      .format(Formats.PARQUET)
      .build
    val dataset: org.kitesdk.data.View[IndexedRecord] =
      Datasets.create("dataset:" + args.outputPath, desc, classOf[IndexedRecord])
    DatasetKeyOutputFormat.configure(job).writeTo(dataset)

    records.map(r => r.swap)
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}