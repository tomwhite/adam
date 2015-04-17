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

import java.io._

import org.bdgenomics.adam.util.{ ADAMFunSuite, HadoopUtil }
import org.bdgenomics.formats.avro.Genotype

class PartitionSuite extends ADAMFunSuite {

  sparkTest("can partition the ADAM equivalent of a simple VCF file") {

    val loader = Thread.currentThread().getContextClassLoader
    val partitionStrategyPath = loader.getResource("genotypes-partition-strategy.json")
    val inputPath = loader.getResource("small.vcf").getPath
    val outputFile = File.createTempFile("adam-cli.PartitionSuite", ".adam")
    val outputPath = outputFile.getAbsolutePath
    val partitionFile = File.createTempFile("partitioned", "")
    val partitionPath = partitionFile.getAbsolutePath

    assert(outputFile.delete(), "Couldn't delete (empty) temp file")
    assert(partitionFile.delete(), "Couldn't delete (empty) temp file")

    val argLine = "%s %s".format(inputPath, outputPath).split("\\s+")
    val args: Vcf2ADAMArgs = Args4j.apply[Vcf2ADAMArgs](argLine)
    val vcf2Adam = new Vcf2ADAM(args)
    val job = HadoopUtil.newJob()
    vcf2Adam.run(sc, job)

    val lister = new ParquetLister[Genotype]()
    val records = lister.materialize(outputPath).toSeq

    assert(records.size === 15)
    assert(records(0).getSampleId === "NA12878")
    assert(records(0).getVariant.getStart == 14396L)
    assert(records(0).getVariant.getEnd == 14400L)

    val partitionArgLine = "-partition_strategy_file %s %s file:%s"
      .format(partitionStrategyPath, outputPath, partitionPath).split("\\s+")
    val partitionArgs: PartitionArgs = Args4j.apply[PartitionArgs](partitionArgLine)
    val partition = new Partition(partitionArgs)
    val partitionJob = HadoopUtil.newJob()
    partition.run(sc, partitionJob)

    {
      val flatLister = new ParquetLister[Genotype]()
      val part = flatLister.materialize(partitionPath + "/chr=1/pos=1").toSeq
      assert(part.size === 9)
      assert(part(0).getSampleId === "NA12878")
      assert(part(0).getVariant.getStart == 14396L)
      assert(part(0).getVariant.getEnd == 14400L)
    }

    {
      val flatLister = new ParquetLister[Genotype]()
      val part = flatLister.materialize(partitionPath + "/chr=1/pos=6").toSeq
      assert(part.size === 3)
      assert(part(0).getSampleId === "NA12878")
      assert(part(0).getVariant.getStart == 63734L)
      assert(part(0).getVariant.getEnd == 63738L)
    }

    {
      val flatLister = new ParquetLister[Genotype]()
      val part = flatLister.materialize(partitionPath + "/chr=1/pos=75").toSeq
      assert(part.size === 3)
      assert(part(0).getSampleId === "NA12878")
      assert(part(0).getVariant.getStart == 752720L)
      assert(part(0).getVariant.getEnd == 752721L)
    }

  }

}
