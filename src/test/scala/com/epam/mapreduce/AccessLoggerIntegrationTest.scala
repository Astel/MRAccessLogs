package com.epam.mapreduce

import java.io.File

import com.epam.mapreduce.types.{RequestSize, Size}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.{IntWritable, SequenceFile, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

import scala.io.Source

class AccessLoggerIntegrationTest extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  it should "write data to output folder" in {
    AccessLogger.main(Array("input", "output"))
    val outputFile = "output/part-r-00000"
    val output = Source.fromFile(new File(outputFile)).getLines().reduce((f, s) => f + "\n" + s)

    output shouldEqual "ip1,2500.0,10000"
  }

  it should "correct use combiner" in {
    val configuration = new Configuration
    configuration.set("mapreduce.output.textoutputformat.separator", ",")

    val job = Job.getInstance(configuration,"word count")
    job.setJarByClass(AccessLogger.getClass)
    job.setMapperClass(classOf[LogsMapper])
    job.setCombinerClass(classOf[LogsCombiner])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[RequestSize])

    FileInputFormat.addInputPath(job, new Path("input"))
    FileOutputFormat.setOutputPath(job, new Path("output"))

    val result = job.waitForCompletion(true)

    val outputFile = "output/part-r-00000"
    val output = Source.fromFile(new File(outputFile)).getLines().reduce((f, s) => f + "\n" + s)

    output shouldEqual "ip1,10000,4"
  }

  override def beforeAll(): Unit = {
    System.setProperty("hadoop.home.dir", "/")
    clearFileSystem
  }

  override def beforeEach(): Unit = {
    val fs = FileSystem.get(new Configuration())
    val inputPath = new Path(getClass.getResource("/000000").getPath)
    fs.copyFromLocalFile(inputPath, new Path("input"))
  }

  override def afterEach: Unit = {
    clearFileSystem
  }

  private def clearFileSystem = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path("output"), true)
    fs.delete(new Path("input"), true)
  }
}
