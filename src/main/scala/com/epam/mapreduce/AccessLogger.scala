package com.epam.mapreduce

import com.epam.mapreduce.counters.Counters
import com.epam.mapreduce.types.{RequestSize, Size}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

object AccessLogger {
  def main(args: Array[String]): Unit = {
    val configuration = new Configuration

    configuration.set("mapreduce.output.textoutputformat.separator", ",")

    val job = Job.getInstance(configuration, "Access logger")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[LogsMapper])
    job.setCombinerClass(classOf[LogsCombiner])
    job.setReducerClass(classOf[LogsReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[RequestSize])
    job.setOutputValueClass(classOf[Size])

    job.setInputFormatClass(classOf[TextInputFormat])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Size]])
    job.waitForCompletion(true)

    println("User browsers")
    job.getCounters.getGroup(Counters.groupName).forEach(c => println(c.getName + " = " + c.getValue))
  }
}
