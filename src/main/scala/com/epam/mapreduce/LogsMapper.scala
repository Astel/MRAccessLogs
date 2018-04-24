package com.epam.mapreduce

import com.epam.mapreduce.counters.Counters
import com.epam.mapreduce.types.RequestSize
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class LogsMapper extends Mapper[LongWritable, Text, Text, RequestSize] {
  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, RequestSize]#Context): Unit = {

    for (log <- value.toString.split("\n")) {
      //find browser name and increment counter
      val browser = getBrowserNameFromLog(log)
      context.getCounter(Counters.groupName, browser).increment(1)

      val key = getIpFromLog(log)
      val size = getSizeFromLog(log)

      context.write(new Text(key), new RequestSize(size, 1))
    }
  }
}
