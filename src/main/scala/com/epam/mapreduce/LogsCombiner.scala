package com.epam.mapreduce

import java.lang.Iterable

import com.epam.mapreduce.types.RequestSize
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters._

class LogsCombiner extends Reducer[Text, RequestSize, Text, RequestSize] {
  override def reduce(
      key: Text,
      values: Iterable[RequestSize],
      context: Reducer[Text, RequestSize, Text, RequestSize]#Context): Unit = {
    val sum = values.asScala.foldLeft(new RequestSize(0, 0))((l, r) => {
      new RequestSize(l.getSize + r.getSize, l.getCount + r.getCount)
    })
    context.write(key, sum)
  }
}
