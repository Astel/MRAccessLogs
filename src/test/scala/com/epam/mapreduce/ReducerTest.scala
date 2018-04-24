package com.epam.mapreduce

import com.epam.mapreduce.types.{RequestSize, Size}
import org.apache.hadoop.io.{IntWritable, Text}
import org.mockito.Mockito.verify
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class ReducerTest extends FlatSpec with MockitoSugar {
  it should "output the ip, average and total" in {
    val reducer = new LogsReducer
    val context = mock[reducer.Context]

    reducer.reduce(
      key = new Text("ip1"),
      values = Seq(new RequestSize(10000, 1), new RequestSize(10000, 2), new RequestSize(0, 1)).asJava,
      context
    )

    verify(context).write(new Text("ip1"), new Size(5000, 20000))
  }
}
