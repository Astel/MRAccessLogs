package com.epam.mapreduce

import com.epam.mapreduce.types.RequestSize
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Counter
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar

class MapperTest extends FlatSpec with MockitoSugar {

  it should "output should be correct" in {
    val mapper = new LogsMapper
    val context = mock[mapper.Context]
    val counter = mock[Counter]
    when(context.getCounter(anyString, anyString))
      .thenReturn(counter)

    mapper.map(
      key = null,
      value = new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 - \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"\n" +
        "ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 10000 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"\n" +
        "ip1525 - - [24/Apr/2011:04:14:36 -0400] \"GET /~strabal/grease/photo9/927-5.jpg HTTP/1.1\" 200 10000 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"\n" +
        "ip1 - - [24/Apr/2011:04:18:54 -0400] \"GET /~strabal/grease/photo1/T97-4.jpg HTTP/1.1\" 404 20000 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""),
      context
    )

    verify(context, times(4)).write(any[Text], any[RequestSize])
  }

}