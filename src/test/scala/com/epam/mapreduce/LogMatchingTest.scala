package com.epam.mapreduce

import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class LogMatchingTest extends FlatSpec with Matchers {
  it should "correct match log string" in {
    val line = "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 2000 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""

    val browser = getBrowserNameFromLog(line)
    browser shouldEqual "Robot/Spider"

    val key = getIpFromLog(line)
    key shouldEqual "ip1"

    val size = getSizeFromLog(line)
    size shouldEqual 2000

    val line2 = "ip1 - - [24/Apr/2011:04:18:54 -0400] \"GET /~strabal/grease/photo1/T97-4.jpg HTTP/1.1\" 404 - \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""
    getSizeFromLog(line2) shouldEqual 0

  }
}
