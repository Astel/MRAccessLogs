package com.epam

import eu.bitwalker.useragentutils.UserAgent
import org.apache.hadoop.io.Text

package object mapreduce {
  def getBrowserNameFromLog(log: String): String = {
    val userAgentPattern = "[\"][^\"]+[\"]".r
    val seqQuotes = userAgentPattern.findAllMatchIn(log).length

    val userAgentString = userAgentPattern
      .findAllMatchIn(log)
      .drop(seqQuotes - 1)
      .next
      .toString()
      .replaceAll("\"","")

      UserAgent.parseUserAgentString(userAgentString).getBrowser.getGroup.getName
  }

  def getIpFromLog(log: String): String = {
    val ip = "ip[0-9]+?".r
    ip.findFirstIn(log) match {
      case Some(s) => s
      case None    => throw new IllegalArgumentException("Can't match ip")
    }
  }

  def getSizeFromLog(log: String): Int = {
    val sizePattern = " [0-9]{3} [0-9]+".r
    sizePattern.findFirstIn(log) match {
      case Some(s) => s.substring(5).toInt
      case None    => 0
    }
  }
}
