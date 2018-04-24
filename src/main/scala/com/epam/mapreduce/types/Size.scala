package com.epam.mapreduce.types

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Writable

class Size(var avrg: Float, var total: Long) extends Writable {
  override def readFields(dataInput: DataInput): Unit = {
    avrg = dataInput.readFloat
    total = dataInput.readLong
  }

  override def write(dataOutput: DataOutput): Unit = {
    dataOutput.writeFloat(avrg)
    dataOutput.writeLong(total)
  }

  override def toString = s"$avrg,$total"

  def canEqual(other: Any): Boolean = other.isInstanceOf[Size]

  override def equals(other: Any): Boolean = other match {
    case that: Size =>
      (that canEqual this) &&
        avrg == that.avrg &&
        total == that.total
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(avrg, total)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
