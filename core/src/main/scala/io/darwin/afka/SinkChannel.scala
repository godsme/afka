package io.darwin.afka

import java.nio.ByteBuffer

/**
  * Created by darwin on 24/12/2016.
  */
trait SinkChannel {
  def putByte(v: Byte)
  def putShort(v: Short)
  def putInt(v: Int)
  def putLong(v: Long)
  def putBytes(v: Array[Byte])
  def putByteBuffer(v: ByteBuffer)
}
