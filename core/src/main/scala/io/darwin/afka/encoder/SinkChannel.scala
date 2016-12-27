package io.darwin.afka.encoder

import akka.util.ByteString

/**
  * Created by darwin on 24/12/2016.
  */
trait SinkChannel {
  def putByte(v: Byte)
  def putShort(v: Short)
  def putInt(v: Int)
  def putLong(v: Long)
  def putBytes(v: Array[Byte])
  def putByteBuffer(v: ByteString)
}
