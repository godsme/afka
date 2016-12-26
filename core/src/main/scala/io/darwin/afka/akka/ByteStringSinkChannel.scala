package io.darwin.afka.akka

import java.nio.ByteBuffer

import akka.util.{ByteString, ByteStringBuilder}
import io.darwin.afka.encoder.SinkChannel

/**
  * Created by darwin on 26/12/2016.
  */
case class ByteStringSinkChannel() extends SinkChannel {

  private val builder = new ByteStringBuilder()

  def get: ByteString = {
    val resultBuilder = new ByteStringBuilder()
    resultBuilder.putInt(builder.length).append(builder.result())
    resultBuilder.result
  }

  override def putByte(v: Byte)         = builder.putByte(v)
  override def putShort(v: Short)       = builder.putShort(v)
  override def putInt(v: Int)           = builder.putInt(v)
  override def putLong(v: Long)         = builder.putLong(v)

  override def putBytes(v: Array[Byte]) = builder.putBytes(v)

  override def putByteBuffer(v: ByteBuffer) = builder.append(ByteString.fromByteBuffer(v))
}
