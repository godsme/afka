package io.darwin.afka.services

import akka.util.{ByteString, ByteStringBuilder}
import io.darwin.afka.encoder.{KafkaEncoder, SinkChannel}
import io.darwin.afka.packets.requests.KafkaRequest

/**
  * Created by darwin on 26/12/2016.
  */
case class ByteStringSinkChannel() extends SinkChannel {

  private val builder = new ByteStringBuilder()

  def getWithoutSize: ByteString = {
    builder.result
  }

  def get: ByteString = {
    val resultBuilder = new ByteStringBuilder()
    resultBuilder.putInt(builder.length).append(builder.result())
    resultBuilder.result
  }

  def encodeWithoutSize[A](o: A)(implicit encoder: KafkaEncoder[A]): ByteString = {
    encoder.encode(this, o)
    getWithoutSize
  }

  def encode[A](o: A)(implicit encoder: KafkaEncoder[A]): ByteString = {
    encoder.encode(this, o)
    get
  }

  def encode(o: KafkaRequest, correlation: Int, clientId: String): ByteString = {
    o.encode(this, correlation, clientId)
    get
  }

  override def putByte(v: Byte)         = builder.putByte(v)
  override def putShort(v: Short)       = builder.putShort(v)
  override def putInt(v: Int)           = builder.putInt(v)
  override def putLong(v: Long)         = builder.putLong(v)

  override def putBytes(v: Array[Byte]) = builder.putBytes(v)

  override def putByteBuffer(v: ByteString) = builder.append(v)
}
