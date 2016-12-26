package io.darwin.afka.akka

import akka.util.ByteString
import io.darwin.afka.decoder.SourceChannel

/**
  * Created by darwin on 26/12/2016.
  */
case class ByteStringSourceChannel(buf: ByteString)
  extends SourceChannel {

  private val i = buf.iterator

  override def getByte: Byte            = i.getByte
  override def getShort: Short          = i.getShort
  override def getInt: Int              = i.getInt
  override def getLong: Long            = i.getLong
  override def getBytes(v: Array[Byte]) = i.getBytes(v)
}
