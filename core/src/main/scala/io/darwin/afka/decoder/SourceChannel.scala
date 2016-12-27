package io.darwin.afka.decoder

import akka.util.ByteString

/**
  * Created by darwin on 26/12/2016.
  */
trait SourceChannel {
  def getByte  : Byte
  def getShort : Short
  def getInt   : Int
  def getLong  : Long
  def getBytes(v: Array[Byte])
  def getByteString(size: Int): ByteString
}
