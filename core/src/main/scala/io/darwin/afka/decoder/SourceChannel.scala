package io.darwin.afka.decoder

/**
  * Created by darwin on 24/12/2016.
  */
trait SourceChannel {
  def getByte: Byte
  def getShort: Short
  def getInt: Int
  def getLong: Long
  def getBytes(v: Array[Byte])
}
