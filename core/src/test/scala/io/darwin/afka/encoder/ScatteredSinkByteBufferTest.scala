package io.darwin.afka.it

/**
  * Created by darwin on 25/12/2016.
  */


import akka.util.{ByteStringBuilder}
import io.darwin.afka.encoder.ScatteredSinkByteBuffer
import org.scalatest._

class ScatteredSinkByteBufferTest extends FlatSpec with Matchers {

  it should "return an 1 elem array if its empty" in {
    val buf = ScatteredSinkByteBuffer(20)
    assert(buf.get.length == 1)
  }

  "the size" should "be 0 if its empty" in {
    val buf = ScatteredSinkByteBuffer(20)
    assert(buf.get(0).getInt == 0)
  }

  it should "have more than 1 elements if it's not empty" in {
    val buf = ScatteredSinkByteBuffer(4)
    buf.putByte(82)

    val bufs = buf.get

    assert(bufs.length == 2)
  }

  "the size" should "be the written size" in {
    val buf = ScatteredSinkByteBuffer(4)
    buf.putByte(82)
    buf.putShort(27)
    buf.putInt(123)

    assert(buf.get(0).getInt == 7)
  }

  "get" should "be referential transparent" in {
    val buf = ScatteredSinkByteBuffer(4)
    buf.putByte(82)
    buf.putShort(27)
    buf.putInt(123)

    val bufs1 = buf.get
    val bufs2 = buf.get

    assert(bufs1.length == bufs2.length)

    for(i <- 0 until bufs1.length) {
      assert(bufs1(i) == bufs2(i))
    }
  }

  it should "create a bigger buffer if object size > blocksize" in {
    val buf = ScatteredSinkByteBuffer(4)
    buf.putLong(345)

    val bufs = buf.get

    assert(2 == bufs.length)
    assert(8 == bufs(0).getInt)
    assert(8 == bufs(1).remaining)
    assert(345 == bufs(1).getLong)
  }

  it should "alloc different byte buffer if current one can't contains the writing integers" in {
    val buf = ScatteredSinkByteBuffer(4)
    buf.putByte(82)
    buf.putInt(123)
    buf.putByte(56)
    buf.putShort(27)

    val bufs = buf.get

    assert(4 == bufs.length)
    assert(1 == bufs(1).remaining)
    assert(4 == bufs(2).remaining)
    assert(3 == bufs(3).remaining)
  }

  it should "fill the remaining first then allow new buffer when writing array is bigger than current remaining space" in {
    val buf = ScatteredSinkByteBuffer(4)
    buf.putByte(82)
    buf.putBytes(Array[Byte](0,1,2,3,4,5,6,7,8,9))

    val bufs = buf.get

    assert(3 == bufs.length)
    assert(11 == bufs(0).getInt)
    assert(4 == bufs(1).remaining())
    assert(7 == bufs(2).remaining())
  }

//  it should "fill the remaining first then allow new buffer when writing ByteBuffer is bigger than current remaining space" in {
//    val buf = ScatteredSinkByteBuffer(4)
//    buf.putByte(82)
//
//    val bytes = new ByteStringBuilder()
//    bytes.putLong(20)
//    bytes.putByte(1.toByte)
//    bytes.putLong(30)
//
//    buf.putByteBuffer(bytes.result())
//
//    val bufs = buf.get
//
//    assert(3 == bufs.length)
//    assert(18 == bufs(0).getInt)
//    assert(4 == bufs(1).remaining())
//    assert(14 == bufs(2).remaining())
//  }
}