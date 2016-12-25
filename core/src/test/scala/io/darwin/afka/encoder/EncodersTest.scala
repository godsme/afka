package io.darwin.afka.encoder

import io.darwin.afka.packets.requests.RequestHeader
import org.scalatest._

/**
  * Created by darwin on 25/12/2016.
  */
class EncodersTest extends FlatSpec with Matchers {

  "String" should "should encode with short length" in {
    val buf = ScatteredSinkByteBuffer(20)

    encoding(buf, "abc")

    val bufs = buf.get
    assert(5 == bufs(0).getInt)
    assert(3 == bufs(1).getShort)

    val bytes = new Array[Byte](3)

    bufs(1).get(bytes)
    assert(bytes(0) == 'a'.toByte)
    assert(bytes(1) == 'b'.toByte)
    assert(bytes(2) == 'c'.toByte)
  }

  "Nullable String array" should "should encode as -1 if it's None" in {
    val buf = ScatteredSinkByteBuffer(20)
    val value: Option[Array[String]] = None

    encoding(buf, value)

    val bufs = buf.get

    assert(4 == bufs(0).getInt)
    assert(-1 == bufs(1).getInt)
  }

  "Nullable String array" should "should encode request header" in {
    val buf = ScatteredSinkByteBuffer()
    val header = RequestHeader(3,1,2,"abcd")
    val value: Option[Array[String]] = None

    header.encode(buf)
    encoding(buf, value)

    val bufs = buf.get

    assert(18 == bufs(0).getInt)
    assert(3  == bufs(1).getShort)
    assert(1  == bufs(1).getShort)
    assert(2  == bufs(1).getInt)
    assert(4  == bufs(1).getShort)

    val bytes = new Array[Byte](4)
    bufs(1).get(bytes)

    assert(-1 == bufs(1).getInt)
    //assert(-1 == bufs(1).getInt)
  }
}

