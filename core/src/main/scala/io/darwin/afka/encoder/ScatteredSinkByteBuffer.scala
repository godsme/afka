package io.darwin.afka.encoder

import java.nio.{BufferOverflowException, ByteBuffer}

import scala.collection.mutable.{ListBuffer}

/**
  * Created by darwin on 25/12/2016.
  */
class ScatteredSinkByteBuffer(blockSize: Int = 512) extends SinkChannel {

  private var current: ByteBuffer = makeBuffer(4)
  private val buffers = new ListBuffer[ByteBuffer]

  // store the size
  buffers.append(ByteBuffer.allocate(4))

  private def makeBuffer(size: Int) = ByteBuffer.allocate(Math.max(size, blockSize))

  private def currentDone {
    if(current != null) {
      if(current.position() > 0) {
        current.flip
        buffers.append(current)
      }
      current = null
    }
  }

  private def makeNewBuffer(size: Int) = {
    currentDone
    current = makeBuffer(size)
  }

  private def write(size: Int)(w: => Unit) = {
    try {
      w
    } catch {
      case _: BufferOverflowException => {
        makeNewBuffer(size)
        w
      }
    }
  }

  def get: Array[ByteBuffer] = {
    currentDone

    val size = buffers.foldLeft(0) { (s, buf) =>
      s + buf.remaining
    }

    val head = buffers.head
    head.putInt(size - 4)
    head.flip

    buffers.toArray
  }

  override def putByte(v: Byte)   = write(1)(current.put(v))
  override def putShort(v: Short) = write(2)(current.putShort(v))
  override def putInt(v: Int)     = write(4)(current.putInt(v))
  override def putLong(v: Long)   = write(8)(current.putLong(v))

  override def putBytes(v: Array[Byte]) = {
    val writableSize = current.remaining
    if(v.length <= writableSize) {
      current.put(v)
    }
    else {
      current.put(v, 0, writableSize)

      val remaining = v.length - writableSize
      makeNewBuffer(remaining)
      current.put(v, writableSize, remaining)
    }
  }

  override def putByteBuffer(v: ByteBuffer): Unit = {
    val writableSize = current.remaining
    if(v.remaining <= writableSize) {
      current.put(v)
    }
    else {
      val writingBuf = v.slice()
      writingBuf.limit(writableSize)
      current.put(writingBuf)

      v.position(writableSize)
      makeNewBuffer(v.remaining)
      current.put(v)
    }
  }
}

object ScatteredSinkByteBuffer {
  def apply(blockSize: Int = 512) = new ScatteredSinkByteBuffer(blockSize)
}
