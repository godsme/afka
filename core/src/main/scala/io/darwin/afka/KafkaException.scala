package io.darwin.afka

/**
  * Created by darwin on 24/12/2016.
  */
class KafkaException(msg: String, cause: Throwable)
  extends RuntimeException(msg, cause) {

  def this()                 = this(null, null)
  def this(msg: String)      = this(msg, null)
  def this(cause: Throwable) = this(null, cause)
}

