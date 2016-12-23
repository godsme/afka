package io.darwin.afka

/**
  * Created by darwin on 24/12/2016.
  */
case class SchemaException(message: String) extends KafkaException(message) {

}