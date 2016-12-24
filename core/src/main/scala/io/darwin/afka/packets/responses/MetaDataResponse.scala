package io.darwin.afka.packets.responses

import io.darwin.afka.decoder.{KafkaDecoder, SourceChannel, decoding, _}
import io.darwin.kafka.macros.{KafkaResponse, KafkaResponseElement}

/**
  * Created by darwin on 24/12/2016.
  */
@KafkaResponseElement
case class Broker
  ( nodeId : Int,
    host   : String,
    port   : Int,
    rack   : Option[String])

object BrokerObject {

  implicit object BrokerDecoder extends KafkaDecoder[Broker] {
    override def decode(chan: SourceChannel): Broker = {
      Broker(
        nodeId = decoding[Int](chan),
        host   = decoding[String](chan),
        port   = decoding[Int](chan),
        rack   = decoding[Option[String]](chan)
      )
    }
  }

  implicit val ARRAY_OF_Broker = ArrayDecoder.make[Broker]
}

@KafkaResponseElement
case class PartitionMetaData
  ( errorCode : Short,
    id        : Int,
    leader    : Int,
    replicas  : Array[Int],
    isr       : Array[Int])

object PartitionMetaDataObject {

  implicit object PartitionMetaDataDecoder extends KafkaDecoder[PartitionMetaData] {
    override def decode(chan: SourceChannel): PartitionMetaData = {
      PartitionMetaData(
        errorCode = decoding[Short](chan),
        id        = decoding[Int](chan),
        leader    = decoding[Int](chan),
        replicas  = decoding[Array[Int]](chan),
        isr       = decoding[Array[Int]](chan)
      )
    }
  }

  implicit val ARRAY_OF_PartitionMetaData = ArrayDecoder.make[PartitionMetaData]
}

@KafkaResponseElement
case class TopicMetaData
  ( errorCode : Short,
    topic:      String,
    isInternal: Boolean,
    partitions: Array[PartitionMetaData])

object TopicMetaDataObject {

  implicit object TopicMetaDataDecoder extends KafkaDecoder[TopicMetaData] {
    override def decode(chan: SourceChannel): TopicMetaData = {

      import PartitionMetaDataObject._

      TopicMetaData(
        errorCode = decoding[Short](chan),
        topic = decoding[String](chan),
        isInternal = decoding[Boolean](chan),
        partitions = decoding[Array[PartitionMetaData]](chan)
      )
    }
  }

  implicit val ARRAY_OF_TopicMetaData = ArrayDecoder.make[TopicMetaData]
}

@KafkaResponse
case class MetaDataResponse
  ( brokers      : Array[Broker],
    controllerId : Int,
    topics       : Array[TopicMetaData])


object MetaDataResponseObject {

  implicit object MetaDataResponseDecoder extends KafkaDecoder[MetaDataResponse] {
    override def decode(chan: SourceChannel): MetaDataResponse = {

      import BrokerObject._
      import TopicMetaDataObject._

      MetaDataResponse(
        brokers = decoding[Array[Broker]](chan),
        controllerId = decoding[Int](chan),
        topics = decoding[Array[TopicMetaData]](chan)
      )
    }
  }

}