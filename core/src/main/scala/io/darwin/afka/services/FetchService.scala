package io.darwin.afka.services

import akka.actor.ActorLogging
import io.darwin.afka.PartitionId
import io.darwin.afka.packets.requests.{OffsetFetchRequest, OffsetFetchTopicRequest}
import io.darwin.afka.packets.responses.{FetchResponse, OffsetFetchResponse, OffsetResponse}

/**
  * Created by darwin on 30/12/2016.
  */
object FetchService {

  case class TopicAssignment(topic: String, partitions: Array[PartitionId])

  trait Actor extends KafkaActor with ActorLogging {
    this: Actor with KafkaService {
      val groupId  : String
      val topics   : Array[TopicAssignment]
    } ⇒

    override def receive: Receive = {
      case KafkaClientConnected(_)      ⇒ onConnected
      case offset : OffsetFetchResponse ⇒ onOffsetFetched(offset)
      case msg    : FetchResponse       ⇒ onMessageFetched(msg)
    }

    private def onConnected = {
      send(OffsetFetchRequest(
        group  = groupId,
        topics = topics.map {
          case TopicAssignment(topic, partitions) ⇒ OffsetFetchTopicRequest(topic, partitions)
      }))
    }

    private def onOffsetFetched(offset: OffsetFetchResponse) = {

    }

    private def onMessageFetched(msg: FetchResponse) = {

    }

  }
}
