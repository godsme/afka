package io.darwin.afka.assignors

import io.darwin.afka.assignors.PartitionAssignor.{MemberAssignment, MemberSubscription}
import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.packets.common.{ProtoPartitionAssignment, ProtoSubscription}

import scala.collection.mutable

/**
  * Created by darwin on 28/12/2016.
  */

class RangeAssignor extends PartitionAssignor {
  override val name: String = "range"

  override def subscribe(topics: Array[String]): ProtoSubscription = ProtoSubscription(topics = topics)

  type Topic = String
  type Member = String
  type TopicMap = scala.collection.mutable.Map[Topic, mutable.MutableList[Member]]

  def getTopicMap(subscriptions: Array[MemberSubscription]): TopicMap = {
    val map: TopicMap = scala.collection.mutable.Map.empty
    subscriptions.foreach { case MemberSubscription(memberId, ProtoSubscription(_, topics, _)) ⇒
      topics.foreach { topic ⇒
        map.getOrElseUpdate(topic, new mutable.MutableList[Member]) += memberId
      }
    }
    map
  }

  override def assign(cluster: KafkaCluster, subscriptions: Array[MemberSubscription]): MemberAssignment = {
    val map = getTopicMap(subscriptions)

    val rMap: MemberAssignment = scala.collection.mutable.Map.empty

    map.foreach { case (topic, members) ⇒
      val optPartitions = cluster.getPartitionsByTopic(topic)
      val numOfPartions: Int = optPartitions.fold[Int](0)(_.length)
        if(numOfPartions > 0) {
          val numOfMember = members.length
          val partitionPerMember = numOfPartions / numOfMember
          var extraParitions = numOfPartions % numOfMember
          val paritions = optPartitions.get.sortBy(_.id)

          var n = 0
          members.foreach { member ⇒
            val num = partitionPerMember + (if(extraParitions > 0) 1 else 0)

            if(num > 0) {

              val p = paritions.slice(n, n+num).map(_.id)

              n += num
              if(extraParitions > 0) extraParitions -= 1
              rMap.getOrElseUpdate(member, mutable.MutableList.empty) += ProtoPartitionAssignment(topic=topic, partitions=p)
            }
          }
        }
    }

    rMap
  }
}