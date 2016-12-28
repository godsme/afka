package io.darwin.afka.assignors


import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.packets.common.{ProtoPartitionAssignment, ProtoSubscription}

import scala.collection.mutable

/**
  * Created by darwin on 28/12/2016.
  */
object PartitionAssignor {
  type Member = String
  type MemberAssignment = scala.collection.mutable.Map[Member, mutable.MutableList[ProtoPartitionAssignment]]
  case class MemberSubscription(memberId: String, subscriptions: Array[ProtoSubscription])
}

trait PartitionAssignor {
  val name: String

  import io.darwin.afka.assignors.PartitionAssignor._

  def subscribe(topics: Array[String]): ProtoSubscription
  def assign(cluster: KafkaCluster, subscriptions: List[MemberSubscription]): MemberAssignment
}
