package io.darwin.afka.services.domain

import io.darwin.afka.packets.requests.HeartBeatRequest
import io.darwin.afka.packets.responses.HeartBeatResponse
import io.darwin.afka.services.common.KafkaServiceSinkChannel

/**
  * Created by darwin on 6/1/2017.
  */
trait HeartBeater {
  this: GroupCoordinator.Actor with KafkaServiceSinkChannel {
    val groupId     : String
  } ⇒

  private var heartbeatAck = true

  def heartBeat = {
    if(heartbeatAck) {
      sending(HeartBeatRequest(groupId, generation, memberId.get))
      heartbeatAck = false
      stay
    }
    else
      suicide("heart beat with response")
  }

  def onHeartBeatRsp(r: HeartBeatResponse): State = {
    heartbeatAck = true
    onResult(r.error, "HeartBeat")(stay)
  }
}
