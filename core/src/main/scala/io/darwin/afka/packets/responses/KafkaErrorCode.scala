package io.darwin.afka.packets.responses

/**
  * Created by darwin on 28/12/2016.
  */
object KafkaErrorCode {
  val NO_ERROR: Short                         = 0
  val UNKNOWN: Short                          = -1
  val OFFSET_OUT_OF_RANGE: Short              = 1
  val CORRUPT_MESSAGE: Short                  = 2
  val UNKNOWN_TOPIC_OR_PARTITION: Short       = 3
  val INVALID_MSG_SIZE: Short                 = 4
  val LEADER_NOT_AVAILABLE: Short             = 5
  val NOT_LEADER_FOR_PARTITION: Short         = 6
  val REQUEST_TIMEOUT: Short                  = 7
  val BROCKER_NOT_AVAIL: Short                = 8
  val REPLICA_NOT_AVAILABLE: Short            = 9
  val MSG_SIZE_TOO_LARGE: Short               = 10
  val STALE_CONTROLLER_EPOCH_CODE             = 11
  val OFFSET_METADATA_TOO_LARGE: Short        = 12

  val GROUP_LOAD_IN_PROGRESS: Short           = 14
  val GROUP_COORDINATOR_NOT_AVAILABLE: Short  = 15
  val NOT_COORDINATOR_FOR_GROUP: Short        = 16
  val INVALID_TOPIC: Short                    = 17
  val RECORD_LIST_TOO_LARGE: Short            = 18
  val NO_ENOUGH_REPLICA: Short                = 19
  val NOT_ENOUGH_REPLICAS_AFTER_APPEND: Short = 20
  val INVALID_REQUIRED_ACKS: Short            = 21
  val ILLEGAL_GENERATION: Short               = 22
  val INCONSISTENT_GROUP_PROTOCOL: Short      = 23
  val INVALID_GROUP: Short                    = 24
  val UNKNOWN_MEMBER_ID: Short                = 25
  val INVALID_SESSION_TIMEOUT: Short          = 26
  val REBALANCE_IN_PROGRESS: Short            = 27
  val INVALID_COMMIT_OFFSET_SIZE: Short       = 28
  val AUTHORIZATION_FAILED: Short             = 29
  val GROUP_AUTHORIZATION_FAILED: Short       = 30
  val CLUSTER_AUTHORIZATION_FAILED: Short     = 31
}
