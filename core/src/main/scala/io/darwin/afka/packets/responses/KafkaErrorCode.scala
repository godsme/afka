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
  val BROKER_NOT_AVAIL: Short                 = 8
  val REPLICA_NOT_AVAILABLE: Short            = 9
  val MSG_SIZE_TOO_LARGE: Short               = 10
  val STALE_CONTROLLER_EPOCH_CODE: Short      = 11
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

  private val map: Map[Short, String] = Map(
    NO_ERROR                         → "ok",
    UNKNOWN                          → "unknown error",
    OFFSET_OUT_OF_RANGE              → "offset out of range",
    CORRUPT_MESSAGE                  → "corrupt message",
    UNKNOWN_TOPIC_OR_PARTITION       → "unknown topic or partition",
    INVALID_MSG_SIZE                 → "invalid message size",
    LEADER_NOT_AVAILABLE             → "leader not available",
    NOT_LEADER_FOR_PARTITION         → "not leader for partition",
    REQUEST_TIMEOUT                  → "request timeout",
    BROKER_NOT_AVAIL                 → "broker not available",
    REPLICA_NOT_AVAILABLE            → "replica not available",
    MSG_SIZE_TOO_LARGE               → "message size too large",
    STALE_CONTROLLER_EPOCH_CODE      → "stale controller epoch code",
    OFFSET_METADATA_TOO_LARGE        → "offset meta data too large",
    GROUP_LOAD_IN_PROGRESS           → "group load in progress",
    GROUP_COORDINATOR_NOT_AVAILABLE  → "group coordinator not available",
    NOT_COORDINATOR_FOR_GROUP        → "not coordinator for group",
    INVALID_TOPIC                    → "invalid topic",
    RECORD_LIST_TOO_LARGE            → "record list too large",
    NO_ENOUGH_REPLICA                → "no enough replica",
    NOT_ENOUGH_REPLICAS_AFTER_APPEND → "not enough replicas after append",
    INVALID_REQUIRED_ACKS            → "invalid required acks",
    ILLEGAL_GENERATION               → "illegal generation",
    INCONSISTENT_GROUP_PROTOCOL      → "inconsistent group protocol",
    INVALID_GROUP                    → "invalid group",
    UNKNOWN_MEMBER_ID                → "unknown member id",
    INVALID_SESSION_TIMEOUT          → "invalid session timeout",
    REBALANCE_IN_PROGRESS            → "rebalance in progress",
    INVALID_COMMIT_OFFSET_SIZE       → "invalid commit offset size",
    AUTHORIZATION_FAILED             → "authorization failed",
    GROUP_AUTHORIZATION_FAILED       → "group authorization failed",
    CLUSTER_AUTHORIZATION_FAILED     → "cluster authorization failed")

  def string(code: Short): String = s"code=${code}, ${map.get(code).getOrElse("unspecified")}"

}
