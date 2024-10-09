package de.tu_berlin.jarnhold.listener

object EventType extends Enumeration {
  type EventType = Value
  val JOB_START, JOB_END, APPLICATION_END = Value
}

case class RequestMessage(
                           app_id: String,
                           app_name: String,
                           app_time: Long,
                           job_id: Int,
                           num_executors: Int,
                           event_type: EventType.Value
                         )
