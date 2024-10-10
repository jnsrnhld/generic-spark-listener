package de.tu_berlin.jarnhold.listener

case class RequestMessage(
                           app_id: String,
                           app_name: String,
                           app_time: Long,
                           job_id: Int,
                           num_executors: Int,
                           event_type: EventType.Value
                         )
