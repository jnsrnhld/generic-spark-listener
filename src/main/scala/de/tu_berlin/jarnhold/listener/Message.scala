package de.tu_berlin.jarnhold.listener

import de.tu_berlin.jarnhold.listener.EventType.EventType

sealed trait Message
case class MessageEnvelope[T](event_type: EventType, payload: T) extends Message

case class AppStartMessage(
                              app_name: String,
                              app_time: Long,
                              target_runtime: Int,
                              initial_executors: Int,
                              min_executors: Int,
                              max_executors: Int
                            ) extends Message

case class AppEndMessage(
                              app_event_id: String,
                              app_name: String,
                              app_time: Long,
                              num_executors: Int,
                            ) extends Message

case class JobEventMessage(
                           app_event_id: String,
                           app_name: String,
                           app_time: Long,
                           job_id: Int,
                           num_executors: Int,
                         ) extends Message

case class ResponseMessage(
                            app_event_id: String,
                            recommended_scale_out: Int
                          ) extends Message
