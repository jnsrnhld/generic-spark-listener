package de.tu_berlin.jarnhold.listener

import org.json4s.{CustomSerializer, JNull, JString}


object EventType extends Enumeration {
  type EventType = Value
  val JOB_START, JOB_END, APPLICATION_START, APPLICATION_END = Value
}

object EventTypeSerializer extends CustomSerializer[EventType.Value](format => (
  {
    case JString(s) => EventType.withName(s)
    case JNull      => null
  },
  {
    case eventType: EventType.Value => JString(eventType.toString)
  }
))
