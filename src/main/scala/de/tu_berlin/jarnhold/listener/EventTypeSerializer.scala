package de.tu_berlin.jarnhold.listener

import org.json4s.{CustomSerializer, JNull, JString}

object EventTypeSerializer extends CustomSerializer[EventType.Value](format => (
  {
    case JString(s) => EventType.withName(s)
    case JNull      => null
  },
  {
    case eventType: EventType.Value => JString(eventType.toString)
  }
))
