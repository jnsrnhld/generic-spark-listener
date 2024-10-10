package de.tu_berlin.jarnhold.listener

import org.json4s.{DefaultFormats, Formats}

object JsonFormats {
  implicit val formats: Formats = DefaultFormats + EventTypeSerializer
}
