package de.tu_berlin.jarnhold.listener

object EventType extends Enumeration {
  type EventType = Value
  val JOB_START, JOB_END, APPLICATION_START, APPLICATION_END = Value
}
