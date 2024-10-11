package de.tu_berlin.jarnhold.listener

case class ResponseMessage(
                            app_event_id: String,
                            recommended_scale_out: Int
                          )
