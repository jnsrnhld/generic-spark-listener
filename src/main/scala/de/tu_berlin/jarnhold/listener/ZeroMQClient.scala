package de.tu_berlin.jarnhold.listener

import de.tu_berlin.jarnhold.listener.EventType.{APPLICATION_END, APPLICATION_START, EventType, JOB_END, JOB_START}
import de.tu_berlin.jarnhold.listener.JsonFormats.formats
import org.json4s.MappingException
import org.json4s.native.{JsonMethods, Serialization}
import org.slf4j.{Logger, LoggerFactory}
import org.zeromq.{SocketType, ZContext, ZMQ, ZMQException}

class ZeroMQClient(bridgeServiceAddress: String) {

  private val RESPONSE_TIMEOUT: Integer = Option(System.getenv("LISTENER_RESPONSE_TIMEOUT")).getOrElse("30000").toInt;
  private val POLLING_TIMEOUT = 500;

  private val logger: Logger = LoggerFactory.getLogger(classOf[ZeroMQClient])
  private val context = new ZContext()
  private val socket = context.createSocket(SocketType.REQ)
  socket.connect(bridgeServiceAddress)
  private val poller = context.createPoller(1)
  poller.register(socket, ZMQ.Poller.POLLIN)

  def sendMessage(eventType: EventType, message: Message): ResponseMessage = {

    val startTime = System.currentTimeMillis()

    try {
      // Send the envelope
      val envelope = MessageEnvelope(eventType, message)
      val jsonString = Serialization.write(envelope)
      socket.send(jsonString.getBytes(ZMQ.CHARSET), 0)

      // Keep polling until we get a response or hit the timeout
      while (!Thread.currentThread().isInterrupted) {
        val polled = poller.poll(POLLING_TIMEOUT)
        if (polled > 0 && poller.pollin(0)) {
          val replyBytes = socket.recv(0)
          val replyString = new String(replyBytes, ZMQ.CHARSET)
          val parsedJson = JsonMethods.parse(replyString)
          parsedJson.extractOpt[ResponseMessage] match {
            case Some(response) => return response
            case None => throw new RuntimeException("Failed to extract ResponseMessage from JSON")
          }
        }

        // Check for timeout
        val elapsed = System.currentTimeMillis() - startTime
        if (elapsed > RESPONSE_TIMEOUT) {
          eventType match {
            case APPLICATION_START => {
              System.err.println(s"Did not receive a response on app start... Critical error. Aborting.")
              System.exit(1)
            }
            case JOB_START | JOB_END | APPLICATION_END => {
              val jobMessage = message.asInstanceOf[AppRuntimeMessage]
              logger.warn(s"Listener received no response within $RESPONSE_TIMEOUT ms. Using current scale-out.")
              ResponseMessage(jobMessage.app_event_id, jobMessage.num_executors)
            }
          }
        }
      }

      throw new RuntimeException("Thread interrupted before response received")

    } catch {
      case e: MappingException =>
        throw new RuntimeException("JSON mapping error", e)
      case e: ZMQException =>
        throw new RuntimeException("ZeroMQ communication error", e)
      case e: Exception =>
        throw new RuntimeException("An unexpected error occurred", e)
    }
  }

    def close(): Unit = {
    socket.close()
    context.close()
  }
}
