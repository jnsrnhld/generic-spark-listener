package de.tu_berlin.jarnhold.listener

import de.tu_berlin.jarnhold.listener.EventType.EventType
import de.tu_berlin.jarnhold.listener.JsonFormats.formats
import org.json4s.MappingException
import org.json4s.native.{JsonMethods, Serialization}
import org.zeromq.{SocketType, ZContext, ZMQ, ZMQException}

class ZeroMQClient(bridgeServiceAddress: String) {

  private val POLLING_TIMEOUT = 500;
  private val context = new ZContext()
  private val socket = context.createSocket(SocketType.REQ)
  socket.connect(bridgeServiceAddress)
  private val poller = context.createPoller(1)
  poller.register(socket, ZMQ.Poller.POLLIN)

  def sendMessage(eventType: EventType, message: Message): ResponseMessage = {
    try {
      val envelope = MessageEnvelope(eventType, message)
      val jsonString = Serialization.write(envelope)
      socket.send(jsonString.getBytes(ZMQ.CHARSET), 0)

      while (!Thread.currentThread().isInterrupted) {
        poller.poll(POLLING_TIMEOUT)
        if (poller.pollin(0)) {
          val replyBytes = socket.recv(0)
          val replyString = new String(replyBytes, ZMQ.CHARSET)
          val parsedJson = JsonMethods.parse(replyString)
          parsedJson.extractOpt[ResponseMessage] match {
            case Some(response) => return response
            case None => throw new RuntimeException("Failed to extract ResponseMessage from JSON")
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
