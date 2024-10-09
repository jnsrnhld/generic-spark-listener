package de.tu_berlin.jarnhold.listener

import org.json4s.Formats
import org.json4s.native.JsonMethods
import org.json4s.native.Serialization
import org.zeromq.{SocketType, ZContext, ZMQ}

class ZeroMQClient(bridgeServiceAddress: String)(implicit formats: Formats) {

  private val context = new ZContext()
  private val socket = context.createSocket(SocketType.REQ)
  socket.connect(bridgeServiceAddress)

  def sendMessage(message: RequestMessage): ResponseMessage = {
    val jsonString = Serialization.write(message)
    socket.send(jsonString.getBytes(ZMQ.CHARSET), 0)

    val replyBytes = socket.recv(0)
    val replyString = new String(replyBytes, ZMQ.CHARSET)
    JsonMethods.parse(replyString).extract[ResponseMessage]
  }

  def close(): Unit = {
    socket.close()
    context.close()
  }
}
