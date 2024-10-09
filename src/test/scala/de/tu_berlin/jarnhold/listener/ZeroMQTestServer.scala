package de.tu_berlin.jarnhold.listener

import org.zeromq.{SocketType, ZContext}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

class ZeroMQTestServer(port: Int) {

  private val context = new ZContext()
  private val socket = context.createSocket(SocketType.REP)
  private val serverThread = Promise[Unit]()

  socket.bind(s"tcp://*:$port")

  def start(respondWith: String): Future[Unit] = Future {
    serverThread.success(())
    while (!Thread.currentThread().isInterrupted) {
      val message = socket.recvStr()
      if (message != null) {
        socket.send(respondWith)
      }
    }
  }

  def stop(): Unit = {
    socket.close()
    context.close()
    serverThread.tryFailure(new Exception("Server stopped"))
  }
}
