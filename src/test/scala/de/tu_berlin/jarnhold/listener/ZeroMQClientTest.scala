package de.tu_berlin.jarnhold.listener

import de.tu_berlin.jarnhold.listener.JsonFormats.formats
import org.json4s.native.Serialization
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import scala.concurrent.Await

class ZeroMQClientTest extends AnyFunSuite with Matchers {

  test("ZeroMQClient should send and receive messages correctly") {
    val port = 5556 // Use a test port
    val bridgeServiceAddress = s"tcp://localhost:$port"

    // Start ZeroMQTestServer
    val server = new ZeroMQTestServer(port)
    val responseMessage = ResponseMessage(
      app_event_id = "test-app",
      recommended_scale_out = 5
    )
    val responseJson = Serialization.write(responseMessage)

    val serverFuture = server.start(responseJson)
    Thread.sleep(1000) // Give the server time to start

    val client = new ZeroMQClient(bridgeServiceAddress)
    val requestMessage = RequestMessage(
      app_event_id = "test-app",
      app_name = "Test Application",
      app_time = System.currentTimeMillis(),
      job_id = 1,
      num_executors = 3,
      event_type = EventType.JOB_START
    )

    val result = client.sendMessage(requestMessage)
    result shouldEqual responseMessage

    // Clean up
    client.close()
    server.stop()
    Await.ready(serverFuture, 2.seconds)
  }

}
