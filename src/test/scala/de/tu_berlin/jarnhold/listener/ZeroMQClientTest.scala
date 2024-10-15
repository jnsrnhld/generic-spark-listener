package de.tu_berlin.jarnhold.listener

import de.tu_berlin.jarnhold.listener.JsonFormats.formats
import org.json4s.native.Serialization
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import scala.concurrent.{Await, Future}

class ZeroMQClientTest extends AnyFunSuite with Matchers {

  val responseMessage: ResponseMessage = ResponseMessage(
    app_event_id = "test-app",
    recommended_scale_out = 5
  )

  test("ZeroMQClient should send and receive job messages messages correctly") {
    val (bridgeServiceAddress: String, server: ZeroMQTestServer, serverFuture: Future[Unit]) = startZMQServer

    val client = new ZeroMQClient(bridgeServiceAddress)
    val jobStartMessage = JobStartMessage(
      app_event_id = "12552352522",
      app_name = "Test Application",
      app_time = System.currentTimeMillis(),
      job_id = 1,
      num_executors = 3,
    )

    val stageMetrics = StageMetrics(
      cpu_utilization = 0.75,
      gc_time_ratio = 0.05,
      shuffle_read_write_ratio = 0.3,
      input_output_ratio = 0.4,
      memory_spill_ratio = 0.1
    )

    val stage = Stage(
      stage_id = "1",
      stage_name = "example_stage",
      num_tasks = 100,
      parent_stage_ids = "1",
      attempt_id = 1,
      failure_reason = "",
      start_time = System.currentTimeMillis() - 5000L,
      end_time = System.currentTimeMillis(),
      start_scale_out = 10,
      end_scale_out = 20,
      rescaling_time_ratio = 1.5,
      rdd_num_partitions = 50,
      rdd_num_cached_partitions = 40,
      rdd_mem_size = 1024 * 1024 * 1024L,
      rdd_disk_size = 2048 * 1024 * 1024L,
      metrics = stageMetrics
    )

    val jobEndMessage = JobEndMessage(
      app_event_id = "app_event_123",
      app_name = "ExampleApp",
      app_time = System.currentTimeMillis(),
      job_id = 42,
      num_executors = 5,
      rescaling_time_ratio = 1.2,
      stages = Array(stage)
    )

    val result1 = client.sendMessage(EventType.JOB_START, jobStartMessage)
    val result2 = client.sendMessage(EventType.JOB_END, jobEndMessage)
    result1 shouldEqual responseMessage
    result2 shouldEqual responseMessage

    client.close()
    server.stop()
    Await.ready(serverFuture, 2.seconds)
  }

  test("ZeroMQClient should send and receive app messages messages correctly") {
    val (bridgeServiceAddress: String, server: ZeroMQTestServer, serverFuture: Future[Unit]) = startZMQServer

    val client = new ZeroMQClient(bridgeServiceAddress)
    val appStartMessage = AppStartMessage(
      app_name = "Test Application",
      app_time = System.currentTimeMillis(),
      target_runtime = 30000,
      initial_executors = 5,
      min_executors = 2,
      max_executors =  10,
      attempt_id = "1"
    )
    val appEndMessage = AppEndMessage(
      app_event_id = "12552352522",
      app_name = "Test Application",
      app_time = System.currentTimeMillis(),
      num_executors = 5
    )

    val result1 = client.sendMessage(EventType.APPLICATION_START, appStartMessage)
    val result2 = client.sendMessage(EventType.APPLICATION_END, appEndMessage)
    result1 shouldEqual responseMessage
    result2 shouldEqual responseMessage

    client.close()
    server.stop()
    Await.ready(serverFuture, 2.seconds)
  }

  private def startZMQServer = {
    val port = 5556 // Use a test port
    val bridgeServiceAddress = s"tcp://localhost:$port"

    // Start ZeroMQTestServer
    val server = new ZeroMQTestServer(port)
    val responseJson = Serialization.write(responseMessage)

    val serverFuture = server.start(responseJson)
    Thread.sleep(1000) // Give the server time to start
    (bridgeServiceAddress, server, serverFuture)
  }
}
