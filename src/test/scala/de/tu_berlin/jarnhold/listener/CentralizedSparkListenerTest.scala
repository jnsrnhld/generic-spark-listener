package de.tu_berlin.jarnhold.listener

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import scala.concurrent.Await

class CentralizedSparkListenerTest extends AnyFunSuite with Matchers with MockitoSugar {

  implicit val formats: Formats = DefaultFormats

  test("CentralizedSparkListener should handle job start and end events") {
    val port = 5558
    val bridgeServiceAddress = s"tcp://localhost:$port"

    // Mock SparkConf and SparkContext
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "Test Application")
    sparkConf.set("spark.customExtraListener.bridgeServiceAddress", bridgeServiceAddress)
    sparkConf.set("spark.customExtraListener.isAdaptive", "true")

    val sparkContext = mock[SparkContext]
    when(sparkContext.requestTotalExecutors(any[Int], any[Int], any[Map[String, Int]])).thenReturn(true)
    when(sparkContext.getExecutorMemoryStatus).thenReturn(Map.empty)
    when(sparkContext.getConf).thenReturn(sparkConf)

    // Start ZeroMQTestServer
    val server = new ZeroMQTestServer(port)
    val responseMessage = ResponseMessage(
      app_id = "test-app",
      recommended_scale_out = 5
    )
    val responseJson = Serialization.write(responseMessage)

    val serverFuture = server.start(responseJson)
    Thread.sleep(1000)

    val listener = new CentralizedSparkListener(sparkConf, sparkContext)

    // Simulate job start
    val jobStart = mock[SparkListenerJobStart]
    when(jobStart.jobId).thenReturn(1)
    when(jobStart.time).thenReturn(System.currentTimeMillis())

    listener.onJobStart(jobStart)

    // Simulate job end
    val jobEnd = mock[SparkListenerJobEnd]
    when(jobEnd.jobId).thenReturn(1)
    when(jobEnd.time).thenReturn(System.currentTimeMillis())

    listener.onJobEnd(jobEnd)

    // Verify that requestTotalExecutors was called
    verify(sparkContext).requestTotalExecutors(5, 0, Map.empty)

    // Clean up
    listener.onApplicationEnd(mock[SparkListenerApplicationEnd])
    server.stop()
    Await.ready(serverFuture, 2.seconds)
  }
}
