package de.tu_berlin.jarnhold.listener

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import scala.concurrent.Await

class CentralizedSparkListenerTest extends AnyFunSuite with MockitoSugar with ScalaFutures {

  implicit val formats: Formats = DefaultFormats

  test("CentralizedSparkListener should handle app start, app end, job start, and job end events") {
    val port = 5558
    val bridgeServiceAddress = s"tcp://localhost:$port"

    // Mock SparkConf and SparkContext
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "Test Application")
    sparkConf.set("spark.customExtraListener.bridgeServiceAddress", bridgeServiceAddress)
    sparkConf.set("spark.customExtraListener.isAdaptive", "true")
    sparkConf.set("spark.customExtraListener.initialExecutors", "5")
    sparkConf.set("spark.customExtraListener.minExecutors", "2")
    sparkConf.set("spark.customExtraListener.maxExecutors", "10")

    val sparkContext = mock[SparkContext]
    when(sparkContext.requestTotalExecutors(any[Int], any[Int], any[Map[String, Int]])).thenReturn(true)
    when(sparkContext.getExecutorMemoryStatus).thenReturn(Map("executor1" -> (123456789L, 987654321L)))
    when(sparkContext.getConf).thenReturn(sparkConf)

    // Start ZeroMQTestServer
    val server = new ZeroMQTestServer(port)
    val responseMessage = ResponseMessage(
      app_event_id = "test-app",
      recommended_scale_out = 5
    )
    val responseJson = Serialization.write(responseMessage)
    val serverFuture = server.start(responseJson)
    Thread.sleep(1000)

    val listener = new CentralizedSparkListener(sparkConf, sparkContext)

    // Simulate application and job events
    val appStart = mock[SparkListenerApplicationStart]
    when(appStart.time).thenReturn(System.currentTimeMillis())
    listener.onApplicationStart(appStart)

    val jobStart = mock[SparkListenerJobStart]
    when(jobStart.jobId).thenReturn(1)
    when(jobStart.time).thenReturn(System.currentTimeMillis())
    listener.onJobStart(jobStart)

    val jobEnd = mock[SparkListenerJobEnd]
    when(jobEnd.jobId).thenReturn(1)
    when(jobEnd.time).thenReturn(System.currentTimeMillis())
    listener.onJobEnd(jobEnd)

    val appEnd = mock[SparkListenerApplicationEnd]
    when(appEnd.time).thenReturn(System.currentTimeMillis())
    listener.onApplicationEnd(appEnd)

    // Verify scaling request
    verify(sparkContext).requestTotalExecutors(5, 0, Map.empty)

    // Cleanup resources
    server.stop()
    Await.ready(serverFuture, 2.seconds)
  }

}
