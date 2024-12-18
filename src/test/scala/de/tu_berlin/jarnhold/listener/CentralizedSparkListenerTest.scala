package de.tu_berlin.jarnhold.listener

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, StageInfo}
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

  class CentralizedSparkListenerTest extends AnyFunSuite with MockitoSugar {

    test("CentralizedSparkListener should handle app start, app end, job start, job end, and stage events") {
      val port = 5558
      val bridgeServiceAddress = s"tcp://localhost:$port"

      // Mock SparkConf and SparkContext
      val sparkConf = new SparkConf()
      sparkConf.set("spark.app.name", "Test Application")
      sparkConf.set("spark.customExtraListener.bridgeServiceAddress", bridgeServiceAddress)
      sparkConf.set("spark.customExtraListener.isAdaptive", "true")
      sparkConf.set("spark.customExtraListener.targetRuntime", "30000")
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

      // Create an instance of CentralizedSparkListener with the mocked SparkContext
      val listener = new CentralizedSparkListener(sparkConf, sparkContext)

      // Mock application start event
      val appStart = mock[SparkListenerApplicationStart]
      when(appStart.time).thenReturn(System.currentTimeMillis())
      listener.onApplicationStart(appStart)

      // Mock job start event
      val jobStart = mock[SparkListenerJobStart]
      when(jobStart.jobId).thenReturn(1)
      when(jobStart.time).thenReturn(System.currentTimeMillis())
      listener.onJobStart(jobStart)

      // Mock stage submitted event
      val stageInfo = mock[StageInfo]
      when(stageInfo.stageId).thenReturn(1)
      when(stageInfo.name).thenReturn("test_stage")
      when(stageInfo.submissionTime).thenReturn(Some(System.currentTimeMillis()))
      when(stageInfo.parentIds).thenReturn(Seq.empty)
      when(stageInfo.numTasks).thenReturn(100)

      val stageSubmitted = mock[SparkListenerStageSubmitted]
      when(stageSubmitted.stageInfo).thenReturn(stageInfo)
      listener.onStageSubmitted(stageSubmitted)

      // Mock stage completed event
      val stageCompletedInfo = mock[StageInfo]
      when(stageCompletedInfo.stageId).thenReturn(1)
      when(stageCompletedInfo.name).thenReturn("test_stage")
      when(stageCompletedInfo.completionTime).thenReturn(Some(System.currentTimeMillis()))
      when(stageCompletedInfo.submissionTime).thenReturn(Some(System.currentTimeMillis() - 5000)) // 5 seconds ago
      when(stageCompletedInfo.failureReason).thenReturn(None)
      when(stageCompletedInfo.rddInfos).thenReturn(Seq.empty)
      when(stageCompletedInfo.taskMetrics).thenReturn(mock[TaskMetrics])

      val stageCompleted = mock[SparkListenerStageCompleted]
      when(stageCompleted.stageInfo).thenReturn(stageCompletedInfo)
      listener.onStageCompleted(stageCompleted)

      // Mock job end event
      val jobEnd = mock[SparkListenerJobEnd]
      when(jobEnd.jobId).thenReturn(1)
      when(jobEnd.time).thenReturn(System.currentTimeMillis())
      listener.onJobEnd(jobEnd)

      // Mock application end event
      val appEnd = mock[SparkListenerApplicationEnd]
      when(appEnd.time).thenReturn(System.currentTimeMillis())
      listener.onApplicationEnd(appEnd)

      // Verify scaling request for job end
      verify(sparkContext).requestTotalExecutors(5, 0, Map.empty)

      // Verify that stage submissions were recorded properly
      // This verifies that the `stageInfoMap` within `CentralizedSparkListener` was updated
      verify(stageInfo).stageId
      verify(stageInfo).name
      verify(stageInfo).submissionTime
      verify(stageInfo).parentIds
      verify(stageInfo).numTasks

      // Verify that the stage completion metrics were accessed
      verify(stageCompletedInfo).completionTime
      verify(stageCompletedInfo).failureReason
      verify(stageCompletedInfo).rddInfos
      verify(stageCompletedInfo).taskMetrics

      // Cleanup resources
      server.stop()
      Await.ready(serverFuture, 2.seconds)
    }
  }}
