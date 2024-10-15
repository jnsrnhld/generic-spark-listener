package de.tu_berlin.jarnhold.listener

import de.tu_berlin.jarnhold.listener.SafeDivision.saveDivision
import org.apache.spark.scheduler._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import scala.util.Try

class CentralizedSparkListener(sparkConf: SparkConf) extends SparkListener {

  private val logger: Logger = LoggerFactory.getLogger(classOf[CentralizedSparkListener])
  logger.info("Initializing CentralizedSparkListener")

  // Listener configuration
  private val isAdaptive: Boolean = sparkConf.getBoolean("spark.customExtraListener.isAdaptive", defaultValue = true)
  private val bridgeServiceAddress: String = sparkConf.get("spark.customExtraListener.bridgeServiceAddress")
  private val targetRuntime: Int = sparkConf.get("spark.customExtraListener.targetRuntime").toInt
  private val initialExecutors: Int = sparkConf.get("spark.customExtraListener.initialExecutors").toInt
  private val minExecutors: Int = sparkConf.get("spark.customExtraListener.minExecutors").toInt
  private val maxExecutors: Int = sparkConf.get("spark.customExtraListener.maxExecutors").toInt
  private val active: Boolean = this.isAdaptive

  // Setup communication
  checkConfigurations()
  private val zeroMQClient = new ZeroMQClient(bridgeServiceAddress)

  // Application parameters
  private val appSignature: String = sparkConf.get("spark.app.name")
  private var appId: String = _
  private var appStartTime: Long = _
  private var sparkContext: SparkContext = _
  private val currentScaleOut = new AtomicInteger(0)
  private val currentJobId = new AtomicInteger(0)

  // scale-out, time of measurement, total time
  private val scaleOutBuffer: ListBuffer[(Int, Long)] = ListBuffer()
  // stage monitoring
  private val stageInfoMap: StageInfoMap = new StageInfoMap()

  def this(sparkConf: SparkConf, sparkContext: SparkContext) = {
    this(sparkConf)
    this.sparkContext = sparkContext
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    val appAttemptId = applicationStart.appAttemptId
    val appStartTime = applicationStart.time
    val response = sendAppStartMessage(appStartTime, appAttemptId)
    this.appId = response.app_event_id
    this.appStartTime = appStartTime
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    if (!this.active) {
      return
    }

    val jobId = jobStart.jobId
    this.currentJobId.set(jobId)
    if (isInitialJobOfSparkApplication(jobId)) {
      ensureSparkContextIsSet()
      setInitialScaleOut()
    }

    this.stageInfoMap.addStages(jobStart)
    sendJobStartMessage(jobStart.jobId, jobStart.time)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    if (!this.active) {
      return
    }
    val jobId = this.currentJobId.get()
    val scaleOut = this.currentScaleOut.get()
    this.stageInfoMap.addStageSubmit(jobId, scaleOut, stageSubmitted)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    if (!this.active) {
      return
    }
   val rescalingTimeRatio = computeRescalingTimeRatio(
      stageCompleted.stageInfo.submissionTime.getOrElse(0L),
      stageCompleted.stageInfo.completionTime.getOrElse(0L)
    )
    val jobId = this.currentJobId.get()
    val scaleOut = this.currentScaleOut.get()
    this.stageInfoMap.addStageComplete(jobId, scaleOut, rescalingTimeRatio, stageCompleted)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if (!this.active) {
      return
    }

    val jobId = jobEnd.jobId
    val jobDuration = jobEnd.time
    val rescalingTimeRatio: Double = computeRescalingTimeRatio(this.appStartTime, jobEnd.time)
    val stages = this.stageInfoMap.getStages(jobId)
    val response = sendJobEndMessage(jobId, jobDuration, rescalingTimeRatio, stages)

    val recommendedScaleOut = response.recommended_scale_out
    if (recommendedScaleOut != this.currentScaleOut.get()) {
      logger.info(s"Requesting scale-out of $recommendedScaleOut after next job...")
      val requestResult = this.sparkContext.requestTotalExecutors(recommendedScaleOut, 0, Map[String, Int]())
      logger.info("Request acknowledged? => " + requestResult.toString)
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (!this.active) {
      return
    }
    this.sendAppEndMessage(applicationEnd.time)
    this.zeroMQClient.close()
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    handleScaleOutMonitoring(Option(executorAdded.time), executorAdded.executorInfo.executorHost)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    handleScaleOutMonitoring(Option(executorRemoved.time), "NO_HOST")
  }

  private def handleScaleOutMonitoring(executorActionTime: Option[Long], executorHost: String): Unit = {
    synchronized {
      // the executors might be added before actual application start => there won't be a SparkContext yet in this case,
      // thus also no current scale out to monitor yet
      if (!this.active || this.sparkContext == null) {
        return
      }
      // An executor was removed? Else, an executor was added
      if (executorHost == "NO_HOST") {
        this.currentScaleOut.decrementAndGet()
      } else {
        this.currentScaleOut.incrementAndGet()
      }

      logger.info(s"Current number of executors: ${currentScaleOut.get()}.")
      if (executorActionTime.isDefined) {
        scaleOutBuffer.append((currentScaleOut.get(), executorActionTime.get))
      }
    }
  }

  private def setInitialScaleOut(): Unit = {
    synchronized {
      val allExecutors = this.sparkContext.getExecutorMemoryStatus.toSeq.map(_._1)
      val driverHost: String = getDriverHost
      val scaleOut = allExecutors
        .filter(!_.split(":")(0).equals(driverHost))
        .toList
        .length
      this.currentScaleOut.set(scaleOut)
    }
  }

  private def checkConfigurations(): Unit = {
    val parametersList = List(
      "spark.customExtraListener.isAdaptive",
      "spark.customExtraListener.bridgeServiceAddress",
      "spark.customExtraListener.targetRuntime",
      "spark.customExtraListener.initialExecutors",
      "spark.customExtraListener.minExecutors",
      "spark.customExtraListener.maxExecutors",
    )
    logger.info("Current spark conf" + sparkConf.toDebugString)
    for (param <- parametersList) {
      if (!sparkConf.contains(param)) {
        throw new IllegalArgumentException(s"Parameter $param is not specified in the environment!")
      }
    }
  }

  private def computeRescalingTimeRatio(startTime: Long, endTime: Long): Double = {

    val scaleOutList: List[(Int, Long)] = scaleOutBuffer.toList

    val dividend: Long = scaleOutList
      .sortBy(_._2)
      .zipWithIndex.map { case (tup, idx) => (tup._1, tup._2, Try(scaleOutList(idx + 1)._2 - tup._2).getOrElse(0L)) }
      .filter(e => e._2 + e._3 >= startTime && e._2 <= endTime)
      .drop(1) // drop first element => is respective start scale-out
      .dropRight(1) // drop last element => is respective end scale-out
      .map(e => {
        val startTimeScaleOut: Long = e._2
        var endTimeScaleOut: Long = e._2 + e._3
        if (e._3 == 0L)
          endTimeScaleOut = endTime

        val intervalStartTime: Long = Math.min(Math.max(startTime, startTimeScaleOut), endTime)
        val intervalEndTime: Long = Math.max(startTime, Math.min(endTime, endTimeScaleOut))

        intervalEndTime - intervalStartTime
      })
      .sum

    saveDivision(dividend, endTime - startTime)
  }

  private def sendJobStartMessage(jobId: Int, appTime: Long): ResponseMessage = {
    val message = JobStartMessage(
      app_event_id = this.appId,
      app_name = this.appSignature,
      app_time = appTime,
      job_id = jobId,
      num_executors = this.currentScaleOut.get(),
    )
    this.zeroMQClient.sendMessage(EventType.JOB_START, message)
  }

  private def sendJobEndMessage(jobId: Int, appTime: Long, rescalingTimeRatio: Double, stages: Array[Stage]): ResponseMessage = {
    val message = JobEndMessage(
      app_event_id = this.appId,
      app_name = this.appSignature,
      app_time = appTime,
      job_id = jobId,
      num_executors = this.currentScaleOut.get(),
      rescaling_time_ratio = rescalingTimeRatio,
      stages = stages
    )
    this.zeroMQClient.sendMessage(EventType.JOB_END, message)
  }

  private def sendAppStartMessage(appTime: Long, appAttemptId: Option[String]): ResponseMessage = {
    val message = AppStartMessage(
      app_name = this.appSignature,
      app_time = appTime,
      target_runtime = this.targetRuntime,
      initial_executors = this.initialExecutors,
      min_executors = this.minExecutors,
      max_executors = this.maxExecutors,
      attempt_id = appAttemptId.orNull
    )
    this.zeroMQClient.sendMessage(EventType.APPLICATION_START, message)
  }

  private def sendAppEndMessage(appTime: Long): ResponseMessage = {
    val message = AppEndMessage(
      app_event_id = this.appId,
      app_name = this.appSignature,
      app_time = appTime,
      num_executors = this.currentScaleOut.get()
    )
    this.zeroMQClient.sendMessage(EventType.APPLICATION_END, message)
  }


  /**
   * Only call after ApplicationStart: Otherwise a second SparkContext will be created which will eventually crash
   * the application execution.
   */
  private def ensureSparkContextIsSet(): Unit = {
    if (this.sparkContext == null) {
      this.sparkContext = SparkContext.getOrCreate(this.sparkConf)
      logger.info("SparkContext successfully registered in CentralizedSparkListener")
    }
  }

  /**
   * Ensure spark context is set before calling.
   */
  private def getDriverHost = {
    this.sparkContext.getConf.get("spark.driver.host")
  }

  private def isInitialJobOfSparkApplication(jobId: Int) = {
    jobId == 0
  }
}
